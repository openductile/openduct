// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `openduct cluster ...`: a cluster's brokers and where its partitions are, spreading them evenly again (after a
//! broker joins), and taking a broker out (its partitions moved to the others first). Spoken with Kafka's own admin
//! messages (Metadata, AlterPartitionReassignments, ListPartitionReassignments, UnregisterBroker).

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use clap::Subcommand;
use kafka_protocol::messages::alter_partition_reassignments_request::{
    ReassignablePartition, ReassignableTopic,
};
use kafka_protocol::messages::{
    AlterPartitionReassignmentsRequest, BrokerId, ListPartitionReassignmentsRequest, MetadataRequest,
    TopicName, UnregisterBrokerRequest,
};
use kafka_protocol::protocol::StrBytes;
use openduct_broker::client::Client;

use crate::admin::{Target, connect};

#[derive(Debug, Subcommand)]
pub(crate) enum ClusterCommand {
    /// The brokers, how many partitions each holds and leads, and the moves under way.
    Status {
        #[command(flatten)]
        target: Target,
    },
    /// Spread the partitions evenly over the brokers - after one joins, say - moving as few as possible.
    Rebalance {
        /// Print the moves without making them.
        #[arg(long)]
        dry_run: bool,
        /// How long to wait for the moves to finish, in seconds.
        #[arg(long, default_value_t = 600)]
        wait_secs: u64,
        #[command(flatten)]
        target: Target,
    },
    /// Take a broker out of the cluster: its partitions move to the others first.
    RemoveBroker {
        /// The broker's id.
        id: i32,
        /// Print the moves without making them.
        #[arg(long)]
        dry_run: bool,
        /// How long to wait for the moves to finish, in seconds.
        #[arg(long, default_value_t = 600)]
        wait_secs: u64,
        #[command(flatten)]
        target: Target,
    },
}

/// A partition and the brokers holding it, the first one leading it.
type Place = (String, i32, Vec<i32>);

struct Cluster {
    brokers: BTreeMap<i32, String>,
    controller: i32,
    partitions: Vec<Place>,
    leaders: BTreeMap<(String, i32), i32>,
}

async fn cluster(client: &mut Client) -> Result<Cluster, String> {
    let res = client
        .send(&MetadataRequest::default().with_topics(None), 9)
        .await
        .map_err(|e| e.to_string())?;
    let brokers = res
        .brokers
        .iter()
        .map(|b| (b.node_id.0, format!("{}:{}", b.host, b.port)))
        .collect();
    let mut partitions = Vec::new();
    let mut leaders = BTreeMap::new();
    for t in &res.topics {
        let Some(name) = t.name.as_ref().map(|n| n.0.to_string()) else {
            continue;
        };
        for p in &t.partitions {
            partitions.push((
                name.clone(),
                p.partition_index,
                p.replica_nodes.iter().map(|b| b.0).collect(),
            ));
            leaders.insert((name.clone(), p.partition_index), p.leader_id.0);
        }
    }
    partitions.sort();
    Ok(Cluster {
        brokers,
        controller: res.controller_id.0,
        partitions,
        leaders,
    })
}

/// Where every partition should be on `brokers`: each keeps as many of its brokers as it can and as many copies as it
/// has (at most one per broker); the copies, and the partitions each broker leads first, even out to within one.
/// Returns the partitions whose brokers, or first broker, change.
pub(crate) fn plan(brokers: &[i32], current: &[Place]) -> Vec<Place> {
    let n = brokers.len();
    if n == 0 {
        return Vec::new();
    }
    let usable: BTreeSet<i32> = brokers.iter().copied().collect();
    let mut load: BTreeMap<i32, usize> = brokers.iter().map(|b| (*b, 0)).collect();
    let mut next: Vec<Vec<i32>> = Vec::with_capacity(current.len());
    for (_, _, replicas) in current {
        let rf = replicas.len().clamp(1, n);
        let mut kept: Vec<i32> = replicas
            .iter()
            .filter(|r| usable.contains(r))
            .copied()
            .take(rf)
            .collect();
        for r in &kept {
            *load.entry(*r).or_default() += 1;
        }
        while kept.len() < rf {
            let Some(b) = least(&load, &kept) else { break };
            *load.entry(b).or_default() += 1;
            kept.push(b);
        }
        next.push(kept);
    }
    // Even out the copies: move one from the busiest broker to the idlest until they differ by one at most.
    for _ in 0..current.len() * n {
        let (Some((&hi, &hi_n)), Some((&lo, &lo_n))) = (
            load.iter().max_by_key(|(b, c)| (**c, std::cmp::Reverse(**b))),
            load.iter().min_by_key(|(b, c)| (**c, **b)),
        ) else {
            break;
        };
        if hi_n <= lo_n + 1 {
            break;
        }
        let Some(slot) = next.iter_mut().find(|r| r.contains(&hi) && !r.contains(&lo)) else {
            break;
        };
        if let Some(r) = slot.iter_mut().find(|r| **r == hi) {
            *r = lo;
        }
        *load.entry(hi).or_default() -= 1;
        *load.entry(lo).or_default() += 1;
    }
    // Even out the leaders: each partition first on the copy leading fewest so far, keeping its first if it can.
    let mut leads: BTreeMap<i32, usize> = brokers.iter().map(|b| (*b, 0)).collect();
    let ceiling = current.len().div_ceil(n);
    for replicas in &mut next {
        let keep = replicas
            .first()
            .copied()
            .filter(|f| leads.get(f).copied().unwrap_or(0) < ceiling);
        let first = keep.or_else(|| {
            replicas
                .iter()
                .copied()
                .min_by_key(|r| (leads.get(r).copied().unwrap_or(0), *r))
        });
        if let Some(first) = first {
            replicas.retain(|r| *r != first);
            replicas.insert(0, first);
            *leads.entry(first).or_default() += 1;
        }
    }
    current
        .iter()
        .zip(next)
        .filter(|((_, _, before), after)| before != after)
        .map(|((t, p, _), after)| (t.clone(), *p, after))
        .collect()
}

fn least(load: &BTreeMap<i32, usize>, not: &[i32]) -> Option<i32> {
    load.iter()
        .filter(|(b, _)| !not.contains(b))
        .min_by_key(|(b, c)| (**c, **b))
        .map(|(b, _)| *b)
}

async fn reassign(client: &mut Client, moves: &[Place]) -> Result<(), String> {
    let mut by_topic: BTreeMap<String, Vec<ReassignablePartition>> = BTreeMap::new();
    for (t, p, replicas) in moves {
        by_topic.entry(t.clone()).or_default().push(
            ReassignablePartition::default()
                .with_partition_index(*p)
                .with_replicas(Some(replicas.iter().map(|b| BrokerId(*b)).collect())),
        );
    }
    let req = AlterPartitionReassignmentsRequest::default()
        .with_timeout_ms(30_000)
        .with_topics(
            by_topic
                .into_iter()
                .map(|(t, partitions)| {
                    ReassignableTopic::default()
                        .with_name(TopicName(StrBytes::from_string(t)))
                        .with_partitions(partitions)
                })
                .collect(),
        );
    let res = client.send(&req, 0).await.map_err(|e| e.to_string())?;
    for t in &res.responses {
        for p in &t.partitions {
            if p.error_code != 0 {
                return Err(format!(
                    "{}/{}: {}",
                    t.name.0,
                    p.partition_index,
                    p.error_message.as_deref().unwrap_or("refused")
                ));
            }
        }
    }
    Ok(())
}

async fn ongoing(client: &mut Client) -> Result<usize, String> {
    let res = client
        .send(&ListPartitionReassignmentsRequest::default().with_topics(None), 0)
        .await
        .map_err(|e| e.to_string())?;
    Ok(res.topics.iter().map(|t| t.partitions.len()).sum())
}

async fn wait_done(client: &mut Client, wait: Duration) -> Result<(), String> {
    let deadline = Instant::now() + wait;
    loop {
        let left = ongoing(client).await?;
        if left == 0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("{left} moves still under way after {wait:?}"));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn print_moves(moves: &[Place], before: &[Place]) {
    for (t, p, after) in moves {
        let was = before
            .iter()
            .find(|(bt, bp, _)| bt == t && bp == p)
            .map(|(_, _, r)| r.clone())
            .unwrap_or_default();
        println!("{t}/{p}: {was:?} -> {after:?}");
    }
}

pub(crate) async fn run(command: ClusterCommand) -> Result<(), String> {
    match command {
        ClusterCommand::Status { target } => {
            let mut client = connect(&target).await?;
            let c = cluster(&mut client).await?;
            println!("coordinator: broker {}", c.controller);
            for (id, addr) in &c.brokers {
                let held = c.partitions.iter().filter(|(_, _, r)| r.contains(id)).count();
                let led = c.leaders.values().filter(|l| *l == id).count();
                println!("broker {id} at {addr}: holds {held} partitions, leads {led}");
            }
            let moving = ongoing(&mut client).await?;
            if moving > 0 {
                println!("{moving} partitions moving");
            }
        }
        ClusterCommand::Rebalance {
            dry_run,
            wait_secs,
            target,
        } => {
            let mut client = connect(&target).await?;
            let c = cluster(&mut client).await?;
            let brokers: Vec<i32> = c.brokers.keys().copied().collect();
            let moves = plan(&brokers, &c.partitions);
            if moves.is_empty() {
                println!("the partitions are spread evenly already");
                return Ok(());
            }
            print_moves(&moves, &c.partitions);
            if !dry_run {
                reassign(&mut client, &moves).await?;
                wait_done(&mut client, Duration::from_secs(wait_secs)).await?;
                println!("moved {} partitions", moves.len());
            }
        }
        ClusterCommand::RemoveBroker {
            id,
            dry_run,
            wait_secs,
            target,
        } => {
            let mut client = connect(&target).await?;
            let c = cluster(&mut client).await?;
            if !c.brokers.contains_key(&id) {
                return Err(format!("broker {id} is not in the cluster"));
            }
            let brokers: Vec<i32> = c.brokers.keys().copied().filter(|b| *b != id).collect();
            let moves = plan(&brokers, &c.partitions);
            print_moves(&moves, &c.partitions);
            if dry_run {
                return Ok(());
            }
            if !moves.is_empty() {
                reassign(&mut client, &moves).await?;
                wait_done(&mut client, Duration::from_secs(wait_secs)).await?;
            }
            let res = client
                .send(
                    &UnregisterBrokerRequest::default().with_broker_id(BrokerId(id)),
                    0,
                )
                .await
                .map_err(|e| e.to_string())?;
            if res.error_code != 0 {
                return Err(res
                    .error_message
                    .map(|m| m.to_string())
                    .unwrap_or_else(|| format!("the cluster refused (error code {})", res.error_code)));
            }
            println!("broker {id} left the cluster; it can be stopped");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn place(p: i32, replicas: &[i32]) -> Place {
        ("t".to_owned(), p, replicas.to_vec())
    }

    fn apply(current: &[Place], moves: &[Place]) -> Vec<Place> {
        current
            .iter()
            .map(|c| {
                moves
                    .iter()
                    .find(|m| m.0 == c.0 && m.1 == c.1)
                    .cloned()
                    .unwrap_or_else(|| c.clone())
            })
            .collect()
    }

    fn counts(after: &[Place]) -> (BTreeMap<i32, usize>, BTreeMap<i32, usize>) {
        let mut held = BTreeMap::new();
        let mut led = BTreeMap::new();
        for (_, _, r) in after {
            for b in r {
                *held.entry(*b).or_default() += 1;
            }
            *led.entry(r[0]).or_default() += 1;
        }
        (held, led)
    }

    #[test]
    fn a_new_broker_gets_its_share_and_little_else_moves() {
        let current: Vec<Place> = (0..6)
            .map(|p| place(p, &[p % 3 + 1, (p + 1) % 3 + 1, (p + 2) % 3 + 1]))
            .collect();
        let moves = plan(&[1, 2, 3, 4], &current);
        let after = apply(&current, &moves);
        let (held, led) = counts(&after);
        // 18 copies over 4 brokers: 4 or 5 each; 6 leaders: 1 or 2 each.
        assert!(held.values().all(|c| (4..=5).contains(c)), "{held:?}");
        assert!(led.values().all(|c| (1..=2).contains(c)), "{led:?}");
        assert_eq!(held.len(), 4);
        // Every partition keeps three copies on distinct brokers.
        assert!(
            after
                .iter()
                .all(|(_, _, r)| r.len() == 3 && r.iter().collect::<BTreeSet<_>>().len() == 3)
        );
        assert!(
            plan(&[1, 2, 3, 4], &after).is_empty(),
            "a second rebalance moves nothing"
        );
    }

    #[test]
    fn a_broker_leaving_hands_every_partition_to_the_others() {
        let current: Vec<Place> = (0..4)
            .map(|p| place(p, &[(p % 4) + 1, ((p + 1) % 4) + 1, ((p + 2) % 4) + 1]))
            .collect();
        let after = apply(&current, &plan(&[1, 2, 3], &current));
        assert!(after.iter().all(|(_, _, r)| !r.contains(&4) && r.len() == 3));
    }
}
