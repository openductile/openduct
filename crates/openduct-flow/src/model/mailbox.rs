// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Reading the `mailbox` step (see [`crate::mailbox`]): each setting checked, and only the settings its action takes
//! allowed.

use serde_json::{Map, Value as Json};

use super::{Kind, Reader, text};
use crate::mailbox::{Action, Criteria, LIST_ITEMS, MOST_LIST_ITEMS, MailboxStep};

fn flag(s: &Map<String, Json>, name: &str, at: &str) -> Result<Option<bool>, String> {
    match s.get(name) {
        None | Some(Json::Null) => Ok(None),
        Some(Json::Bool(b)) => Ok(Some(*b)),
        Some(_) => Err(format!("{at}.{name}: true or false")),
    }
}

impl Reader {
    pub(super) fn mailbox(&self, body: &Json, at: &str) -> Result<Kind, String> {
        let s = body
            .as_object()
            .ok_or_else(|| format!("{at}: a set of settings"))?;
        let named =
            text(s.get("action")).ok_or_else(|| format!("{at}.action: {}", Action::ALL.join(", ")))?;
        let action = Action::parse(named)
            .ok_or_else(|| format!("{at}.action: {} - not {named:?}", Action::ALL.join(", ")))?;
        let own: &[&str] = match action {
            Action::List => &[
                "unseen", "flagged", "from", "to", "subject", "text", "since", "before", "limit",
            ],
            Action::Read => &["uid", "files", "raw"],
            Action::Mark => &["uid", "seen", "flagged"],
            Action::Move => &["uid", "to_folder"],
            Action::Delete => &["uid"],
            Action::Folders => &[],
        };
        let mut allowed = vec!["action", "credential", "folder"];
        allowed.extend_from_slice(own);
        if let Some(k) = s.keys().find(|k| !allowed.contains(&k.as_str())) {
            return Err(format!(
                "{at}.{k}: not a setting of mailbox's {named} ({})",
                allowed.join(", ")
            ));
        }
        let credential = text(s.get("credential"))
            .filter(|c| !c.trim().is_empty())
            .ok_or_else(|| format!("{at}.credential: the mailbox's imap credential"))?
            .to_owned();
        let template = |name: &str| {
            s.get(name)
                .filter(|v| !v.is_null())
                .map(|v| self.template(v, &format!("{at}.{name}")))
                .transpose()
        };
        let uid = template("uid")?;
        if uid.is_none() && !matches!(action, Action::List | Action::Folders) {
            return Err(format!(
                "{at}.uid: the message's UID, as {{{{ value.uid }}}} (a list or the trigger gives it)"
            ));
        }
        let limit = match s.get("limit") {
            None | Some(Json::Null) => LIST_ITEMS,
            Some(n) => n
                .as_u64()
                .and_then(|n| usize::try_from(n).ok())
                .filter(|n| (1..=MOST_LIST_ITEMS).contains(n))
                .ok_or_else(|| format!("{at}.limit: 1 to {MOST_LIST_ITEMS}"))?,
        };
        let seen = flag(s, "seen", at)?;
        let flagged_mark = flag(s, "flagged", at)?;
        if action == Action::Mark && seen.is_none() && flagged_mark.is_none() {
            return Err(format!(
                "{at}: seen, flagged or both - true to set, false to take away"
            ));
        }
        let to_folder = template("to_folder")?;
        if action == Action::Move && to_folder.is_none() {
            return Err(format!("{at}.to_folder: the folder it goes to, as Archive"));
        }
        let criteria = Criteria {
            unseen: action == Action::List && flag(s, "unseen", at)?.unwrap_or(false),
            flagged: action == Action::List && flagged_mark.unwrap_or(false),
            from: template("from")?,
            to: template("to")?,
            subject: template("subject")?,
            text: template("text")?,
            since: template("since")?,
            before: template("before")?,
        };
        Ok(Kind::Mailbox(Box::new(MailboxStep {
            action,
            credential,
            folder: template("folder")?,
            criteria,
            limit,
            uid,
            seen,
            flagged: if action == Action::Mark {
                flagged_mark
            } else {
                None
            },
            to_folder,
            files: flag(s, "files", at)?.unwrap_or(true),
            raw: flag(s, "raw", at)?.unwrap_or(false),
        })))
    }
}
