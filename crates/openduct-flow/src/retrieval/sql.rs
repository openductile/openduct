// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The SQL a vector store runs. The table's name is the only thing written into it, and only after it is checked
//! when the workflow is read (small letters, digits and `_`, so nothing in it can end a quoted name); a number of
//! dimensions is written as the number it is. Every other value - the pieces, their vectors, a filter, a source, a
//! top-k - goes to the database as a parameter, read by the server as its place's type.

use super::{Distance, MAX_DIMENSIONS, MAX_INDEXED, Table};

/// The longest table name: room for `_embedding_idx` within PostgreSQL's 63 bytes.
const MAX_NAME: usize = 48;

fn identifier(name: &str, at: &str) -> Result<String, String> {
    let ok = name.len() <= MAX_NAME
        && name.starts_with(|c: char| c.is_ascii_lowercase() || c == '_')
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if ok {
        Ok(name.to_owned())
    } else {
        Err(format!(
            "{at}: {name:?} is not a table name: small letters, digits and _, starting with a letter, at most \
             {MAX_NAME}"
        ))
    }
}

/// A table's name as a workflow writes it (`pieces`, or `docs.pieces` in a schema), checked.
pub(crate) fn table_name(text: &str, at: &str) -> Result<(Option<String>, String), String> {
    match text.split_once('.') {
        Some((schema, name)) => Ok((Some(identifier(schema, at)?), identifier(name, at)?)),
        None => Ok((None, identifier(text, at)?)),
    }
}

impl Table {
    /// The table, quoted: `"docs"."pieces"`.
    pub(crate) fn quoted(&self) -> String {
        match &self.schema {
            Some(schema) => format!("\"{schema}\".\"{}\"", self.name),
            None => format!("\"{}\"", self.name),
        }
    }

    /// One of its indexes, quoted as the table is (an index lives in its table's schema).
    fn index(&self, what: &str) -> String {
        match &self.schema {
            Some(schema) => format!("\"{schema}\".\"{}_{what}_idx\"", self.name),
            None => format!("\"{}_{what}_idx\"", self.name),
        }
    }

    fn index_name(&self, what: &str) -> String {
        format!("\"{}_{what}_idx\"", self.name)
    }

    /// Which of the table and its vector index are there: `{kept_table, kept_index}`, each a name or null.
    pub(crate) fn present(&self) -> (String, Vec<String>) {
        (
            "SELECT to_regclass($1)::text AS kept_table, to_regclass($2)::text AS kept_index".to_owned(),
            vec![self.quoted(), self.index("embedding")],
        )
    }

    /// What makes the table, with vectors of `dimensions`, and its indexes: the source's, the metadata's (for
    /// filters), and - up to 2,000 dimensions, pgvector's most - the vectors' (HNSW, for the store's distance).
    pub(crate) fn create(&self, dimensions: u32) -> Vec<String> {
        let dimensions = dimensions.clamp(1, MAX_DIMENSIONS);
        let table = self.quoted();
        let mut out = vec![
            "CREATE EXTENSION IF NOT EXISTS vector".to_owned(),
            format!(
                "CREATE TABLE IF NOT EXISTS {table} (id text PRIMARY KEY, source text NOT NULL, position integer \
                 NOT NULL DEFAULT 0, content text NOT NULL, metadata jsonb NOT NULL DEFAULT '{{}}'::jsonb, \
                 embedding vector({dimensions}) NOT NULL, updated_at timestamptz NOT NULL DEFAULT now())"
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS {} ON {table} (source)",
                self.index_name("source")
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS {} ON {table} USING gin (metadata jsonb_path_ops)",
                self.index_name("metadata")
            ),
        ];
        if dimensions <= MAX_INDEXED {
            out.push(format!(
                "CREATE INDEX IF NOT EXISTS {} ON {table} USING hnsw (embedding {})",
                self.index_name("embedding"),
                self.distance.class()
            ));
        }
        out
    }

    /// Adds pieces, or replaces those whose id is there: `$1` the pieces, a JSON list of `{id, source, position,
    /// content, metadata, embedding}` (the vector as pgvector writes one, `[0.1,0.2]`).
    pub(crate) fn upsert(&self) -> String {
        format!(
            "INSERT INTO {} (id, source, position, content, metadata, embedding, updated_at) SELECT p.id, p.source, \
             p.position, p.content, COALESCE(p.metadata, jsonb_build_object()), p.embedding::vector, now() FROM \
             jsonb_to_recordset($1::jsonb) AS p(id text, source text, position integer, content text, metadata \
             jsonb, embedding text) ON CONFLICT (id) DO UPDATE SET source = EXCLUDED.source, position = \
             EXCLUDED.position, content = EXCLUDED.content, metadata = EXCLUDED.metadata, embedding = \
             EXCLUDED.embedding, updated_at = now()",
            self.quoted()
        )
    }

    /// Removes the pieces of the sources in `$1` (a JSON list) whose id is not in `$2` (a JSON list): what a
    /// document no longer has, once its new pieces are in.
    pub(crate) fn remove_stale(&self) -> String {
        format!(
            "DELETE FROM {} WHERE source IN (SELECT jsonb_array_elements_text($1::jsonb)) AND id NOT IN (SELECT \
             jsonb_array_elements_text($2::jsonb))",
            self.quoted()
        )
    }

    /// Removes every piece of the sources in `$1` (a JSON list).
    pub(crate) fn delete_sources(&self) -> String {
        format!(
            "DELETE FROM {} WHERE source IN (SELECT jsonb_array_elements_text($1::jsonb))",
            self.quoted()
        )
    }

    /// The `$3` pieces nearest the vector `$1`. With a filter (`$2`, a JSON record every piece found holds in its
    /// metadata), every matching piece is compared - an exact search, found through the metadata's index - so a
    /// narrow filter still finds its top-k; without one, the vector index finds the nearest, as pgvector's do.
    pub(crate) fn search(&self, filtered: bool) -> String {
        let op = self.distance.operator();
        let (filter, order) = if filtered {
            // `+ 0` keeps the planner from the vector index, which would look at its nearest few before filtering.
            (
                "WHERE metadata @> $2::jsonb ",
                format!("(embedding {op} $1::vector) + 0"),
            )
        } else {
            ("", format!("embedding {op} $1::vector"))
        };
        let limit = if filtered { "$3" } else { "$2" };
        format!(
            "SELECT id, source, position, content, metadata, (embedding {op} $1::vector)::float8 AS distance FROM {} \
             {filter}ORDER BY {order} LIMIT {limit}::integer",
            self.quoted()
        )
    }
}

impl Distance {
    pub(crate) fn operator(self) -> &'static str {
        match self {
            Self::Cosine => "<=>",
            Self::L2 => "<->",
            Self::InnerProduct => "<#>",
        }
    }

    fn class(self) -> &'static str {
        match self {
            Self::Cosine => "vector_cosine_ops",
            Self::L2 => "vector_l2_ops",
            Self::InnerProduct => "vector_ip_ops",
        }
    }

    /// A score from a distance, higher for nearer: cosine similarity (1 - the cosine distance), the inner product
    /// (pgvector gives its negative), and 1 / (1 + the Euclidean distance).
    pub(crate) fn score(self, distance: f64) -> f64 {
        match self {
            Self::Cosine => 1.0 - distance,
            Self::InnerProduct => -distance,
            Self::L2 => 1.0 / (1.0 + distance.max(0.0)),
        }
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Cosine => "cosine",
            Self::L2 => "l2",
            Self::InnerProduct => "inner_product",
        }
    }
}

/// A vector as pgvector reads one: `[0.1,0.2]`; refused when a number is not one a vector can hold.
pub(crate) fn vector_text(vector: &[f64]) -> Result<String, String> {
    let mut out = String::with_capacity(vector.len() * 10 + 2);
    out.push('[');
    for (i, x) in vector.iter().enumerate() {
        if !x.is_finite() || x.abs() > f64::from(f32::MAX) {
            return Err(format!("the vector's number {i} ({x}) is not one a vector holds"));
        }
        if i > 0 {
            out.push(',');
        }
        out.push_str(&x.to_string());
    }
    out.push(']');
    Ok(out)
}

/// The number of rows a command tag says: `INSERT 0 3` and `DELETE 3` are 3.
pub(crate) fn rows_of(tag: Option<&str>) -> u64 {
    tag.and_then(|t| t.rsplit(' ').next())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(text: &str, distance: Distance) -> Table {
        let (schema, name) = table_name(text, "table").unwrap();
        Table {
            schema,
            name,
            distance,
        }
    }

    #[test]
    fn a_table_name_is_letters_digits_and_underscores_only() {
        assert_eq!(
            table_name("doc_pieces", "t").unwrap(),
            (None, "doc_pieces".to_owned())
        );
        assert_eq!(
            table_name("kb.pieces_2", "t").unwrap(),
            (Some("kb".to_owned()), "pieces_2".to_owned())
        );
        for bad in [
            "",
            "Pieces",
            "2pieces",
            "pie ces",
            "x\"; DROP TABLE users; --",
            "a.b.c",
            "a.",
            ".a",
            "pi\u{e9}ces",
            &"x".repeat(49),
        ] {
            let e = table_name(bad, "steps[0].table").unwrap_err();
            assert!(e.starts_with("steps[0].table: "), "{bad:?}: {e}");
        }
    }

    #[test]
    fn statements_quote_the_checked_name_and_take_every_value_as_a_parameter() {
        let t = table("kb.pieces", Distance::Cosine);
        assert_eq!(t.quoted(), "\"kb\".\"pieces\"");
        let made = t.create(1536);
        assert_eq!(made[0], "CREATE EXTENSION IF NOT EXISTS vector");
        assert!(made[1].starts_with("CREATE TABLE IF NOT EXISTS \"kb\".\"pieces\" (id text PRIMARY KEY"));
        assert!(made[1].contains("embedding vector(1536) NOT NULL"), "{}", made[1]);
        assert_eq!(
            made[4],
            "CREATE INDEX IF NOT EXISTS \"pieces_embedding_idx\" ON \"kb\".\"pieces\" USING hnsw (embedding \
             vector_cosine_ops)"
        );
        // Past 2,000 dimensions pgvector indexes no vector: the table is made, searched exactly.
        assert_eq!(t.create(3072).len(), 4);
        assert_eq!(
            t.present().1,
            ["\"kb\".\"pieces\"", "\"kb\".\"pieces_embedding_idx\""]
        );
        for sql in [
            t.upsert(),
            t.remove_stale(),
            t.delete_sources(),
            t.search(true),
            t.search(false),
        ] {
            assert!(sql.contains("$1"), "{sql}");
            assert!(!sql.contains('\''), "no value written into it: {sql}");
        }
        assert_eq!(
            t.search(true),
            "SELECT id, source, position, content, metadata, (embedding <=> $1::vector)::float8 AS distance FROM \
             \"kb\".\"pieces\" WHERE metadata @> $2::jsonb ORDER BY (embedding <=> $1::vector) + 0 LIMIT $3::integer"
        );
        assert_eq!(
            table("p", Distance::L2).search(false),
            "SELECT id, source, position, content, metadata, (embedding <-> $1::vector)::float8 AS distance FROM \
             \"p\" ORDER BY embedding <-> $1::vector LIMIT $2::integer"
        );
        assert!(table("p", Distance::InnerProduct).create(3)[4].ends_with("(embedding vector_ip_ops)"));
    }

    #[test]
    fn scores_are_higher_for_nearer_pieces() {
        assert!((Distance::Cosine.score(0.25) - 0.75).abs() < 1e-12);
        assert!((Distance::InnerProduct.score(-3.0) - 3.0).abs() < 1e-12);
        assert!(Distance::L2.score(0.0) > Distance::L2.score(2.0));
    }

    #[test]
    fn vectors_are_written_as_pgvector_reads_them() {
        assert_eq!(vector_text(&[0.5, -1.0, 0.000_1]).unwrap(), "[0.5,-1,0.0001]");
        assert!(vector_text(&[f64::NAN]).is_err());
        assert!(vector_text(&[1e300]).is_err());
        assert_eq!(rows_of(Some("INSERT 0 12")), 12);
        assert_eq!(rows_of(Some("DELETE 3")), 3);
        assert_eq!(rows_of(None), 0);
    }
}
