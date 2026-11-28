// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! What a step can tell of its SQL before it runs, read as each database reads it - strings, quoted names and
//! comments set aside: how many values it takes (`?`, `$1`, `@p1`), and whether it only reads.
//!
//! The read-only check is a guard beside the database's own (a read-only transaction, or one rolled back): one
//! statement, starting with a word that reads (SELECT, WITH ...), and none of the words that write or reach outside
//! anywhere in it. It refuses some SQL that only reads (a column named `update`, say, unless quoted); it cannot know
//! what a function does. A database user that may only read is the sure way.

use super::Db;

/// The SQL as code: strings, quoted names and comments blanked to spaces, so what is left is the SQL's own words.
fn code(sql: &str, db: Db, backslash: bool) -> Result<String, String> {
    let chars: Vec<char> = sql.chars().collect();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    let at = |i: usize| chars.get(i).copied();
    let word = |c: char| c.is_alphanumeric() || c == '_' || c == '$';
    while let Some(c) = at(i) {
        let next = at(i + 1);
        // A comment to the end of the line: `--` (MySQL: followed by a space), `#` in MySQL.
        let line_comment =
            (c == '-' && next == Some('-') && (db != Db::Mysql || at(i + 2).is_none_or(char::is_whitespace)))
                || (c == '#' && db == Db::Mysql);
        if line_comment {
            while at(i).is_some_and(|c| c != '\n') {
                i += 1;
                out.push(' ');
            }
            continue;
        }
        if c == '/' && next == Some('*') {
            // MySQL runs what is in `/*! ... */` (and reads hints in `/*+ ... */`): that is code.
            if db == Db::Mysql && matches!(at(i + 2), Some('!' | '+')) {
                i += 3;
                out.push_str("   ");
                while at(i).is_some_and(|c| c.is_ascii_digit()) {
                    i += 1;
                    out.push(' ');
                }
                continue;
            }
            let mut depth = 0;
            loop {
                match (at(i), at(i + 1)) {
                    (Some('/'), Some('*')) => {
                        depth += 1;
                        i += 2;
                        out.push_str("  ");
                        // MySQL's comments do not nest.
                        if db == Db::Mysql && depth > 1 {
                            depth = 1;
                        }
                    }
                    (Some('*'), Some('/')) => {
                        depth -= 1;
                        i += 2;
                        out.push_str("  ");
                        if depth == 0 {
                            break;
                        }
                    }
                    (Some(_), _) => {
                        i += 1;
                        out.push(' ');
                    }
                    (None, _) => return Err("the SQL has a comment (/*) that is not closed".to_owned()),
                }
            }
            continue;
        }
        if db == Db::Mysql && c == '*' && next == Some('/') {
            // The end of an executable comment.
            i += 2;
            out.push_str("  ");
            continue;
        }
        // PostgreSQL's dollar quotes: $$ ... $$, $tag$ ... $tag$.
        if db == Db::Postgres && c == '$' && !at(i.wrapping_sub(1)).is_some_and(word) {
            let mut j = i + 1;
            while at(j).is_some_and(|c| c.is_alphanumeric() || c == '_') {
                j += 1;
            }
            let is_tag = at(j) == Some('$') && !at(i + 1).is_some_and(|c| c.is_ascii_digit());
            if is_tag {
                let tag: String = chars.get(i..=j).map(|t| t.iter().collect()).unwrap_or_default();
                let rest: String = chars.get(j + 1..).map(|t| t.iter().collect()).unwrap_or_default();
                let Some(end) = rest.find(&tag) else {
                    return Err("the SQL has a $$ quote that is not closed".to_owned());
                };
                let skipped = tag.chars().count() * 2 + rest.get(..end).map_or(0, |s| s.chars().count());
                i += skipped;
                out.extend(std::iter::repeat_n(' ', skipped));
                continue;
            }
        }
        let close = match c {
            '\'' => Some('\''),
            '"' => Some('"'),
            '`' if db == Db::Mysql => Some('`'),
            '[' if db == Db::SqlServer => Some(']'),
            _ => None,
        };
        if let Some(close) = close {
            // Backslashes escape in MySQL's strings (unless the server is told otherwise), and in PostgreSQL's E'...'.
            let escapes = c != '['
                && c != '`'
                && ((db == Db::Mysql && backslash)
                    || (db == Db::Postgres && c == '\'' && matches!(at(i.wrapping_sub(1)), Some('e' | 'E'))));
            i += 1;
            out.push(' ');
            loop {
                match at(i) {
                    None => return Err("the SQL has a quote that is not closed".to_owned()),
                    Some('\\') if escapes => {
                        i += 2;
                        out.push_str("  ");
                    }
                    Some(q) if q == close && at(i + 1) == Some(close) => {
                        i += 2;
                        out.push_str("  ");
                    }
                    Some(q) if q == close => {
                        i += 1;
                        out.push(' ');
                        break;
                    }
                    Some(_) => {
                        i += 1;
                        out.push(' ');
                    }
                }
            }
            continue;
        }
        out.push(c);
        i += 1;
    }
    Ok(out)
}

/// How many values the SQL takes: MySQL's `?`s; the highest `$n` (PostgreSQL) or `@pn` (SQL Server).
pub(crate) fn places(sql: &str, db: Db) -> Result<usize, String> {
    let code = code(sql, db, true)?;
    let chars: Vec<char> = code.chars().collect();
    let word =
        |c: Option<&char>| c.is_some_and(|c| c.is_alphanumeric() || matches!(c, '_' | '@' | '$' | '#'));
    let mut most = 0;
    for (i, c) in chars.iter().enumerate() {
        let number = |from: usize| -> Option<usize> {
            let digits: String = chars
                .get(from..)?
                .iter()
                .take_while(|d| d.is_ascii_digit())
                .collect();
            let after = chars.get(from + digits.len());
            (!digits.is_empty() && !word(after))
                .then(|| digits.parse().ok())
                .flatten()
        };
        match db {
            Db::Mysql if *c == '?' => most += 1,
            Db::Postgres if *c == '$' && !word(i.checked_sub(1).and_then(|p| chars.get(p))) => {
                most = most.max(number(i + 1).unwrap_or(0));
            }
            Db::SqlServer
                if *c == '@'
                    && matches!(chars.get(i + 1), Some('p' | 'P'))
                    && !word(i.checked_sub(1).and_then(|p| chars.get(p))) =>
            {
                most = most.max(number(i + 2).unwrap_or(0));
            }
            _ => {}
        }
    }
    Ok(most)
}

/// What a placeholder looks like, for messages.
pub(crate) fn placeholder(db: Db) -> &'static str {
    match db {
        Db::Mysql => "?",
        Db::Postgres => "$1, $2 ...",
        Db::SqlServer => "@p1, @p2 ...",
    }
}

/// The words a read may start with.
fn reads(db: Db) -> &'static [&'static str] {
    match db {
        Db::Postgres => &["SELECT", "WITH", "VALUES", "TABLE", "SHOW", "EXPLAIN"],
        Db::Mysql => &[
            "SELECT", "WITH", "VALUES", "TABLE", "SHOW", "DESCRIBE", "DESC", "EXPLAIN",
        ],
        Db::SqlServer => &["SELECT", "WITH"],
    }
}

/// Words that write, change the session, or reach outside the database: never in SQL that only reads.
const WRITES: &[&str] = &[
    "INSERT",
    "UPDATE",
    "DELETE",
    "MERGE",
    "UPSERT",
    "CREATE",
    "ALTER",
    "DROP",
    "TRUNCATE",
    "RENAME",
    "GRANT",
    "REVOKE",
    "DENY",
    "CALL",
    "EXEC",
    "EXECUTE",
    "DO",
    "HANDLER",
    "LOAD",
    "LOCK",
    "UNLOCK",
    "SET",
    "RESET",
    "COPY",
    "INTO",
    "OUTFILE",
    "DUMPFILE",
    "BEGIN",
    "START",
    "COMMIT",
    "ROLLBACK",
    "SAVEPOINT",
    "SAVE",
    "RELEASE",
    "ANALYZE",
    "OPTIMIZE",
    "REPAIR",
    "CHECKPOINT",
    "VACUUM",
    "REINDEX",
    "CLUSTER",
    "REFRESH",
    "LISTEN",
    "NOTIFY",
    "UNLISTEN",
    "PREPARE",
    "DEALLOCATE",
    "KILL",
    "SHUTDOWN",
    "OPENROWSET",
    "OPENQUERY",
    "OPENDATASOURCE",
    "BULK",
    "BACKUP",
    "RESTORE",
    "DBCC",
    "RECONFIGURE",
    "WAITFOR",
    "IMPORT",
    "INSTALL",
    "UNINSTALL",
    "FLUSH",
    "PURGE",
    "SIGNAL",
    "RESIGNAL",
    "DISCARD",
    "COMMENT",
    "WRITETEXT",
    "UPDATETEXT",
    "READTEXT",
    "SECURITY",
    // Functions that act on the server, its files or other sessions.
    "PG_TERMINATE_BACKEND",
    "PG_CANCEL_BACKEND",
    "PG_RELOAD_CONF",
    "PG_ROTATE_LOGFILE",
    "LO_IMPORT",
    "LO_EXPORT",
    "DBLINK",
    "DBLINK_EXEC",
    "PG_READ_FILE",
    "PG_READ_BINARY_FILE",
    "PG_LS_DIR",
    "PG_FILE_WRITE",
    "SET_CONFIG",
    "LOAD_FILE",
    "GET_LOCK",
    "RELEASE_LOCK",
    "RELEASE_ALL_LOCKS",
    "SLEEP",
    "BENCHMARK",
    "PG_SLEEP",
];

fn check_read(code: &str, db: Db) -> Result<(), String> {
    let word_char = |c: char| c.is_alphanumeric() || matches!(c, '_' | '$' | '@' | '#');
    let words: Vec<String> = code
        .split(|c: char| !word_char(c))
        .filter(|w| !w.is_empty())
        .map(str::to_uppercase)
        .collect();
    let statements = code.split(';').filter(|s| !s.trim().is_empty()).count();
    if statements > 1 {
        return Err("only one statement, as it only reads".to_owned());
    }
    let first = words.first().map(String::as_str).unwrap_or_default();
    if !reads(db).contains(&first) {
        return Err(format!(
            "it only reads: the SQL must start with {}",
            reads(db).join(", ")
        ));
    }
    if let Some(w) = words.iter().find(|w| {
        WRITES.contains(&w.as_str())
            || (db == Db::SqlServer && (w.starts_with("XP_") || w.starts_with("SP_")))
    }) {
        return Err(format!(
            "it only reads: the SQL says {w}, which writes or acts outside a read (quote a name that is a word like it)"
        ));
    }
    Ok(())
}

/// Whether `sql` only reads, by the rules above; why not, when it does not.
pub(crate) fn read_only(sql: &str, db: Db) -> Result<(), String> {
    check_read(&code(sql, db, true)?, db)?;
    // MySQL may be set to read backslashes as they are: the SQL must pass read either way.
    if db == Db::Mysql {
        check_read(&code(sql, db, false)?, db)?;
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn values_are_counted_where_each_database_takes_them() {
        assert_eq!(
            places("SELECT * FROM t WHERE a = ? AND b = ?", Db::Mysql).unwrap(),
            2
        );
        assert_eq!(
            places("SELECT '?', `a?`, \"?\" FROM t WHERE a = ? -- ?\n", Db::Mysql).unwrap(),
            1
        );
        assert_eq!(places("SELECT 1 # ?\n, ? /* ? */", Db::Mysql).unwrap(), 1);
        assert_eq!(places("SELECT 'it\\'s ?' , ?", Db::Mysql).unwrap(), 1);
        assert_eq!(
            places("SELECT $2, $1, '$3', $$ $4 $$ FROM t", Db::Postgres).unwrap(),
            2
        );
        assert_eq!(places("SELECT a$1 FROM t", Db::Postgres).unwrap(), 0);
        assert_eq!(
            places("SELECT @p1, @P3, '@p9', [@p8], @@p7, @pa FROM t", Db::SqlServer).unwrap(),
            3
        );
        assert_eq!(places("SELECT @p10x", Db::SqlServer).unwrap(), 0);
        assert!(places("SELECT 'open", Db::Mysql).is_err());
        assert!(places("SELECT /* open", Db::SqlServer).is_err());
        assert!(places("SELECT $q$ open", Db::Postgres).is_err());
    }

    #[test]
    fn reads_pass_and_writes_are_refused() {
        for (sql, db) in [
            ("SELECT * FROM orders WHERE id = ?", Db::Mysql),
            ("  select name, 'delete me' AS note FROM t", Db::Mysql),
            ("WITH x AS (SELECT 1) SELECT * FROM x;", Db::Postgres),
            ("SELECT \"update\" FROM t -- drop it\n", Db::Postgres),
            ("SELECT [delete], created_at FROM t WHERE x = @p1", Db::SqlServer),
            ("SELECT REPLACE(name, 'a', 'b') FROM t", Db::SqlServer),
            ("SHOW TABLES", Db::Mysql),
            ("EXPLAIN SELECT 1", Db::Postgres),
            ("(SELECT 1) UNION (SELECT 2)", Db::Postgres),
        ] {
            assert!(read_only(sql, db).is_ok(), "{sql}: {:?}", read_only(sql, db));
        }
        for (sql, db, says) in [
            ("DELETE FROM t", Db::Mysql, "must start with"),
            ("SELECT 1; DELETE FROM t", Db::Mysql, "one statement"),
            ("SELECT 1 DELETE FROM t", Db::SqlServer, "DELETE"),
            ("SELECT * INTO copy FROM t", Db::SqlServer, "INTO"),
            ("SELECT * FROM t INTO OUTFILE '/tmp/x'", Db::Mysql, "INTO"),
            (
                "WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d",
                Db::Postgres,
                "DELETE",
            ),
            ("SELECT * FROM t FOR UPDATE", Db::Postgres, "UPDATE"),
            ("SELECT 1 /*! ; DROP TABLE t */", Db::Mysql, "one statement"),
            ("SELECT 1 /*!80000 DROP */", Db::Mysql, "DROP"),
            ("SELECT 1 EXEC xp_cmdshell 'dir'", Db::SqlServer, "EXEC"),
            (
                "SELECT * FROM OPENROWSET('x', 'y', 'z')",
                Db::SqlServer,
                "OPENROWSET",
            ),
            (
                "SELECT pg_terminate_backend(42)",
                Db::Postgres,
                "PG_TERMINATE_BACKEND",
            ),
            ("SELECT LOAD_FILE('/etc/passwd')", Db::Mysql, "LOAD_FILE"),
            ("SELECT sp_helpdb", Db::SqlServer, "SP_HELPDB"),
            ("EXPLAIN ANALYZE DELETE FROM t", Db::Postgres, "ANALYZE"),
            // Read with backslashes as they are, the string ends early and DELETE is code.
            ("SELECT '\\' , DELETE , \\'' FROM t", Db::Mysql, "DELETE"),
        ] {
            let e = read_only(sql, db).unwrap_err();
            assert!(e.contains(says), "{sql}: {e}");
        }
    }
}
