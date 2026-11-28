// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `openduct app from-openapi` and `openduct app from-mcp`: an app's folder made from a vendor's API description or
//! from the tools of its MCP server - a checked definition, a README and recorded tests - with what it does not do
//! listed, and its quality report. A description or a server named by its address is reached as an HTTP step
//! reaches one: this machine's own network refused unless `--allow-private-network`.

use std::path::{Path, PathBuf};
use std::time::Duration;

use openduct_expr::Value;
use openduct_flow::app_files::{self, generate, quality};
use openduct_flow::{Credential, Effects, HttpRequest, Live};

/// How long fetching a description, or listing a server's tools, may take.
const FETCH: Duration = Duration::from_secs(60);

/// What `from-openapi` is given.
pub(crate) struct FromOpenapi {
    pub(crate) spec: String,
    pub(crate) id: String,
    pub(crate) options: generate::Options,
    pub(crate) out: Option<PathBuf>,
    pub(crate) force: bool,
    pub(crate) allow_private: bool,
}

/// What `from-mcp` is given.
pub(crate) struct FromMcp {
    pub(crate) url: String,
    pub(crate) options: generate::McpOptions,
    pub(crate) token_file: Option<PathBuf>,
    pub(crate) out: Option<PathBuf>,
    pub(crate) force: bool,
    pub(crate) allow_private: bool,
}

/// The text at `url`, fetched as an HTTP step fetches it.
async fn fetch(url: &str, allow_private: bool) -> Result<String, String> {
    let live = Live::new("127.0.0.1:9092", allow_private)?;
    let request = HttpRequest {
        read: true,
        method: "GET".to_owned(),
        url: url.to_owned(),
        headers: vec![(
            "accept".to_owned(),
            "application/json, application/yaml, text/yaml, */*".to_owned(),
        )],
        query: Vec::new(),
        body: None,
        timeout: FETCH,
    };
    let raw = live
        .http_bytes(request)
        .await
        .map_err(|e| format!("{url}: {e}"))?;
    let status = match &raw.answer {
        Value::Record(r) => r.get("status").map(openduct_flow::as_text).unwrap_or_default(),
        _ => String::new(),
    };
    if !status.starts_with('2') {
        return Err(format!("{url}: the address answered {status}"));
    }
    let bytes = raw.body.unwrap_or_default();
    if bytes.len() > generate::MAX_SPEC {
        return Err(format!("{url}: the description is too large"));
    }
    String::from_utf8(bytes).map_err(|_| format!("{url}: the description is not UTF-8 text"))
}

/// Writes a made app's files into `folder`: its definition, README and recorded tests.
fn write(made: &generate::Generated, folder: &Path, force: bool) -> Result<(), String> {
    if folder.join("app.yaml").exists() && !force {
        return Err(format!(
            "{} has an app already: --force writes over its app.yaml, README.md and the tests made",
            folder.display()
        ));
    }
    std::fs::create_dir_all(folder.join("tests"))
        .map_err(|e| format!("cannot make {}: {e}", folder.display()))?;
    // Written over: the recordings made before go (those written by hand stay).
    if let Ok(entries) = std::fs::read_dir(folder.join("tests")) {
        for file in entries.filter_map(Result::ok).map(|e| e.path()) {
            let made =
                std::fs::read_to_string(&file).is_ok_and(|t| t.starts_with("# Made by `openduct app from-"));
            if made {
                std::fs::remove_file(&file).map_err(|e| format!("cannot remove {}: {e}", file.display()))?;
            }
        }
    }
    let put = |file: PathBuf, text: &str| {
        std::fs::write(&file, text).map_err(|e| format!("cannot write {}: {e}", file.display()))
    };
    put(folder.join("app.yaml"), &made.app_yaml)?;
    put(folder.join("README.md"), &made.readme)?;
    for (file, text) in &made.tests {
        put(folder.join("tests").join(file), text)?;
    }
    Ok(())
}

/// Says what was made: where, its quality, what it does not do, and what to run next.
fn said(made: &generate::Generated, folder: &Path) -> Result<(), String> {
    let source = folder.join("app.yaml").display().to_string();
    if app_files::find(&made.id).is_some_and(|a| a.source != source) {
        println!(
            "! {} is an app this build carries: in an installation's --apps-dir, yours takes its place",
            made.id
        );
    }
    let app = app_files::add(&made.app_yaml, &source)?;
    let report = quality::report(app, &made.tests, true);
    println!(
        "Made {}: {} action{}, {} recorded test{}",
        folder.display(),
        app.actions.len(),
        if app.actions.len() == 1 { "" } else { "s" },
        made.tests.len(),
        if made.tests.len() == 1 { "" } else { "s" }
    );
    for line in report.lines() {
        println!("  {line}");
    }
    if !made.not_done.is_empty() {
        println!("  not done ({}): see README.md", made.not_done.len());
        for n in made.not_done.iter().take(8) {
            println!("    - {n}");
        }
    }
    println!("Next:");
    println!("  openduct app check {}", folder.display());
    println!("  openduct app test {}", folder.display());
    Ok(())
}

pub(crate) async fn from_openapi(given: FromOpenapi) -> Result<(), String> {
    let FromOpenapi {
        spec,
        id,
        mut options,
        out,
        force,
        allow_private,
    } = given;
    let remote = spec.starts_with("https://") || spec.starts_with("http://");
    let text = if remote {
        fetch(&spec, allow_private).await?
    } else {
        let path = Path::new(&spec);
        let size = std::fs::metadata(path)
            .map_err(|e| format!("cannot read {spec}: {e}"))?
            .len();
        if size > u64::try_from(generate::MAX_SPEC).unwrap_or(u64::MAX) {
            return Err(format!("{spec}: the description is too large"));
        }
        std::fs::read_to_string(path).map_err(|e| format!("cannot read {spec}: {e}"))?
    };
    options.id = id.clone();
    if options.source.is_empty() {
        options.source = if remote {
            spec.clone()
        } else {
            Path::new(&spec)
                .file_name()
                .map_or_else(|| spec.clone(), |f| f.to_string_lossy().into_owned())
        };
    }
    if remote && options.origin.is_none() {
        options.origin = reqwest_origin(&spec);
    }
    if options.made_on.is_empty() {
        options.made_on = generate::today();
    }
    let made = tokio::task::spawn_blocking(move || generate::from_openapi(&text, &options))
        .await
        .map_err(|_| "the description could not be read".to_owned())??;
    let folder = out.unwrap_or_else(|| PathBuf::from("apps").join(&id));
    write(&made, &folder, force)?;
    said(&made, &folder)
}

/// The origin of an address (`https://host:port`).
fn reqwest_origin(url: &str) -> Option<String> {
    let (scheme, rest) = url.split_once("://")?;
    let host = rest.split(['/', '?', '#']).next()?;
    Some(format!("{scheme}://{host}"))
}

pub(crate) async fn from_mcp(given: FromMcp) -> Result<(), String> {
    let FromMcp {
        url,
        mut options,
        token_file,
        out,
        force,
        allow_private,
    } = given;
    let credential = match &token_file {
        Some(path) => {
            let fields = crate::secrets_file::read(path)?;
            let token = fields
                .get("token")
                .cloned()
                .ok_or_else(|| format!("{}: holds no token=", path.display()))?;
            Some(Credential {
                kind: "bearer".to_owned(),
                fields: [("token".to_owned(), token)].into_iter().collect(),
            })
        }
        None => None,
    };
    let live = Live::new("127.0.0.1:9092", allow_private)?;
    let server = openduct_flow::mcp::Server {
        url: url.clone(),
        credential,
        timeout: FETCH,
    };
    let mut session = openduct_flow::mcp::Session::new(&live, server);
    let listed = session.tools().await;
    session.close().await;
    let tools = match listed? {
        openduct_flow::mcp::Reached::Answered(tools) => tools,
        openduct_flow::mcp::Reached::NotSent(_) => {
            return Err("the server's tools were not asked for".to_owned());
        }
    };
    options.url = url;
    if options.made_on.is_empty() {
        options.made_on = generate::today();
    }
    let made = generate::from_mcp(&tools, &options)?;
    let folder = out.unwrap_or_else(|| PathBuf::from("apps").join(&options.id));
    write(&made, &folder, force)?;
    said(&made, &folder)
}

/// The quality of the app in `folder` (read already as `app`): its recordings and README looked at.
pub(crate) fn quality_of(app: &app_files::AppDef, folder: &Path) -> quality::Report {
    let mut recordings = Vec::new();
    if let Ok(entries) = std::fs::read_dir(folder.join("tests")) {
        let mut files: Vec<PathBuf> = entries.filter_map(Result::ok).map(|e| e.path()).collect();
        files.retain(|f| f.extension().is_some_and(|e| e == "yaml"));
        files.sort();
        for f in files {
            if let Ok(text) = std::fs::read_to_string(&f) {
                recordings.push((f.display().to_string(), text));
            }
        }
    }
    quality::report(app, &recordings, folder.join("README.md").is_file())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn an_origin_is_its_scheme_and_host() {
        assert_eq!(
            reqwest_origin("https://api.x.example/v1/spec.json").as_deref(),
            Some("https://api.x.example")
        );
    }
}
