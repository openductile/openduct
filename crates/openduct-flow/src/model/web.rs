// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Reading a `web` step (see [`crate::web`]): its settings are the web module's to check, with this workflow's
//! templates and expressions.

use serde_json::Value as Json;

use super::{Kind, Reader};

impl Reader {
    pub(super) fn web(&self, body: &Json, at: &str) -> Result<Kind, String> {
        let step = crate::web::settings::parse(body, at, &|v, a| self.template(v, a), &|v, a| {
            self.expression(v, a)
        })?;
        Ok(Kind::Web(Box::new(step)))
    }
}
