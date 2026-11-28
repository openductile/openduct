// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The places a person connects (Dropbox, Box, OneDrive and SharePoint, Google Drive): a credential that holds an
//! OAuth sign-in, made by the studio's Connect with the install's own registration with the vendor, as an app's
//! sign-in is. Its access token goes with each request; one that runs out (or that the vendor no longer takes) is
//! refreshed with the credential's refresh token and kept in the credential, as an app step's is. A Box developer
//! token (`box_token`) goes as it is: it lasts an hour and is not refreshed.

use super::web::{self, Answer, Budget};
use crate::apps::Credential;
use crate::run::{Effects, HttpRequest};

/// A credential's token, sent with each request.
pub(crate) struct Bearer<'a> {
    effects: &'a dyn Effects,
    /// The credential's name: a refreshed token is kept in it.
    name: String,
    credential: Credential,
    /// The vendor, as its errors name it.
    pub vendor: &'static str,
    pub budget: Budget,
}

impl std::fmt::Debug for Bearer<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bearer")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl<'a> Bearer<'a> {
    /// The token of credential `name`, refreshed first when it has run out.
    pub(crate) async fn open(
        effects: &'a dyn Effects,
        name: &str,
        credential: Credential,
        vendor: &'static str,
    ) -> Result<Self, String> {
        let mut bearer = Self {
            effects,
            name: name.to_owned(),
            credential,
            vendor,
            budget: Budget::default(),
        };
        if let Some(sign_in) = bearer.sign_in()
            && crate::app_files::oauth::wanted(sign_in, &bearer.credential, false)
        {
            bearer.refresh().await?;
        }
        if bearer.token().is_empty() {
            return Err(format!(
                "{} holds no sign-in: connect it again under Credentials",
                bearer.name
            ));
        }
        Ok(bearer)
    }

    /// The app sign-in the credential is of; none for a developer token.
    fn sign_in(&self) -> Option<&'static crate::app_files::SignIn> {
        crate::app_files::by_credential(&self.credential.kind)
            .map(|(_, s)| s)
            .filter(|s| s.oauth2.is_some())
    }

    fn token(&self) -> String {
        let field = if self.credential.kind == "box_token" {
            "token"
        } else {
            "access_token"
        };
        self.credential.fields.get(field).cloned().unwrap_or_default()
    }

    /// A fresh token, kept in the credential.
    async fn refresh(&mut self) -> Result<(), String> {
        let Some((app, sign_in)) = crate::app_files::by_credential(&self.credential.kind) else {
            return Err(format!("{}: its sign-in has run out", self.name));
        };
        let registration = self.effects.registration(&app.id);
        let request = crate::app_files::oauth::refresh(sign_in, &self.credential, registration.as_ref())
            .map_err(|e| format!("{}: {e}", self.name))?;
        self.budget.spend(self.vendor)?;
        let answer = self.effects.http(request).await?;
        let tokens = crate::app_files::oauth::tokens(&answer, &self.credential)
            .map_err(|e| format!("{}: {e}", self.name))?;
        self.effects.keep_credential(&self.name, tokens.clone()).await?;
        self.credential.fields.extend(tokens);
        Ok(())
    }

    /// Sends `request` with the token; a token the vendor no longer takes (401) is refreshed once and the request
    /// sent again.
    pub(crate) async fn send(&mut self, request: HttpRequest) -> Result<Answer, String> {
        let again = request.clone();
        let answer = self.once(request).await?;
        if answer.status == 401
            && let Some(sign_in) = self.sign_in()
            && crate::app_files::oauth::wanted(sign_in, &self.credential, true)
        {
            self.refresh().await?;
            return self.once(again).await;
        }
        if answer.status == 401 {
            return Err(format!(
                "{} refused {}'s sign-in: connect it again under Credentials",
                self.vendor, self.name
            ));
        }
        Ok(answer)
    }

    async fn once(&mut self, mut request: HttpRequest) -> Result<Answer, String> {
        self.budget.spend(self.vendor)?;
        request
            .headers
            .push(("authorization".to_owned(), format!("Bearer {}", self.token())));
        web::send(self.effects, request, self.vendor).await
    }

    /// A JSON request: `body` sent as JSON (none for a GET); the answer.
    pub(crate) async fn json(
        &mut self,
        method: &str,
        url: String,
        body: Option<&serde_json::Value>,
    ) -> Result<Answer, String> {
        let body = body.map(|b| ("application/json".to_owned(), b.to_string().into_bytes()));
        let headers = vec![("accept".to_owned(), "application/json".to_owned())];
        self.send(web::request(method, url, headers, body)).await
    }
}
