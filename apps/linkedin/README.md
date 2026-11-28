# LinkedIn

Posts on [LinkedIn](https://www.linkedin.com), as yourself or as a company page you look after.

## Registering this installation with LinkedIn (once, by its admin)

1. At [linkedin.com/developers/apps](https://www.linkedin.com/developers/apps), **Create app** (LinkedIn asks for a
   company page it belongs to).
2. Under **Products**, add **Sign In with LinkedIn using OpenID Connect** and **Share on LinkedIn**. To post as a
   company page, LinkedIn must also grant the app the **Community Management API** (`w_organization_social`).
3. Under **Auth**, add the redirect URL shown in OpenDuct under **Settings → App sign-ins**
   (`https://<your studio>/oauth/callback`), and copy the **Client ID** and **Primary Client Secret** into
   **Settings → App sign-ins → LinkedIn**.

## Connecting an account

Under Credentials, add a LinkedIn credential and **Connect**: **Sign in with LinkedIn** to post as yourself, or
**Sign in with LinkedIn, for company pages too** (it asks also for `w_organization_social`, which only an app LinkedIn
has approved may ask for). OpenDuct keeps the token, and the member's id (from LinkedIn's `userinfo`) to post as them.
LinkedIn's tokens last 60 days and, for most apps, cannot be renewed: connect again when LinkedIn stops taking it.

## Actions

- **Post** (`post`, the default): text, as you or as a company page (its organization id), with a link to an article
  (its title and a line about it) if given; gives the post's id (`urn:li:share:...`).
- **Remove a post** (`delete_post`): by its id.
- **Who is signed in** (`me`): the member's id, name and picture.

Not yet: posts with pictures, videos or documents (LinkedIn takes those as uploads first), and reading posts (a
permission LinkedIn grants to approved apps only).

## Tested

Recorded exchanges built from LinkedIn's Posts API and OpenID Connect documentation (`tests/`), on its version
`202611`.

Not run against a live LinkedIn account: the sign-in (LinkedIn's handling of the PKCE values OpenDuct sends) is
untried.
