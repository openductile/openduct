# X (Twitter)

Posts on [X](https://x.com): written, answered, found, liked and reposted.

## Registering this installation with X (once, by its admin)

1. In the [X developer portal](https://developer.x.com), make a project and an app (X's plans decide how many posts
   it may write and read each month).
2. Under the app's **User authentication settings**, turn on OAuth 2.0 as a **Web App** with **Read and write**
   permissions, and give the callback URI shown in OpenDuct under **Settings → App sign-ins**
   (`https://<your studio>/oauth/callback`).
3. Copy the OAuth 2.0 **Client ID** and **Client Secret** into **Settings → App sign-ins → X (Twitter)**.

## Connecting an account

Under Credentials, add an X credential and **Connect**. OpenDuct keeps the token and the account's id (liking and
reposting name it), and renews the token every two hours as X asks, keeping each new refresh token.

## Actions

- **Post** (`post`, the default): text, or a reply to a post.
- **Remove a post** (`delete_post`).
- **Find recent posts** (`find_posts`): what a search finds among the last 7 days' posts, 100 a page, up to 5 pages;
  more of their fields if asked (`created_at,author_id,public_metrics`).
- **Like a post** (`like`) and **Repost a post** (`repost`).
- **Who is signed in** (`me`).

Not yet: pictures and videos (X takes them as uploads first), and direct messages.

## Tested

Recorded exchanges built from X's API v2 reference (`tests/`).

Not run against a live X account.
