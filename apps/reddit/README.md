# Reddit

Posts, comments and private messages on [Reddit](https://www.reddit.com): post to a community, reply, read a
community's newest, top and popular posts, search, and start a workflow for each new post or message.

Written by hand from Reddit's own reference, fetched 2026-11-27: the API reference at <https://www.reddit.com/dev/api>
and its OAuth2 guide at <https://github.com/reddit-archive/reddit/wiki/OAuth2>. Only their facts are used, in our own
words.

## Signing in

Reddit asks for its approval before an app may use its API (its Responsible Builder Policy, since November 2025): the
installation's admin needs an approved app at <https://www.reddit.com/prefs/apps>, of the **web app** kind for a
sign-in.

- **Sign in with Reddit** (a Reddit sign-in): the app's redirect URI is the callback address shown under **Settings →
  App sign-ins** (`https://<your studio>/oauth/callback`); copy its client id (under its name) and secret there. A
  workspace then **Connects** under Credentials. It asks for the scopes `identity`, `read`, `submit`, `edit`,
  `history`, `privatemessages` and `save`, and for a lasting sign-in (`duration=permanent`): Reddit's tokens last an
  hour, and are renewed.
- **An app's id and secret** (reading only): the app's own token, with no person signed in - for reading communities,
  posts and searches. Posting, replying and messages need a sign-in.

Every request says who it is with a User-Agent of its own, as Reddit asks.

## Actions

- **Post to a community** (`submit_post`, the default): a text post, or a link when an address is given; flair, adult
  and spoiler marks.
- **Comment or reply** (`comment`): on a post, to a comment, or to a private message - by its full name (`t3_...`,
  `t1_...`, `t4_...`).
- **Change a post's or comment's text** (`edit_text`), **Send a private message** (`send_message`), **Save a post or
  comment** (`save`).
- **List a community's newest posts** (`list_new_posts`), **top posts** (`list_top_posts`, over an hour to all time)
  and **popular posts** (`list_hot_posts`): every page, up to 1,000.
- **Search posts** (`search_posts`): in one community or everywhere, by relevance, new, top ...
- **Read a post and its comments** (`get_post_comments`), **Look up posts or comments** (`get_things`), **Look up a
  community** (`get_subreddit`), **Who is signed in** (`get_me`).
- **List messages and replies** (`list_inbox`: all, or only the unread, left unread), **List a user's posts**
  (`list_user_posts`) and **comments** (`list_user_comments`).

A post or comment refused (a community that does not exist, a title too long, posting too often) fails the step in
Reddit's words, with its code - Reddit answers those with 200.

## Triggers

- **A new post in a community** (`new_post`): the newest asked for every 5 minutes.
- **A new message or reply** (`new_message`): the inbox, every 5 minutes.

Reddit has no webhooks for these: polling is how a workflow learns of them.

## Not done

- Removing a post (its endpoint is not in the current reference), voting (Reddit's rules leave votes to people),
  images and videos (an upload), moderation, flair and wiki pages, live threads.

## Tested

Recorded exchanges built from the API reference (`tests/`): each action, pages by `after`, a refusal answered with 200
and one with 403, the app's own token, and both triggers. Not run against Reddit.

```sh
openduct app check apps/reddit --bar 80
openduct app test apps/reddit
```
