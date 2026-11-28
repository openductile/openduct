# Disqus

The comments on your site through [Disqus](https://disqus.com): comments listed, posted, edited, approved, removed and
marked as spam; discussion threads started, read and closed; commenters, emails, addresses and words blocked.

Written by hand from Disqus's API 3.0 reference, <https://disqus.com/api/docs/> (read on 2026-11-27): Disqus
publishes no API description file. Labels and descriptions are our own words.

## Signing in

- **An application's keys and your access token** (`disqus`): register an application at
  <https://disqus.com/api/applications/>; its page gives its **API key** (public key), its **API secret**, and your
  own **access token**, which acts as the application's owner and does not run out. All three go on each request, as
  Disqus's server-side requests do. The owner should moderate the sites the workflows look after.

## Good to know

- A site is a *forum* in Disqus's words, named by its short name (`shop-example` of `shop-example.disqus.com`).
- Disqus answers every call with a `code` (0 when it went well) and a `response`; a refusal's `response` says why.

## Actions

- **List comments** (`list_comments`): The comments on a site (a forum, in Disqus's words), the newest first; approved ones unless asked; every page.
- **List a thread's comments** (`list_thread_comments`): The comments on one page's discussion, the newest first; every page.
- **Look up a comment** (`get_comment`): One comment, with its author.
- **Post a comment** (`post_comment`): A new comment on a thread, or a reply to a comment, as the account that owns the application.
- **Edit a comment** (`edit_comment`): Changes a comment's text (one written by the account, or on a site it moderates).
- **Approve a comment** (`approve_comment`): Approves a comment waiting for it, so it shows on the site.
- **Remove a comment** (`remove_comment`): Deletes a comment.
- **Mark a comment as spam** (`mark_spam`): Marks a comment as spam, which hides it.
- **List threads** (`list_threads`): The discussions of a site, one for each page with comments, the newest first; every page.
- **Look up a thread** (`get_thread`): One page's discussion: its title, address and number of comments.
- **Start a thread** (`create_thread`): A discussion for a page of the site, before anyone comments on it.
- **Close a thread** (`close_thread`): Closes a discussion to new comments.
- **Look up a site** (`get_forum`): A site's settings in Disqus: its name, address, founder and options.
- **Look up a commenter** (`get_user`): A Disqus user's profile, by id or username.
- **Block a commenter, email, address or word** (`block`): Adds a user, an email, an IP address, a word or a domain to a site's blocklist, so their comments are held back.

## Triggers

- **A new comment** (`new_comment`): Each comment posted on the site and shown, the newest asked for every few minutes.
- **A comment waiting for approval** (`comment_to_approve`): Each comment held back for a moderator to approve, the newest asked for every few minutes.

Both are asked every 5 minutes, the newest comments first (Disqus's API reference documents no webhooks).

## Not done

- Signing in at Disqus (OAuth) for other people's accounts: their requests also carry the application's keys, which a sign-in at the vendor does not add yet.
- Lists of several values in one call (several comments approved at once): one at a time here.
- Categories, moderators, reports, votes, exports and imports.

## Tested

Recorded exchanges (`tests/`): what each request must send, and an answer that fits it, made from the vendor's API description and reference. Not run against a live Disqus account.

Triggers are tested with recorded exchanges and deliveries (`tests/trigger_*.yaml`), any signature made as Disqus documents it; none was run against a live account.

```sh
openduct app check apps/disqus --bar 80
openduct app test apps/disqus
```
