# Google Business Profile

Reviews of a business on Google Search and Maps, the business's replies to them, and the posts on its profile,
through Google's Business Profile APIs (the My Business API v4, which holds reviews and posts).

Written by hand from Google's reference pages, <https://developers.google.com/my-business/reference/rest> (reviews:
`accounts.locations.reviews`; posts: `accounts.locations.localPosts`; fetched 2026-11-27, CC BY 4.0). Labels and
descriptions are our own words.

## Registering this installation with Google (once, by its admin)

1. Google gives the Business Profile APIs only to projects it has approved: in the
   [Google Cloud console](https://console.cloud.google.com), make (or pick) a project and ask for access with
   Google's [access request form](https://developers.google.com/my-business/content/prereqs). Once approved, turn on
   the **Google My Business API**.
2. **APIs & Services → OAuth consent screen**: set it up, with the scope
   `https://www.googleapis.com/auth/business.manage`.
3. **APIs & Services → Credentials → Create credentials → OAuth client ID**: a **Web application**, with the
   authorised redirect URI shown in OpenDuct under **Settings → App sign-ins** (`https://<your studio>/oauth/callback`).
4. Copy its **Client ID** and **Client secret** into **Settings → App sign-ins → Google Business Profile**.

## Connecting an account

Under Credentials, add a Google Business Profile credential and **Connect**, as a person who owns or manages the
business's profile.

## Actions

Each action names the business by its account's id and its location's id - the numbers in
`accounts/<account id>/locations/<location id>`, as Google's Business Profile tools show them.

- **List reviews** (`list_reviews`), **Look up a review** (`get_review`): with the stars (`ONE` to `FIVE`), the words,
  who wrote it, and any reply.
- **Reply to a review** (`reply_to_review`), **Delete a review's reply** (`delete_reply`).
- **List posts** (`list_posts`), **Look up a post** (`get_post`), **Add a post** (`create_post`: words, a button -
  Book, Order, Shop, Learn more, Sign up or Call - and a photo by its public address), **Delete a post**
  (`delete_post`).

## Triggers

- **A new review** (`new_review`): each review written of the location, asked for every fifteen minutes.

## Not done

- **Listing the accounts and locations**, and changing a location's hours, phone or address: those are on two other
  Google hosts (`mybusinessaccountmanagement.googleapis.com`, `mybusinessbusinessinformation.googleapis.com`), and an
  app reaches its one base address; the ids are given by hand for now.
- Event and offer posts, photos of the location, questions and answers, and performance figures.

## Tested

Recorded exchanges built from Google's documented requests and answers (`tests/`), with made-up reviews. Not run
against a live Business Profile.

```sh
openduct app check apps/google_business_profile
openduct app test apps/google_business_profile
```
