# Facebook and Instagram

Posts and photos on a [Facebook](https://www.facebook.com) Page, posts on an Instagram business account linked to
it, replies to comments, and insights - through Meta's Graph API.

## Signing in

An **access token** for the Page (and its Instagram account):

- In [Meta for Developers](https://developers.facebook.com/apps), make an app of the **Business** type with
  Facebook Login for Business, and give it the permissions it needs: `pages_manage_posts`, `pages_read_engagement`
  and `pages_manage_engagement` for a Page; `instagram_basic`, `instagram_content_publish` and
  `instagram_manage_comments` for Instagram; `read_insights` (or `ads_read` for an ad account) for insights.
- In **Business Settings → Users → System users**, add a system user, give it the Page (and the Instagram account),
  and **Generate new token** for the app with those permissions. A system user's token does not run out as a
  person's does.

Requests go to Graph API **v25.0** (which Meta supports until July 2028); a credential may name a later one
(`v26.0`).

## Actions

- **Post to a Facebook Page** (`post_to_page`, the default): text, a link, now or at a later time.
- **Post a photo to a Facebook Page** (`post_photo_to_page`): a picture on the web, with its text.
- **Prepare an Instagram post** (`make_instagram_media`), then **Publish an Instagram post**
  (`publish_instagram_media`): Instagram fetches the picture or video (public, on the web) and gives a container id,
  which publishing takes. A video may need a minute: **Check an Instagram post** (`check_instagram_media`) says when
  it is `FINISHED`.
- **Reply to a comment** (`reply_to_comment`): under a comment on a Facebook post, or (on_instagram) an Instagram
  one.
- **Get insights** (`get_insights`): a Page's, post's or Instagram account's numbers by metric and period, or an ad
  account's by fields; every page.
- **Read from the Graph API** (`read`): any object or list it has, such as a Page's posts.

Not yet: carousels of several pictures, uploading files (pictures and videos are fetched from the web), Messenger,
and the Instagram API with Instagram Login (graph.instagram.com).

## Tested

Recorded exchanges built from Meta's Graph API, Pages API and Instagram content publishing documentation (`tests/`).

Not run against a live Page or Instagram account.
