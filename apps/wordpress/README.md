# WordPress

Posts and pages of a [WordPress](https://wordpress.org) site: written, changed and found, through its REST API.

## Signing in

- **The site's address**, such as `https://blog.example.com`.
- **A user name and an application password**: in the site's admin, **Users → Profile → Application Passwords**, add
  one (WordPress 5.6 and later; the site must be on https). Its spaces may stay. The user's role decides what it may
  do: an Author publishes their own posts, an Editor anyone's.

## Actions

- **Write a post** (`add_post`, the default): title and content (text or HTML), kept as a draft unless the status
  says `publish` (or `future`, with a date); categories and tags by their ids; a picture from the media library.
- **Change a post** (`update_post`): only what is given changes.
- **Look up a post** (`get_post`).
- **Find posts** (`list_posts`): by words, status, date or category; 100 a page, every page up to 1,000, as WordPress
  says how many pages there are.
- **Write a page** (`add_page`): a page of the site, under another if given.

Not yet: uploading pictures to the media library (it takes a file), and the WordPress.com hosted API.

## Tested

Recorded exchanges built from the WordPress REST API handbook (`tests/`).

Not run against a live WordPress site.
