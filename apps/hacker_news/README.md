# Hacker News

Stories, comments and people of [Hacker News](https://news.ycombinator.com): its front page and lists, a story with
all its comments, and a search of everything posted - for a daily digest, or an alert when a name or subject comes
up.

Written by hand, in our own words, from two published APIs:

- Hacker News's official API, <https://github.com/HackerNews/API> at commit 8a0528f538bc (MIT licence), at
  `hacker-news.firebaseio.com/v0`: items, users and the story lists.
- Hacker News Search by Algolia, the search Hacker News's own pages link to, <https://hn.algolia.com/api> (read
  2026-11-27; its source, algolia/hn-search, is MIT licence), at `hn.algolia.com/api/v1`: search, the front page with
  each story's points and comments, and a story with its comment tree.

## Signing in

- **No sign-in** (`hacker_news`): both APIs are open to all. A step still names a credential, so make one of this
  kind; it holds nothing.

## Actions

- **Search Hacker News** (`search`, the default): stories (or comments, Ask HN, Show HN, one author's posts with
  `author_<name>`, one story's comments with `story_<id>`) that match the words given, the most relevant first or the
  newest first, with at least `min_points` or posted `since` a time; 20 a page unless `hits_per_page` says.
- **Get the front page** (`front_page`): its stories now, with title, address, author, points and comment count.
- **List the ids of top, new or best stories** (`list_story_ids`): one of Hacker News's own lists, in its order (top,
  new, best, ask, show or job), as ids; look each up with **Look up a story or comment** (`get_item`).
- **Get a story with its comments** (`get_story_with_comments`): the story and its whole comment tree.
- **Look up a person** (`get_user`): when they joined, their karma, their about text and what they posted.

Every action only reads, so a preview runs it for real.

## Triggers

- **A new story** (`new_story`): each story posted (or only those with the words given; or comments, with `tags:
  comment`), asked every 5 minutes, newest first.
- **A story on the front page** (`front_page_story`): each story as it reaches the front page, asked every 15 minutes.

The first poll of each notes what is there and runs nothing.

## Not done

- Posting, voting and commenting: Hacker News's API only reads.
- The official lists give ids only: each item is a request of its own (a workflow's loop over `get_item`).
- The live change feed (`/v0/updates`) and the largest id (`/v0/maxitem`).

## Tested

Recorded exchanges written from the two APIs' references with made-up stories and people (`tests/`): every action,
a list other than the top one, a newest-first search of comments with two filters, a search refused (400), and both
triggers (a first poll, and new items run oldest first). Not run against the live services.

```sh
openduct app check apps/hacker_news --bar 80
openduct app test apps/hacker_news
```
