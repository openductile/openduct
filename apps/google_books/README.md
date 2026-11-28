# Google Books

Find books in [Google Books](https://books.google.com), read their details, and keep them on your bookshelves.

Written by hand from Google's own description of the Books API v1 - its discovery document,
<https://www.googleapis.com/discovery/v1/apis/books/v1/rest> (revision 20261119, fetched 2026-11-27) - and its
reference pages, <https://developers.google.com/books/docs/v1/reference> and
<https://developers.google.com/books/docs/v1/using> (Google's developer pages: CC BY 4.0). Descriptions are our own.

## Signing in

- **Sign in with Google** (`google_books`): everything, the signed-in user's bookshelves included.
- **An API key** (`google_books_key`): searching, a book's details, and anyone's public bookshelves - not your own
  library. In the [Google Cloud console](https://console.cloud.google.com), turn on the **Books API**, then
  **APIs & Services → Credentials → Create credentials → API key** (restrict it to the Books API). Sent as `key=`.

### Registering this installation with Google (once, by its admin, for the sign-in)

1. In the Google Cloud console, make (or pick) a project and turn on the **Books API**.
2. **APIs & Services → OAuth consent screen**: set it up, with the scope `https://www.googleapis.com/auth/books`.
3. **APIs & Services → Credentials → Create credentials → OAuth client ID**: a **Web application**, with the
   authorised redirect URI shown in OpenDuct under **Settings → App sign-ins** (`https://<your studio>/oauth/callback`).
4. Copy its **Client ID** and **Client secret** into **Settings → App sign-ins → Google Books**. One OAuth client
   serves every Google app of this installation: give it each app's scope.

## Actions

- **Search for books** (`search_books`): as Google Books searches - words, `intitle:`, `inauthor:`, `inpublisher:`,
  `subject:`, `isbn:`; newest or most relevant first; books or magazines; free, paid or previewable e-books; one
  language. Up to 200 results (5 pages of 40).
- **Get a book** (`get_book`): title, authors, publisher, date, description, ISBNs, pages, categories, links.
- **List my bookshelves** (`list_bookshelves`) and **Get one of my bookshelves** (`get_bookshelf`).
- **List the books on one of my bookshelves** (`list_bookshelf_books`): every page.
- **Put a book on one of my bookshelves** (`add_to_bookshelf`), **Take a book off one of my bookshelves**
  (`remove_from_bookshelf`), **Move a book on one of my bookshelves** (`move_on_bookshelf`), **Empty one of my
  bookshelves** (`clear_bookshelf`).
- **List someone's public bookshelves** (`list_public_bookshelves`) and **List the books on someone's public
  bookshelf** (`list_public_bookshelf_books`).

Shelves are numbered: 0 Favorites, 1 Purchased, 2 To read, 3 Reading now, 4 Have read, 5 Reviewed, 6 Recently
viewed, 7 My eBooks, 8 Books for you; a shelf of your own is above 1000.

## Triggers

- **A book put on one of my bookshelves** (`new_book_on_shelf`), asked every 15 minutes: each book added to a shelf.

## Not done

- Reading positions, annotations and layers, uploaded books, family sharing and the account's settings: they serve
  Google's own reading apps.
- Google Books has no webhooks; the trigger polls.

## Tested

Recorded exchanges from the requests and answers Google documents (`tests/`), with made-up values: both sign-ins, a
search over two pages and one that finds nothing, the trigger's poll. Not run against a live account.

```sh
openduct app check apps/google_books --bar 80
openduct app test apps/google_books
```
