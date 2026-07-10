# %%
# Imports #

import os

import requests

from utils.display_tools import pprint_df, pprint_dict, pprint_ls  # noqa: F401

# %%
# Variables #


project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
book_data_dir = os.path.join(project_root, "data")
print(book_data_dir)

# %%
# Functions #


def get_book_info_by_isbn(isbn):
    """
    Query Open Library's API for a book using its ISBN.

    Parameters:
        isbn (str): The ISBN of the book.

    Returns:
        dict or None: The book data if found, otherwise None.
    """
    # Open Library API endpoint for book data
    url = "https://openlibrary.org/api/books"

    # The API expects the ISBN prefixed with 'ISBN:' in the bibkeys parameter.
    params = {"bibkeys": f"ISBN:{isbn}", "format": "json", "jscmd": "data"}

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an exception for HTTP errors
    except requests.RequestException as e:
        print(f"HTTP error occurred: {e}")
        return None

    data = response.json()
    key = f"ISBN:{isbn}"
    if key in data:
        return data[key]
    else:
        print(f"No data found for ISBN: {isbn}")
        return None


def get_googlebooks_info_by_isbn(isbn: str, api_key: str | None = None) -> dict | None:
    """
    Fetch title/author(s)/publishedDate/description/categories/pageCount from Google Books by ISBN.

    Parameters:
        isbn (str): ISBN-10 or ISBN-13
        api_key (str|None): Optional Google Books API key. If None, uses unauthenticated request.

    Returns:
        dict | None: Normalized book info dict, or None if not found.
    """
    url = "https://www.googleapis.com/books/v1/volumes"
    params = {"q": f"isbn:{isbn}", "maxResults": 1}
    if api_key:
        params["key"] = api_key

    try:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"HTTP error occurred: {e}")
        return None

    data = r.json()
    if not data.get("items"):
        return None

    vi = (data["items"][0] or {}).get("volumeInfo") or {}

    return {
        "isbn": isbn,
        "title": vi.get("title"),
        "subtitle": vi.get("subtitle"),
        "authors": vi.get("authors") or [],
        "published_date": vi.get("publishedDate"),
        "description": vi.get("description"),
        "categories": vi.get("categories") or [],
        "page_count": vi.get("pageCount"),
        "publisher": vi.get("publisher"),
        "language": vi.get("language"),
        "google_volume_id": data["items"][0].get("id"),
        "info_link": vi.get("infoLink"),
    }


def get_data_from_ls_isbn_files(ls_file_paths):

    ls_books_in_order = []
    for file_path in ls_file_paths:

        for isbn in open(file_path, "r").read().splitlines():
            print(f"ISBN: {isbn}")

            google_data = get_googlebooks_info_by_isbn(isbn)
            pprint_dict(google_data)

            ls_books_in_order.append(google_data)

    print("\nBooks in order:")
    # for each book print isbn, author, title
    for book in ls_books_in_order:
        if not book:
            continue
        print(
            f"ISBN: {book.get('isbn')} | Author(s): {', '.join(book.get('authors', []))} | Title: {book.get('title')}"
        )
        desc = book.get("description") or ""
        print(f"{' ' * 10} Description: {desc[:1000]}...")


def search_books(query, limit=10):
    """
    Search for books on Open Library using an imperfect query string.

    Parameters:
        query (str): The search term (could be part of a title, author, etc.)
        limit (int): Maximum number of results to return.

    Returns:
        list: A list of dictionaries, each representing a matching book.
    """
    url = "https://openlibrary.org/search.json"
    params = {"q": query, "limit": limit}

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"HTTP error occurred: {e}")
        return []

    data = response.json()
    # The search results are stored in the "docs" key.
    return data.get("docs", [])


# %%
# Main #

if __name__ == "__main__":
    search_string = "Throne of Glass 0.4 - The Assassin and the Empire"
    search_string = "The Assassin and the Empire"

    ls_results = search_books(search_string)

    for item in ls_results:
        print("-----------------")
        pprint_dict(item)


# %%
# Main #

if __name__ == "__main__":
    isbn = "9781599909875"
    book_info = get_book_info_by_isbn(isbn)
    if book_info:
        pprint_dict(book_info)

    search_string = "A Throne of Glass Novella"


# %%
