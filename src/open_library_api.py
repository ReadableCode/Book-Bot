# %%
# Imports #

import requests

from utils.display_tools import pprint_df, pprint_dict, pprint_ls  # noqa: F401

# %%
# Variables #


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
