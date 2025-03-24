# %%
# Imports #

import os

from local_database_postgres import (
    get_authors_list,
    get_books_by_author,
    get_series_by_author,
)
from utils.display_tools import pprint_df, pprint_dict, pprint_ls  # noqa

# %%
# Variables #


project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

ls_possible_book_dirs = [
    os.path.join("Y:\\", "Books"),
    os.path.join("U:\\", "Books"),
]


for book_dir in ls_possible_book_dirs:
    if os.path.isdir(os.path.join(book_dir, "Calibre-library")):
        local_books_dir = os.path.join(book_dir, "Calibre-library")
        break
else:
    raise FileNotFoundError("Books directory not found.")

print(local_books_dir)

ls_remove_non_authors = [
    "",
    "Jack Reacher",
]
ls_remove_non_authors = [auth.lower() for auth in ls_remove_non_authors]


# %%
# Folder Structure #


"""
C:.
├───Author
│   ├───Series ( if any )
│   │   ├───Book Title
│   │   └───Book Title
"""


# %%
# Functions #


def check_if_valid_book(dict_meta_data):
    author = dict_meta_data.get("author", "")
    if author == "":
        print("Author not found.")
        return False

    return True


def get_author_from_path(path):
    ls_authors = get_authors_list()
    print(f"Checking path: {path} for author")

    for auth in ls_authors:
        if len(auth) < 8:
            continue
        if auth in ls_remove_non_authors:
            continue
        if auth in path.lower():
            author = auth.title()
            print(f"Found author: {author}")
            return author

    print(f"Author not found in path: {path}")
    return ""


def get_metadata_from_path(path):
    """
    Get the parts of a path.
    """
    # get file path relative to book directory in linux format
    linux_rel_path = os.path.normpath(path).replace("\\", "/")

    file_extension = linux_rel_path.split(".")[-1]
    print(f"Checking file type: {file_extension}")
    if file_extension not in ["pdf", "epub", "mobi", "azw3", "opf"]:
        print(f"Invalid file type: {file_extension}")
        return {}

    author = get_author_from_path(linux_rel_path)
    if author == "":
        return {}

    ls_titles_by_author = list(set(get_books_by_author(author)))
    print(f"Titles by {author}:")
    pprint_ls(ls_titles_by_author)

    series = ""
    title = ""

    for title in ls_titles_by_author:
        if title.lower() in linux_rel_path.lower():
            print(f"Found title: {title}")
            break
    else:
        print(f"Title not found in path: {linux_rel_path}")
        return {}

    dict_meta_data = {
        "path": linux_rel_path,
        "author": author,
        "series": series,
        "title": title,
        "file_type": file_extension,
    }

    return dict_meta_data


# %%


author_name = "Sarah J. Maas"
ls_series_by_author = get_series_by_author(author_name)

print(f"Series by {author_name}:")
pprint_ls(ls_series_by_author)


# %%

author_name = "Sarah J. Maas"
author_name = "orson scott card"
books_by_author = get_books_by_author(author_name)

print(f"Books by {author_name}:")
pprint_ls(books_by_author)


# %%


test_path = "Sarah J. Maas/A Court of Frost and Starlight (A Court of Thorns and Roses) (799)/A Court of Frost and Starlight (A Court of - Sarah J. Maas.epub"

print(f"Getting metadata from path: {test_path}")
print(get_metadata_from_path(test_path))


# %%
# Main #

ls_skip_dirs = [
    "Calibre-library",
    "Calibre-books",
]

if __name__ == "__main__":
    for root, dirs, files in os.walk(local_books_dir):
        if os.path.normpath(root) == os.path.normpath(local_books_dir):
            dirs[:] = [d for d in dirs if d not in ls_skip_dirs]

        print(f"Checking root: {root}")
        if files:
            rel_path = os.path.relpath(os.path.join(root, files[0]), local_books_dir)
            dict_book_metadata = get_metadata_from_path(rel_path)
            valid_book = check_if_valid_book(dict_book_metadata)

            if valid_book:
                pprint_dict(dict_book_metadata)
                break


# %%
