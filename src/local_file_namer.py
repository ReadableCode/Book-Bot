# %%
# Imports #

import os

from tqdm import tqdm

from local_database_postgres import query_postgres
from utils.display_tools import pprint_df, pprint_dict, pprint_ls  # noqa

# %%
# Variables #


project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
local_books_dir = os.path.join("U:\\", "Books")
print(local_books_dir)

# list dir
ls_books = os.listdir(local_books_dir)
pprint_ls(ls_books)

dict_vars = {}


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
# List Builders #


def get_authors_list():
    """
    Get a list of all authors
    """
    key = "list_authors"
    if key in dict_vars:
        return dict_vars[key].copy()

    print("Buillding list of authors...")
    # get a list of all unique author names from table
    query = """
    SELECT DISTINCT name FROM authors
    """

    df_authors = query_postgres(query)
    ls_authors = df_authors["name"].tolist()

    dict_vars[key] = ls_authors.copy()

    return ls_authors


# %%
# Functions #


def check_if_valid_book(dict_meta_data):
    file_extension = dict_meta_data.get("file_type", "")
    if file_extension not in ["pdf", "epub", "mobi"]:
        print(f"Invalid file type: {file_extension}")
        return False

    author = dict_meta_data.get("author", "")
    if author == "":
        print("Author not found.")
        return False

    return True


def get_author_from_path(path, tqdm=False):
    ls_authors = get_authors_list()
    if tqdm:
        # tqdm for all authors
        for auth in tqdm(ls_authors, desc="Checking Authors"):
            if auth in path:
                author = auth
                return author
    else:
        for auth in ls_authors:
            if auth in path:
                author = auth
                return author

    return ""


def get_metadata_from_path(path):
    """
    Get the parts of a path.
    """
    author = ""
    series = ""
    title = ""
    copy_num = ""

    author = get_author_from_path(path)
    if author == "":
        return {}

    parts = os.path.normpath(path).split(os.sep)
    print(parts)

    # get file extension
    file_ext = parts[-1].split(".")[-1]

    if len(parts) >= 1:
        author = parts[0]
    if len(parts) >= 2:
        series = parts[1]
    if len(parts) >= 3:
        title = parts[2]
    if len(parts) >= 4:
        copy_num = parts[3]

    # convert to dictionary
    dict_meta_data = {
        "path": path,
        "author": author,
        "series": series,
        "title": title,
        "file_type": file_ext,
        "copy_num": copy_num,
    }

    return dict_meta_data


# loop through all paths and get parts

# Walk recursively and get first matching path
for root, dirs, files in os.walk(local_books_dir):
    if files:
        rel_path = os.path.relpath(os.path.join(root, files[0]), local_books_dir)
        dict_book_metadata = get_metadata_from_path(rel_path)
        valid_book = check_if_valid_book(dict_book_metadata)

        if valid_book:
            pprint_dict(dict_book_metadata)
            break


# %%
