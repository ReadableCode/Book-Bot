# %%
# Imports #

import os

from local_database_postgres import query_postgres
from utils.display_tools import pprint_df, pprint_dict, pprint_ls  # noqa

# %%
# Variables #


project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
local_books_dir = os.path.join("Z:\\", "Books")
print(local_books_dir)

# list dir
ls_books = os.listdir(local_books_dir)

dict_vars = {}

ls_blocked_authors = [
    "",
    "Jack Reacher",
]
ls_blocked_authors = [auth.lower() for auth in ls_blocked_authors]


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
    SELECT DISTINCT LOWER(name) AS name, LENGTH(LOWER(name)) AS name_len
    FROM authors
    ORDER BY name_len DESC
    """

    df_authors = query_postgres(query)
    # fillna
    df_authors = df_authors.fillna("")
    ls_authors = df_authors["name"].tolist()

    dict_vars[key] = ls_authors.copy()

    return ls_authors


def get_books_by_author(author_name):
    """
    Fetches all books by a given author.
    """
    query = f"""
    SELECT w.work_key, w.title
    FROM works w
    JOIN work_authors wa
    ON w.work_key = wa.work_key
    JOIN authors a
    ON wa.author_key = a.author_key
    WHERE a.name ILIKE '%{author_name}%'
    ORDER BY w.title;
    """

    df = query_postgres(query)

    list_books_by_author = df["title"].tolist()

    return list_books_by_author


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
        if auth in ls_blocked_authors:
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
    if file_extension not in ["pdf", "epub", "mobi"]:
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

    # convert to dictionary
    dict_meta_data = {
        "path": linux_rel_path,
        "author": author,
        "series": series,
        "title": title,
        "file_type": file_extension,
    }

    return dict_meta_data


# %%
# Main #

if __name__ == "__main__":
    # Walk recursively and get first matching path
    for root, dirs, files in os.walk(local_books_dir):
        if root == local_books_dir and "Calibre-library" in dirs:
            dirs.remove("Calibre-library")

        if files:
            rel_path = os.path.relpath(os.path.join(root, files[0]), local_books_dir)
            dict_book_metadata = get_metadata_from_path(rel_path)
            valid_book = check_if_valid_book(dict_book_metadata)

            if valid_book:
                pprint_dict(dict_book_metadata)
                break


# %%
