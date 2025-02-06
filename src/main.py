# %%
# Imports #


import os

from local_database_postgres import query_postgres
from utils.display_tools import pprint_df, pprint_dict, pprint_ls  # noqa

# %%
# Variables #


project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
book_data_dir = os.path.join("F:\\", "book-data")
print(book_data_dir)

verbose = False
data_dumps_url = "https://openlibrary.org/developers/dumps"
COMMIT_EVERY_ROW_NUM = 100000
MAX_ROWS_TO_READ = None  # Set to None to read all rows


# %%
# Sample Functions #


df_authors_sample = query_postgres("SELECT * FROM authors LIMIT 10;")
print("Authors:")
pprint_df(df_authors_sample)

df_works_sample = query_postgres("SELECT * FROM works LIMIT 10;")
print("Works:")
pprint_df(df_works_sample)

df_works_authors_sample = query_postgres("SELECT * FROM work_authors LIMIT 10;")
print("Work Authors:")
pprint_df(df_works_authors_sample)


# %%
# Search Functions #


def get_authors_from_string_parts(search_string):
    """
    Gets a result of authors where each part of string parts is in the author name
    """

    # generate query where each part of string is in a like statemnt
    ls_string_parts = search_string.split(" ")

    query = f"""
    SELECT * FROM authors
    WHERE {' AND '.join([f"name ILIKE '%{part}%'" for part in ls_string_parts])}
    """

    print(query)

    return query_postgres(query)


search_string = "orson scott card"
# search_string = "Orson Scott Card"

df_authors = get_authors_from_string_parts(search_string)

print("Authors:")
pprint_df(df_authors)


# %%


def get_books_from_string_parts(search_string):
    """
    Gets a result of books where each part of string parts is in the title
    """

    ls_string_parts = [part.replace("'", "''") for part in search_string.split(" ")]

    query = f"""
    SELECT * FROM works
    WHERE {' AND '.join([f"title ILIKE '%%{part}%%'" for part in ls_string_parts])}
    """

    print(query)

    return query_postgres(query)


search_string = "ender's game"
# search_string = "Ender's Game"

df_books = get_books_from_string_parts(search_string)

print("Books:")
pprint_df(df_books)


# %%


def get_books_with_authors_by_title(search_string):
    """
    Fetches books matching the search string in their title,
    along with their respective authors.
    """
    ls_string_parts = [
        part.replace("'", "''") for part in search_string.split(" ")
    ]  # Escape single quotes

    query = f"""
    SELECT DISTINCT w.work_key, w.title, a.author_key, a.name
    FROM works w
    JOIN work_authors wa ON w.work_key = wa.work_key
    JOIN authors a ON wa.author_key = a.author_key
    WHERE {' AND '.join([f"w.title ILIKE '%%{part}%%'" for part in ls_string_parts])}
    ORDER BY w.title, a.name;
    """

    print(query)

    return query_postgres(query)


# Example usage
search_string = "ender's game"  # Contains an apostrophe
df_results = get_books_with_authors_by_title(search_string)

print("Books and Their Authors:")
pprint_df(df_results)


# %%
