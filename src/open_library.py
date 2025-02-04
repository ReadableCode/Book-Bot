# %%
# Imports #

import glob
import gzip
import json
import os
import shutil
import sqlite3

import pandas as pd
import requests
from tqdm import tqdm

from utils.display_tools import pprint_df, pprint_dict, pprint_ls  # noqa

# %%
# Variables #

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
book_data_dir = os.path.join(os.path.expanduser("~"), "book-data")
print(book_data_dir)

verbose = False
data_dumps_url = "https://openlibrary.org/developers/dumps"


# %%
# Book Data #


def extract_file(file_path):
    """
    Extract a gzip file to a text file.

    Parameters:
        file_path (str): The path to the gzip file.

    Returns:
        str: The path to the extracted text file.
    """
    with gzip.open(file_path, "rb") as f_in:
        with open(file_path.replace(".gz", ""), "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    return file_path.replace(".gz", "")


# %%
# Extract or Load Authors Data #

ls_authors_files_in_book_data = glob.glob(
    os.path.join(book_data_dir, "ol_dump_authors*.txt")
)

# if text file exists use it
if len(ls_authors_files_in_book_data) > 0:
    print("Found text file for authors, skipping extraction")
    authors_text_file_path = sorted(ls_authors_files_in_book_data)[0]
    print(f"Text file path: {authors_text_file_path}")
else:
    ls_gz_files = glob.glob(os.path.join(book_data_dir, "*.gz"))
    if len(ls_gz_files) > 0:
        gzip_path = sorted(ls_gz_files)[0]
        print(f"Gzip path is: {gzip_path}")
        authors_text_file_path = extract_file(gzip_path)
        print(f"Extraction complete. Text file path: {authors_text_file_path}")
    else:
        raise ValueError("Missing any file type")


# %%
# Extract or Load Works Data #

ls_works_files_in_book_data = glob.glob(
    os.path.join(book_data_dir, "ol_dump_works*.txt")
)

# if text file exists use it
if len(ls_works_files_in_book_data) > 0:
    print("Found text file for works, skipping extraction")
    works_text_file_path = sorted(ls_works_files_in_book_data)[0]
    print(f"works_text_file_path: {works_text_file_path}")
else:
    ls_gz_files = glob.glob(os.path.join(book_data_dir, "*.gz"))
    if len(ls_gz_files) > 0:
        gzip_path = sorted(ls_gz_files)[0]
        print(f"Gzip path is: {gzip_path}")
        works_text_file_path = extract_file(gzip_path)
        print(f"Extraction complete. Text file path: {works_text_file_path}")
    else:
        raise ValueError("Missing any file type")


# %%
# Generate sqlite database #

sqlite_file_path = os.path.join(book_data_dir, "book_data.db")
conn = sqlite3.connect(sqlite_file_path)
cursor = conn.cursor()


# ensure tables
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS authors (
        author_key TEXT PRIMARY KEY,
        revision INTEGER,
        last_modified TEXT,
        name TEXT,
        source_records TEXT,
        latest_revision INTEGER,
        created TEXT
    )
"""
)
conn.commit()

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS works (
        work_key TEXT PRIMARY KEY,
        revision INTEGER,
        last_modified TEXT,
        title TEXT,
        created TEXT,
        covers TEXT,
        latest_revision INTEGER,
        authors TEXT
    )
"""
)

# create authors works table
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS work_authors (
        work_key TEXT,
        author_key TEXT,
        FOREIGN KEY (work_key) REFERENCES works(work_key),
        FOREIGN KEY (author_key) REFERENCES authors(author_key),
        PRIMARY KEY (work_key, author_key)
    );
"""
)


# %%
# Book Data: Authors #

row_counter = 0
# read the first few lines of the text file
with open(authors_text_file_path, "r") as f:
    for line in tqdm(f, desc="Importing records"):
        line = line.strip()
        if not line:
            continue
        # The file is tab-separated. The columns are:
        # 0: Type (/type/author)
        # 1: Author key (e.g. /authors/OL10000278A)
        # 2: Revision number (e.g. 3)
        # 3: Timestamp (e.g. 2021-12-26T21:22:34.663256)
        # 4: JSON blob with the record details
        parts = line.split("\t")

        if len(parts) < 5:
            # Skip lines that don't have enough columns.
            break

        line_type = parts[0]
        line_key = parts[1]
        line_revision = parts[2]
        line_last_modified = parts[3]
        line_json_blob = parts[4]

        try:
            record = json.loads(line_json_blob)
        except Exception as e:
            print(f"JSON parse error for line_key: {line_key}: {e}")
            continue

        if verbose:
            print("---------------- Line Details ----------------")
            print(f"line_type: {line_type}")
            print(f"line_key: {line_key}")
            print(f"line_revision: {line_revision}")
            print(f"line_last_modified: {line_last_modified}")
            print(f"line_json_blob: {line_json_blob}")

            pprint_dict(record)

        name = record.get("name", "")
        source_records = json.dumps(record.get("source_records", []))
        latest_revision = record.get("latest_revision", "")
        created = record.get("created", {}).get("value", "")

        if verbose:
            print(f"name: {name}")
            print(f"source_records: {source_records}")
            print(f"created: {created}")

        query = """
        INSERT INTO authors (
            author_key, revision, last_modified, name, 
            source_records, latest_revision, created
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(author_key)
        DO UPDATE SET
            revision = excluded.revision,
            last_modified = excluded.last_modified,
            name = excluded.name,
            source_records = excluded.source_records,
            latest_revision = excluded.latest_revision,
            created = excluded.created
        """

        if verbose:
            print("query")
            print(query)

        cursor.execute(
            query,
            (
                line_key,
                line_revision,
                line_last_modified,
                name,
                source_records,
                latest_revision,
                created,
            ),
        )

        row_counter += 1
        if row_counter % 100 == 0:
            print(f"Row count: {row_counter}")
        if row_counter % 1000 == 0:
            conn.commit()

    conn.commit()


# %%
# Book Data: Works #


row_counter = 0
# read the first few lines of the text file
with open(works_text_file_path, "r") as f:
    for line in tqdm(f, desc="Importing records"):
        line = line.strip()
        if not line:
            continue
        # The file is tab-separated. The columns are:
        # 0: Type (/type/work)
        # 1: Work key (e.g. /works/OL10000278W)
        # 2: Revision number (e.g. 3)
        # 3: Timestamp
        # 4: JSON blob with the record details
        parts = line.split("\t")

        if len(parts) < 5:
            # Skip lines that don't have enough columns.
            break

        line_type = parts[0]
        line_key = parts[1]
        line_revision = parts[2]
        line_last_modified = parts[3]
        line_json_blob = parts[4]

        try:
            record = json.loads(line_json_blob)
        except Exception as e:
            print(f"JSON parse error for line_key: {line_key}: {e}")
            continue

        if verbose:
            print("---------------- Line Details ----------------")
            print(f"line_type: {line_type}")
            print(f"line_key: {line_key}")
            print(f"line_revision: {line_revision}")
            print(f"line_last_modified: {line_last_modified}")
            print(f"line_json_blob: {line_json_blob}")

            pprint_dict(record)

        title = record.get("title", "")
        created = record.get("created", {}).get("value", "")
        covers = json.dumps(record.get("covers", []))
        latest_revision = record.get("latest_revision", "")
        authors = json.dumps(record.get("authors", []))

        authors_list = record.get("authors", [])

        for author in authors_list:
            author_key = author.get("author", {}).get("key")
            if author_key:
                cursor.execute(
                    """
                    INSERT INTO work_authors (work_key, author_key) 
                    VALUES (?, ?) 
                    ON CONFLICT(work_key, author_key) DO NOTHING
                    """,
                    (line_key, author_key),
                )

        if verbose:
            print(f"title: {title}")
            print(f"created: {created}")
            print(f"covers: {covers}")
            print(f"latest_revision: {latest_revision}")
            print(f"authors: {authors}")

        query = """
        INSERT INTO works (
            work_key, revision, last_modified, title, 
            created, covers, latest_revision, authors
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(work_key)
        DO UPDATE SET
            revision = excluded.revision,
            last_modified = excluded.last_modified,
            title = excluded.title,
            created = excluded.created,
            covers = excluded.covers,
            latest_revision = excluded.latest_revision,
            authors = excluded.authors
        """

        if verbose:
            print("Executing query:")
            print(query)

        cursor.execute(
            query,
            (
                line_key,
                line_revision,
                line_last_modified,
                title,
                created,
                covers,
                latest_revision,
                authors,
            ),
        )

        row_counter += 1
        if row_counter % 100 == 0:
            print(f"Rows processed: {row_counter}")
        if row_counter % 1000 == 0:
            conn.commit()

    conn.commit()


# %%
# Query Data #

# select all authors
sql_authors = """
SELECT
    *
FROM
    authors
LIMIT
    10
"""

authors_df = pd.read_sql_query(sql_authors, conn)
print("Authors")
pprint_df(authors_df.head())

# select all works
sql_works = """
SELECT
    *
FROM
    works
LIMIT 10
"""

works_df = pd.read_sql_query(sql_works, conn)
print("Works")
pprint_df(works_df.head())

# select all work authors
sql_work_authors = """
SELECT
    *
FROM
    work_authors
LIMIT 10
"""
work_authors_df = pd.read_sql_query(sql_work_authors, conn)
print("Work Authors")
pprint_df(work_authors_df.head())

# get all works with their authors
sql_works_authors = """
SELECT w.work_key, w.title, a.author_key, a.name
FROM works w
JOIN work_authors wa ON w.work_key = wa.work_key
JOIN authors a ON wa.author_key = a.author_key
"""
works_authors_df = pd.read_sql_query(sql_works_authors, conn)
print("Works with Authors")
pprint_df(works_authors_df.head())

# get all works by a specefic author
author_id = "/authors/OL111136A"
sql_works_by_author = f"""
SELECT w.work_key, w.title
FROM works w
JOIN work_authors wa ON w.work_key = wa.work_key
WHERE wa.author_key = '{author_id}'
"""
works_by_author_df = pd.read_sql_query(sql_works_by_author, conn)
print("Works by Author")
pprint_df(works_by_author_df.head())

# find all authors of a specific work
work_id = "/works/OL1079322W"
sql_authors_by_work = f"""
SELECT a.author_key, a.name
FROM authors a
JOIN work_authors wa ON a.author_key = wa.author_key
WHERE wa.work_key = '{work_id}'
"""
authors_by_work_df = pd.read_sql_query(sql_authors_by_work, conn)
print("Authors of Work")
pprint_df(authors_by_work_df.head())


def find_authors_by_work_title(conn, search_query):
    """Find authors for works whose title contains all words in the search query."""

    # Split input into keywords (assuming input is space-separated words)
    keywords = search_query.split()

    # Start SQL query
    sql_authors_by_work = """
    SELECT a.author_key, a.name, w.work_key, w.title
    FROM authors a
    JOIN work_authors wa ON a.author_key = wa.author_key
    JOIN works w ON wa.work_key = w.work_key
    WHERE
    """

    # Dynamically add LIKE conditions for each keyword
    conditions = " AND ".join(["w.title LIKE ?" for _ in keywords])

    sql_authors_by_work += conditions  # Append conditions to SQL
    sql_authors_by_work += ";"  # End SQL query

    # Prepare wildcard parameters for LIKE queries
    like_params = [f"%{word}%" for word in keywords]

    # Execute query and return DataFrame
    authors_by_work_df = pd.read_sql_query(
        sql_authors_by_work, conn, params=like_params
    )

    return authors_by_work_df


work_title_parts = "Biology for grammar"
df = find_authors_by_work_title(conn, "work_title_parts")
print(f"Authors of Works Matching Search Query: {work_title_parts}")
pprint_df(df)


def find_works_by_author_name(conn, search_query):
    """Find works by authors whose name contains all words in the search query."""

    # Split input into keywords (assuming input is space-separated words)
    keywords = search_query.split()

    # Start SQL query
    sql_works_by_author = """
    SELECT w.work_key, w.title, a.author_key, a.name
    FROM works w
    JOIN work_authors wa ON w.work_key = wa.work_key
    JOIN authors a ON wa.author_key = a.author_key
    WHERE
    """

    # Dynamically add LIKE conditions for each keyword
    conditions = " AND ".join(["a.name LIKE ?" for _ in keywords])

    sql_works_by_author += conditions  # Append conditions to SQL
    sql_works_by_author += ";"  # End SQL query

    # Prepare wildcard parameters for LIKE queries
    like_params = [f"%{word}%" for word in keywords]

    # Execute query and return DataFrame
    works_by_author_df = pd.read_sql_query(
        sql_works_by_author, conn, params=like_params
    )

    return works_by_author_df


author_name_parts = "Science Masters"
df = find_works_by_author_name(conn, author_name_parts)
print(f"Works by Authors Matching Search Query: {author_name_parts}")
pprint_df(df)


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
