# %%
# Imports #

import json
import os
import sqlite3

import pandas as pd

from utils.display_tools import pprint_df, pprint_dict, pprint_ls  # noqa

# %%
# Variables #

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
book_data_dir = os.path.join("F:\\", "book-data")
print(book_data_dir)

verbose = False
data_dumps_url = "https://openlibrary.org/developers/dumps"

# %%
# Generate sqlite database #


def get_sqlite_db_conn_cursor():
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

    conn.commit()

    return conn, cursor


sqlite_conn, sqlite_cursor = get_sqlite_db_conn_cursor()


# %%
# Book Data: Authors #


def load_db_authors_sqlite(authors_text_file_path, max_rows_to_read=None):
    row_counter = 0
    # read the first few lines of the text file
    with open(authors_text_file_path, "r") as f:
        for line in f:
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

            sqlite_cursor.execute(
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
            if row_counter % 10000 == 0:
                print(f"Authors row count: {row_counter}")
            if row_counter % 10000 == 0:
                sqlite_conn.commit()
            if max_rows_to_read and row_counter >= max_rows_to_read:
                break

        sqlite_conn.commit()

        print("Authors row count updated: ", row_counter)


# %%
# Book Data: Works #


def load_db_works_sqlite(works_text_file_path, max_rows_to_read=None):
    row_counter = 0
    # read the first few lines of the text file
    with open(works_text_file_path, "r") as f:
        for line in f:
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
                # if authors list is string
                if isinstance(author.get("author", {}), str):
                    print(f"author is string: {author.get('author', {})}")
                    author_key = author.get("author", {})
                else:
                    author_key = author.get("author", {}).get("key")
                if author_key:
                    sqlite_cursor.execute(
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

            sqlite_cursor.execute(
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
            if row_counter % 1000 == 0:
                print(f"Works row count: {row_counter}")
            if row_counter % 10000 == 0:
                sqlite_conn.commit()
            if max_rows_to_read and row_counter >= max_rows_to_read:
                break

        sqlite_conn.commit()

        print("Works row count updated: ", row_counter)


# %%
# Query Data #


def get_authors_sample(conn):
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

    return authors_df


def get_works_sample(conn):
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

    return works_df


def get_work_authors_sample(conn):
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

    return work_authors_df


def get_works_authors_sample(conn):
    # get all works with their authors
    sql_works_authors = """
    SELECT w.work_key, w.title, a.author_key, a.name
    FROM works w
    JOIN work_authors wa ON w.work_key = wa.work_key
    JOIN authors a ON wa.author_key = a.author_key
    LIMIT 10
    """
    works_authors_df = pd.read_sql_query(sql_works_authors, conn)
    print("Works with Authors")
    pprint_df(works_authors_df.head())

    return works_authors_df


def get_books_for_author_id(conn, author_id):
    # get all works by a specefic author
    sql_works_by_author = f"""
    SELECT w.work_key, w.title
    FROM works w
    JOIN work_authors wa ON w.work_key = wa.work_key
    WHERE wa.author_key = '{author_id}'
    """
    works_by_author_df = pd.read_sql_query(sql_works_by_author, conn)

    return works_by_author_df


def get_authors_for_book_id(conn, work_id):
    # find all authors of a specific work
    sql_authors_by_work = f"""
    SELECT a.author_key, a.name
    FROM authors a
    JOIN work_authors wa ON a.author_key = wa.author_key
    WHERE wa.work_key = '{work_id}'
    """
    authors_by_work_df = pd.read_sql_query(sql_authors_by_work, conn)
    print("Authors of Work")
    pprint_df(authors_by_work_df.head())

    return authors_by_work_df


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


# %%
# Book Data: Works #


# %%
# Main #

if __name__ == "__main__":
    max_rows_to_read = None
    # load_db_authors(authors_text_file_path, max_rows_to_read=max_rows_to_read)
    # load_db_works(works_text_file_path, max_rows_to_read=max_rows_to_read)


# %%
# Main #

if __name__ == "__main__":
    author_id = "/authors/OL6822361A"
    works_by_author_df = get_books_for_author_id(sqlite_conn, author_id)
    print("Works by Author")
    pprint_df(works_by_author_df.head())

# %%
# Main #

if __name__ == "__main__":
    work_id = "/works/OL1079322W"
    get_authors_for_book_id(sqlite_conn, work_id)

# %%
# Main #

if __name__ == "__main__":
    work_title_parts = "fourth wing"
    df = find_authors_by_work_title(sqlite_conn, work_title_parts)
    print(f"Authors of Works Matching Search Query: {work_title_parts}")
    pprint_df(df)

# %%
# Main #

if __name__ == "__main__":
    author_name_parts = "Rebecca Yarros"
    df = find_works_by_author_name(sqlite_conn, author_name_parts)
    print(f"Works by Authors Matching Search Query: {author_name_parts}")
    pprint_df(df)


# %%
