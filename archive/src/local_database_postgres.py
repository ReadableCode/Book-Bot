# %%
# Imports #

import json
import os

import pandas as pd
from dotenv import load_dotenv
from psycopg2 import pool
from tqdm import tqdm

from utils.display_tools import pprint_df, pprint_dict, pprint_ls  # noqa

# %%
# Variables #

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

verbose = False

COMMIT_EVERY_ROW_NUM = 100000

dict_vars: dict[str, list[str]] = {}


# %%
# Credentials #

dotenv_path = os.path.join(project_root, ".env")
print(dotenv_path)
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

POSTGRES_URL = os.getenv("POSTGRES_URL")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = "book_bot"

# %%
# Connect To Postgres #


# Initialize the connection pool (adjust minconn and maxconn as needed)
POSTGRES_POOL = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=20,  # Limit connections to avoid resource waste
    host=POSTGRES_URL,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    dbname=POSTGRES_DB,
    port=POSTGRES_PORT,
)


def get_connection():
    """Get a connection from the pool."""
    return POSTGRES_POOL.getconn()


def release_connection(conn):
    """Release a connection back to the pool."""
    POSTGRES_POOL.putconn(conn)


# %%
# Connect To Postgres #


def ensure_postgres_tables():
    pg_conn = get_connection()
    pg_cursor = pg_conn.cursor()

    # Create authors table
    pg_cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS authors (
            author_key TEXT PRIMARY KEY,
            revision INTEGER,
            last_modified TIMESTAMP WITHOUT TIME ZONE,
            name TEXT,
            source_records TEXT,
            latest_revision INTEGER,
            created TIMESTAMP WITHOUT TIME ZONE
        );
        """
    )

    # Create works table
    pg_cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS works (
            work_key TEXT PRIMARY KEY,
            revision INTEGER,
            last_modified TIMESTAMP WITHOUT TIME ZONE,
            title TEXT,
            created TIMESTAMP WITHOUT TIME ZONE,
            covers TEXT,
            latest_revision INTEGER,
            authors TEXT
        );
        """
    )

    # Create work_authors table (many-to-many relationship)
    pg_cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS work_authors (
            work_key TEXT,
            author_key TEXT,
            FOREIGN KEY (work_key) REFERENCES works(work_key) ON DELETE CASCADE,
            FOREIGN KEY (author_key) REFERENCES authors(author_key) ON DELETE CASCADE,
            PRIMARY KEY (work_key, author_key)
        );
        """
    )

    pg_conn.commit()
    pg_cursor.close()
    pg_conn.close()

    print("Tables ensured.")


# %%
# Queries #


def query_postgres(query):
    """Executes a given SQL query and returns a Pandas DataFrame."""
    pg_conn = None
    pg_cursor = None
    try:
        pg_conn = get_connection()
        pg_cursor = pg_conn.cursor()

        pg_cursor.execute(query)

        if not pg_cursor.description:
            raise ValueError("No data found or invalid query.")

        # Get column names
        columns = [desc[0] for desc in pg_cursor.description]

        # Convert result to DataFrame
        df = pd.DataFrame(pg_cursor.fetchall(), columns=columns)

        return df
    finally:
        if pg_cursor:
            pg_cursor.close()
        if pg_conn:
            release_connection(pg_conn)


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


def get_series_by_author(author_name):
    """
    Fetches all works by a given author where the title contains a slash (/), indicating a series.
    """
    query = f"""
    SELECT w.work_key, w.title
    FROM works w
    JOIN work_authors wa ON w.work_key = wa.work_key
    JOIN authors a ON wa.author_key = a.author_key
    WHERE a.name ILIKE '%{author_name}%' AND w.title LIKE '%/%'
    ORDER BY w.title;
    """
    df = query_postgres(query)
    return df["title"].tolist()


def get_books_by_author(author_name):
    """
    Fetches all unique books by a given author.
    """
    query = f"""
    SELECT DISTINCT w.title
    FROM works w
    JOIN work_authors wa ON w.work_key = wa.work_key
    JOIN authors a ON wa.author_key = a.author_key
    WHERE a.name ILIKE '%{author_name}%'
    ORDER BY w.title;
    """

    df = query_postgres(query)
    return df["title"].tolist()


# %%
# Book Data: Paths #


def count_lines(file_path):
    """Fast way to count total lines in a file."""
    with open(file_path, "r", encoding="utf-8") as f:
        num_lines = sum(1 for _ in f)

    print(f"Total lines in {file_path}: {num_lines}")
    return num_lines


# %%
# Book Data: Authors #


def load_db_authors_postgres(authors_text_file_path, max_rows_to_read=None):
    row_counter = 0
    pg_conn = None
    pg_cursor = None

    total_lines = count_lines(authors_text_file_path)
    if max_rows_to_read:
        total_lines = min(total_lines, max_rows_to_read)

    try:
        pg_conn = get_connection()
        pg_cursor = pg_conn.cursor()

        # read the first few lines of the text file
        with open(authors_text_file_path, "r") as f:
            for line in tqdm(f, total=total_lines, desc="Processing Authors"):
                line = line.strip()
                if not line:
                    continue

                parts = line.split("\t")

                if len(parts) < 5:
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
                latest_revision = record.get("latest_revision")
                created = record.get("created", {}).get("value")

                if verbose:
                    print(f"name: {name}")
                    print(f"source_records: {source_records}")
                    print(f"created: {created}")

                query = """
                INSERT INTO authors (
                    author_key, revision, last_modified, name, 
                    source_records, latest_revision, created
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (author_key)
                DO UPDATE SET
                    revision = EXCLUDED.revision,
                    last_modified = EXCLUDED.last_modified,
                    name = EXCLUDED.name,
                    source_records = EXCLUDED.source_records,
                    latest_revision = EXCLUDED.latest_revision,
                    created = EXCLUDED.created
                """

                pg_cursor.execute(
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

                if verbose:
                    print("query")
                    print(query)

                row_counter += 1
                if row_counter % COMMIT_EVERY_ROW_NUM == 0:
                    pg_conn.commit()
                if max_rows_to_read and row_counter >= max_rows_to_read:
                    break

        pg_conn.commit()

        print("Authors row count updated: ", row_counter)
    finally:
        if pg_cursor:
            pg_cursor.close()
        if pg_conn:
            release_connection(pg_conn)


# %%
# Book Data: Works #


def load_db_works_postgres(works_text_file_path, max_rows_to_read=None, verbose=False):
    row_counter = 0
    pg_conn = None
    pg_cursor = None

    total_lines = count_lines(works_text_file_path)
    if max_rows_to_read:
        total_lines = min(total_lines, max_rows_to_read)

    try:
        pg_conn = get_connection()
        pg_cursor = pg_conn.cursor()

        # Read the first few lines of the text file
        with open(works_text_file_path, "r") as f:
            for line in tqdm(f, total=total_lines, desc="Processing Works"):
                line = line.strip()
                if not line:
                    continue

                parts = line.split("\t")
                if len(parts) < 5:
                    break

                line_type = parts[0]
                line_key = parts[1]
                line_revision = parts[2]
                line_last_modified = parts[3]
                line_json_blob = parts[4]

                if verbose:
                    print("---------------- Line Details ----------------")
                    print(f"line_type: {line_type}")
                    print(f"line_key: {line_key}")
                    print(f"line_revision: {line_revision}")
                    print(f"line_last_modified: {line_last_modified}")
                    print(f"line_json_blob: {line_json_blob}")

                try:
                    record = json.loads(line_json_blob)
                except Exception as e:
                    print(f"JSON parse error for line_key: {line_key}: {e}")
                    continue

                title = record.get("title", "")
                created = record.get("created", {}).get("value", "")
                covers = json.dumps(record.get("covers", []))
                latest_revision = record.get("latest_revision", "")
                authors_list = record.get("authors", [])

                # Ensure authors exist before inserting into `work_authors`
                for author in authors_list:
                    author_key = (
                        author.get("author", {}).get("key")
                        if isinstance(author.get("author", {}), dict)
                        else author.get("author", {})
                    )
                    if author_key:
                        pg_cursor.execute(
                            """
                            INSERT INTO authors (author_key)
                            VALUES (%s)
                            ON CONFLICT (author_key) DO NOTHING;
                            """,
                            (author_key,),
                        )

                # Insert work into `works`
                pg_cursor.execute(
                    """
                    INSERT INTO works (
                        work_key, revision, last_modified, title, 
                        created, covers, latest_revision, authors
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(work_key)
                    DO UPDATE SET
                        revision = EXCLUDED.revision,
                        last_modified = EXCLUDED.last_modified,
                        title = EXCLUDED.title,
                        created = EXCLUDED.created,
                        covers = EXCLUDED.covers,
                        latest_revision = EXCLUDED.latest_revision,
                        authors = EXCLUDED.authors;
                    """,
                    (
                        line_key,
                        line_revision,
                        line_last_modified,
                        title,
                        created,
                        covers,
                        latest_revision,
                        json.dumps(authors_list),
                    ),
                )

                # Insert relationships into `work_authors`
                for author in authors_list:
                    author_key = (
                        author.get("author", {}).get("key")
                        if isinstance(author.get("author", {}), dict)
                        else author.get("author", {})
                    )
                    if author_key:
                        pg_cursor.execute(
                            """
                            INSERT INTO work_authors (work_key, author_key) 
                            VALUES (%s, %s)
                            ON CONFLICT (work_key, author_key) DO NOTHING;
                            """,
                            (line_key, author_key),
                        )

                row_counter += 1
                if row_counter % COMMIT_EVERY_ROW_NUM == 0:
                    pg_conn.commit()

                if max_rows_to_read and row_counter >= max_rows_to_read:
                    break

        pg_conn.commit()

        print("Works row count updated:", row_counter)
    finally:
        if pg_cursor:
            pg_cursor.close()
        if pg_conn:
            release_connection(pg_conn)


# %%
