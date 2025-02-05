# %%
# Imports #

import glob
import gzip
import json
import os
import shutil

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2 import pool

from utils.display_tools import pprint_df, pprint_dict, pprint_ls  # noqa

# %%
# Variables #


project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
book_data_dir = os.path.join("F:\\", "book-data")
print(book_data_dir)

verbose = False
data_dumps_url = "https://openlibrary.org/developers/dumps"


# %%
# Credentials #

dotenv_path = os.path.join(project_root, ".env")
print(dotenv_path)
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

POSTGRES_URL = os.getenv("POSTGRES_URL")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")

# %%
# Connect To Postgres #


# Initialize the connection pool (adjust minconn and maxconn as needed)
POSTGRES_POOL = psycopg2.pool.SimpleConnectionPool(
    minconn=1,
    maxconn=5,  # Limit connections to avoid resource waste
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


def create_and_test_table():
    """Creates a books table in book_bot DB and inserts a test record."""

    pg_conn = get_connection()
    pg_cursor = pg_conn.cursor()

    # Create authors table
    pg_cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS authors (
            author_key TEXT PRIMARY KEY,
            revision INTEGER,
            last_modified TIMESTAMP,
            name TEXT,
            source_records TEXT,
            latest_revision INTEGER,
            created TIMESTAMP
        );
    """
    )

    pg_conn.commit()
    pg_cursor.close()
    pg_conn.close()

    print("Table ensured.")

    # test query
    pg_conn = psycopg2.connect(
        host=POSTGRES_URL,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
        port=POSTGRES_PORT,
    )
    pg_cursor = pg_conn.cursor()

    pg_cursor.execute(
        """
        SELECT * FROM authors LIMIT 1;
        """
    )
    record = pg_cursor.fetchone()
    print("Test record:")
    print(record)


create_and_test_table()


# %%
# Book Data #


def extract_gz_file(file_path):
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


def get_authors_text_file_path():
    ls_authors_files_in_book_data = glob.glob(
        os.path.join(book_data_dir, "ol_dump_authors*.txt")
    )

    # if text file exists use it
    if len(ls_authors_files_in_book_data) > 0:
        print("Found text file for authors, skipping extraction")
        authors_text_file_path = sorted(ls_authors_files_in_book_data)[0]
        print(f"Text file path: {authors_text_file_path}")
        return authors_text_file_path
    else:
        ls_gz_files = glob.glob(os.path.join(book_data_dir, "*.gz"))
        if len(ls_gz_files) > 0:
            gzip_path = sorted(ls_gz_files)[0]
            print(f"Gzip path is: {gzip_path}")
            authors_text_file_path = extract_gz_file(gzip_path)
            print(f"Extraction complete. Text file path: {authors_text_file_path}")
            return authors_text_file_path
        else:
            raise ValueError("Missing any file type")


# %%
# Extract or Load Works Data #


def get_works_text_file_path():
    ls_works_files_in_book_data = glob.glob(
        os.path.join(book_data_dir, "ol_dump_works*.txt")
    )

    # if text file exists use it
    if len(ls_works_files_in_book_data) > 0:
        print("Found text file for works, skipping extraction")
        works_text_file_path = sorted(ls_works_files_in_book_data)[0]
        print(f"works_text_file_path: {works_text_file_path}")
        return works_text_file_path
    else:
        ls_gz_files = glob.glob(os.path.join(book_data_dir, "*.gz"))
        if len(ls_gz_files) > 0:
            gzip_path = sorted(ls_gz_files)[0]
            print(f"Gzip path is: {gzip_path}")
            works_text_file_path = extract_gz_file(gzip_path)
            print(f"Extraction complete. Text file path: {works_text_file_path}")
            return works_text_file_path
        else:
            raise ValueError("Missing any file type")


# %%
# Book Data: Paths #

authors_text_file_path = get_authors_text_file_path()
works_text_file_path = get_works_text_file_path()

# %%
# Book Data: Authors #


def load_db_authors_postgres(authors_text_file_path, max_rows_to_read=None):
    row_counter = 0

    pg_conn = get_connection()
    pg_cursor = pg_conn.cursor()

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
            if row_counter % 10000 == 0:
                print(f"Authors row count: {row_counter}")
            if row_counter % 10000 == 0:
                print("Committing")
                pg_conn.commit()
            if max_rows_to_read and row_counter >= max_rows_to_read:
                break

        pg_conn.commit()
        pg_cursor.close()

        print("Authors row count updated: ", row_counter)


verbose = True
authors_text_file_path = get_authors_text_file_path()
max_rows_to_read = 10
load_db_authors_postgres(authors_text_file_path, max_rows_to_read=max_rows_to_read)


# %%


def query_postgres_authors():
    """Fetches up to 100 authors from PostgreSQL and returns a DataFrame."""
    conn = psycopg2.connect(
        host=POSTGRES_URL,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
        port=POSTGRES_PORT,
    )
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM authors LIMIT 100;")

    if not cursor.description:
        raise ValueError("No data found in the table.")

    # Get column names from the cursor description
    columns = [desc[0] for desc in cursor.description]

    # Fetch all records and convert to a DataFrame
    df = pd.DataFrame(cursor.fetchall(), columns=columns)

    cursor.close()
    conn.close()

    return df


df = query_postgres_authors()
pprint_df(df)


# %%
