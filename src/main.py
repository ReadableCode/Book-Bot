# %%
# Imports #

import glob
import gzip
import json
import os
import shutil

import pandas as pd
from dotenv import load_dotenv
from psycopg2 import pool
from tqdm import tqdm

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
# Functions #


def query_postgres_authors():
    """Fetches up to 10 authors from PostgreSQL and returns a DataFrame."""
    pg_conn = None
    try:
        pg_conn = get_connection()
        pg_cursor = pg_conn.cursor()

        pg_cursor.execute("SELECT * FROM authors LIMIT 10;")

        if not pg_cursor.description:
            raise ValueError("No data found in the table.")

        # Get column names from the cursor description
        columns = [desc[0] for desc in pg_cursor.description]

        # Fetch all records and convert to a DataFrame
        df = pd.DataFrame(pg_cursor.fetchall(), columns=columns)

        pg_cursor.close()

        return df
    finally:
        if pg_conn:
            release_connection(pg_conn)


def query_postgres_works():
    """Fetches up to 10 works from PostgreSQL and returns a DataFrame."""
    pg_conn = None
    try:
        pg_conn = get_connection()
        pg_cursor = pg_conn.cursor()

        pg_cursor.execute("SELECT * FROM works LIMIT 10;")

        if not pg_cursor.description:
            raise ValueError("No data found in the table.")

        # Get column names from the cursor description
        columns = [desc[0] for desc in pg_cursor.description]

        # Fetch all records and convert to a DataFrame
        df = pd.DataFrame(pg_cursor.fetchall(), columns=columns)

        pg_cursor.close()

        return df
    finally:
        if pg_conn:
            release_connection(pg_conn)


def query_postgres_work_authors():
    """Fetches up to 10 work_authors from PostgreSQL and returns a DataFrame."""
    pg_conn = None
    try:
        pg_conn = get_connection()
        pg_cursor = pg_conn.cursor()

        pg_cursor.execute("SELECT * FROM work_authors LIMIT 10;")

        if not pg_cursor.description:
            raise ValueError("No data found in the table.")

        # Get column names from the cursor description
        columns = [desc[0] for desc in pg_cursor.description]

        # Fetch all records and convert to a DataFrame
        df = pd.DataFrame(pg_cursor.fetchall(), columns=columns)

        pg_cursor.close()

        return df
    finally:
        if pg_conn:
            release_connection(pg_conn)


df = query_postgres_authors()
print("Authors:")
pprint_df(df)

df = query_postgres_works()
print("Works:")
pprint_df(df)

df = query_postgres_work_authors()
print("Work Authors:")
pprint_df(df)


# %%
