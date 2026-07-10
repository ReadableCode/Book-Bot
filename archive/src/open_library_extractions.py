# %%
# Imports #
import glob
import gzip
import os
import shutil

from local_database_postgres import (
    ensure_postgres_tables,
    load_db_authors_postgres,
    load_db_works_postgres,
)
from utils.display_tools import pprint_df, pprint_dict, pprint_ls  # noqa

# %%
# Variables #

data_dumps_url = "https://openlibrary.org/developers/dumps"

book_data_dir = os.path.join("F:\\", "book-data")

MAX_ROWS_TO_READ = None  # Set to None to read all rows

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
# Main #

if __name__ == "__main__":
    authors_text_file_path = get_authors_text_file_path()
    works_text_file_path = get_works_text_file_path()

    ensure_postgres_tables()

    load_db_authors_postgres(authors_text_file_path, max_rows_to_read=MAX_ROWS_TO_READ)

    load_db_works_postgres(works_text_file_path, max_rows_to_read=MAX_ROWS_TO_READ)


# %%
