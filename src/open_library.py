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
path_works_data_dir = os.path.join(os.path.expanduser("~"), "book-data")
print(path_works_data_dir)


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


ls_works_files_in_book_data = glob.glob(
    os.path.join(path_works_data_dir, "ol_dump_works*.txt")
)

# if text file exists use it
if len(ls_works_files_in_book_data) > 0:
    print("found text file already")
    works_text_file_path = sorted(ls_works_files_in_book_data)[0]
    print(f"works_text_file_path: {works_text_file_path}")
else:
    ls_gz_files = glob.glob(os.path.join(path_works_data_dir, "*.gz"))
    if len(ls_gz_files) > 0:
        gzip_path = sorted(ls_gz_files)[0]
        print(f"Gzip path is: {gzip_path}")
        works_text_file_path = extract_file(gzip_path)
        print(f"Extraction complete. Text file path: {works_text_file_path}")
    else:
        raise ValueError("Missing any file type")

ls_authors_files_in_book_data = glob.glob(
    os.path.join(path_works_data_dir, "ol_dump_authors")
)

# if text file exists use it
if len(ls_authors_files_in_book_data) > 0:
    print("found text file already")
    authors_text_file_path = sorted(ls_authors_files_in_book_data)[0]
    print(f"Text file path: {authors_text_file_path}")
else:
    ls_gz_files = glob.glob(os.path.join(path_works_data_dir, "*.gz"))
    if len(ls_gz_files) > 0:
        gzip_path = sorted(ls_gz_files)[0]
        print(f"Gzip path is: {gzip_path}")
        authors_text_file_path = extract_file(gzip_path)
        print(f"Extraction complete. Text file path: {authors_text_file_path}")
    else:
        raise ValueError("Missing any file type")


# %%
# Book Data: Authors #


# read the first few lines of the text file
with open(authors_text_file_path, "r") as f:
    for line in tqdm(f, desc="Importing records"):
        line = line.strip()
        if not line:
            continue
        # The file is tab-separated. According to your example, the columns are:
        # 0: Type (/type/author)
        # 1: Author key (e.g. /authors/OL10000278A)
        # 2: Revision number (e.g. 3)
        # 3: Timestamp
        # 4: JSON blob with the record details
        parts = line.split("\t")

        print("parts")
        pprint_dict(parts)

        if len(parts) < 5:
            # Skip lines that don't have enough columns.
            break

        line_type = parts[0]
        line_key = parts[1]
        line_revision = parts[2]
        last_modified = parts[3]
        json_blob = parts[4]

        print(f"line_type: {line_type}")
        print(f"line_key: {line_key}")
        print(f"line_revision: {line_revision}")
        print(f"last_modified: {last_modified}")
        print(f"json_blob: {json_blob}")

        try:
            record = json.loads(json_blob)
        except Exception as e:
            print(f"JSON parse error for line_key: {line_key}: {e}")
            continue

        print("record")
        pprint_dict(record)
        break


# %%
# Book Data: Works #


# read the first few lines of the text file
with open(works_text_file_path, "r") as f:
    for line in tqdm(f, desc="Importing records"):
        line = line.strip()
        if not line:
            continue
        # The file is tab-separated. According to your example, the columns are:
        # 0: Type (/type/work)
        # 1: Work key (e.g. /works/OL10000278W)
        # 2: Revision number (e.g. 3)
        # 3: Timestamp
        # 4: JSON blob with the record details
        parts = line.split("\t")

        print("parts")
        pprint_dict(parts)

        if len(parts) < 5:
            # Skip lines that don't have enough columns.
            break

        line_type = parts[0]
        line_key = parts[1]
        line_revision = parts[2]
        last_modified = parts[3]
        json_blob = parts[4]

        print(f"line_type: {line_type}")
        print(f"line_key: {line_key}")
        print(f"line_revision: {line_revision}")
        print(f"last_modified: {last_modified}")
        print(f"json_blob: {json_blob}")

        try:
            record = json.loads(json_blob)
        except Exception as e:
            print(f"JSON parse error for line_key: {line_key}: {e}")
            continue

        print("record")
        pprint_dict(record)
        break


# %%
# Generate sqlite database #

# sqlite_file_path = os.path.join(path_works_data_dir, "book_data.db")
# conn = sqlite3.connect(sqlite_file_path)
# cursor = conn.cursor()

# # Create a table for works. We store:
# # - work_key: the unique key (e.g. "/works/OL10000278W")
# # - title: the title from the JSON blob
# # - authors: a string containing comma-separated author keys
# # - full_json: the entire JSON record as a string for later reference if needed
# cursor.execute(
#     """
#     CREATE TABLE IF NOT EXISTS works (
#         work_key TEXT PRIMARY KEY,
#         title TEXT,
#         authors TEXT,
#         full_json TEXT
#     )
# """
# )
# conn.commit()


# # Batch processing parameters: process records in batches for efficiency.
# batch_size = 1000
# batch = []

# with open(text_file_path, "r", encoding="utf-8") as f:
#     for line in tqdm(f, desc="Importing records"):
#         line = line.strip()
#         if not line:
#             continue
#         # The file is tab-separated. According to your example, the columns are:
#         # 0: Type (/type/work)
#         # 1: Work key (e.g. /works/OL10000278W)
#         # 2: Revision number (e.g. 3)
#         # 3: Timestamp
#         # 4: JSON blob with the record details
#         parts = line.split("\t")
#         if len(parts) < 5:
#             # Skip lines that don't have enough columns.
#             continue

#         work_key = parts[1]
#         json_blob = parts[4]

#         try:
#             record = json.loads(json_blob)
#         except Exception as e:
#             print(f"JSON parse error for work {work_key}: {e}")
#             continue

#         # Extract the title.
#         title = record.get("title", "")

#         # Extract authors.
#         # In your example, the authors are stored as a list of dictionaries under the "authors" key.
#         # Each dictionary is expected to have an "author" sub-dictionary with a "key".
#         authors_list = record.get("authors", [])
#         author_keys = [
#             author_entry.get("author", {}).get("key", "")
#             for author_entry in authors_list
#         ]
#         # Join the author keys into a comma-separated string.
#         authors_str = ", ".join(author_keys)

#         # Keep the full JSON for reference.
#         full_json = json.dumps(record)

#         # Append the tuple to the batch.
#         batch.append((work_key, title, authors_str, full_json))

#         # When the batch size is reached, perform a bulk insert.
#         if len(batch) >= batch_size:
#             cursor.executemany(
#                 """
#                 INSERT OR IGNORE INTO works (work_key, title, authors, full_json)
#                 VALUES (?, ?, ?, ?)
#             """,
#                 batch,
#             )
#             conn.commit()
#             batch = []  # Reset the batch

# # Insert any remaining records that didn't fill a complete batch.
# if batch:
#     cursor.executemany(
#         """
#         INSERT OR IGNORE INTO works (work_key, title, authors, full_json)
#         VALUES (?, ?, ?, ?)
#     """,
#         batch,
#     )
#     conn.commit()

# conn.close()
# print("Data import complete.")


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
