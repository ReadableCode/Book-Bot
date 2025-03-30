# %%
# Imports #

import os
import json
from local_database_postgres import (
    get_authors_list,
    get_books_by_author,
    get_series_by_author,
)
from utils.display_tools import pprint_df, pprint_dict, pprint_ls  # noqa
from ai_helper import query_ai_for_book_metadata, extract_json_from_ai_output

# %%
# Constants #

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# %%
# Settings #


PATH_LIST_POSSIBLE_MEDIA_LOCS = [
    os.path.join("Y:\\", "Books"),
    os.path.join("U:\\", "Books"),
]


LS_INVALID_ATHORS_IN_DATABASE = [
    "",
    "Jack Reacher",
]

DESIRED_FOLDER_STRUCT_W_SERIES = "{author}/{series}/{series_number} - {book_title}"
DESIRED_FOLDER_STRUCT_WO_SERIES = "{author}/{book_title}"


# %%
# Calculated Variables #


for book_dir in PATH_LIST_POSSIBLE_MEDIA_LOCS:
    if os.path.isdir(os.path.join(book_dir, "Calibre-library")):
        local_books_dir = book_dir
        break
else:
    raise FileNotFoundError("Books directory not found.")

print(local_books_dir)

LS_INVALID_ATHORS_IN_DATABASE = [auth.lower() for auth in LS_INVALID_ATHORS_IN_DATABASE]


# %%
# Functions: Files #

def get_desired_path_for_book(dict_meta_data, extension):
    """
    Get the desired path for a book.
    """
    author = dict_meta_data.get("author", "")
    series = dict_meta_data.get("series", "")
    series_number = dict_meta_data.get("series_number", "")
    title = dict_meta_data.get("title", "")
    
    # series number to 2 digit padded string
    if series_number != "":
        series_number = str(int(series_number)).zfill(2)

    if series:
        return [
            author,
            series,
            f"{series_number} - {title}",
        ]
    else:
        return [
            author,
            title
        ]


# test_book_dict = {
#   "author": "Sarah J. Maas",
#   "series": "A Court of Thorns and Roses Series",
#   "series_number": "3",
#   "title": "A Court of Frost and Starlight"
# }
# extension = "epub"

# pprint_ls(get_desired_path_for_book(test_book_dict, extension))


# %%
# Functions: Metadata #


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
        if auth in LS_INVALID_ATHORS_IN_DATABASE:
            continue
        if auth in path.lower():
            author = auth.title()
            print(f"Found author: {author}")
            return author

    print(f"Author not found in path: {path}")
    return ""


def get_metadata_from_path(path, use_ai=True):
    """
    Get the parts of a path.
    """
    # get file path relative to book directory in linux format
    linux_rel_path = os.path.normpath(path).replace("\\", "/")

    file_extension = linux_rel_path.split(".")[-1]
    print(f"Checking file type: {file_extension}")
    if file_extension not in ["pdf", "epub", "mobi", "azw3", "opf"]:
        print(f"Invalid file type: {file_extension}")
        return {}
    
    if use_ai:
        # Use AI to get the metadata
        ai_book_details = query_ai_for_book_metadata(
            linux_rel_path,
        )
        dict_meta_data = extract_json_from_ai_output(ai_book_details)
        author = dict_meta_data.get("author", "")
        if author == "":
            print("Author not found.")
            return {}
        author = author.title()
        title = dict_meta_data.get("title", "")
        if title == "":
            print("Title not found.")
            return {}
        series = dict_meta_data.get("series", "")
        series = series.title()
        series_number = dict_meta_data.get("series_number", "")

        title = title.title()
        print(f"Found author: {author}")
        print(f"Found title: {title}")
        print(f"Found series: {series}")
        print(f"Found series number: {series_number}")

    else:
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
        series_number = ""

    dict_meta_data = {
        "path": linux_rel_path,
        "author": author,
        "series": series,
        "series_number": series_number,
        "title": title,
        "file_type": file_extension,
    }

    return dict_meta_data


# %%


# author_name = "Sarah J. Maas"
# ls_series_by_author = get_series_by_author(author_name)

# print(f"Series by {author_name}:")
# pprint_ls(ls_series_by_author)


# # %%

# author_name = "Sarah J. Maas"
# author_name = "orson scott card"
# books_by_author = get_books_by_author(author_name)

# print(f"Books by {author_name}:")
# pprint_ls(books_by_author)


# %%


# test_path = "Sarah J. Maas/A Court of Frost and Starlight (A Court of Thorns and Roses) (799)/A Court of Frost and Starlight (A Court of - Sarah J. Maas.epub"

# print(f"Getting metadata from path: {test_path}")
# ai_response: str = query_ai_for_book_metadata(test_path)
# print("AI response:")


# # %%


# print("==== AI RAW RESPONSE ====")
# for i, line in enumerate(ai_response.splitlines(), 1):
#     print(f"{i:02}: {line}")
# print("=========================")


# # %%



# print("Extracting JSON from AI output")
# pprint_dict(extract_json_from_ai_output(ai_response))

# %%
# Main #

ls_skip_dirs = [
    "Calibre-library",
    "Calibre-books",
]

if __name__ == "__main__":
    max_files_to_do = 5
    files_done = 0

    for root, dirs, files in os.walk(local_books_dir):
        # Skip this root entirely if it's a skip folder
        if os.path.basename(os.path.normpath(root)) in ls_skip_dirs:
            continue

        dirs[:] = [d for d in dirs if d not in ls_skip_dirs]

        if files:
            rel_path = os.path.relpath(os.path.join(root, files[0]), local_books_dir)
            print("-" * 100)
            print(f"Checking file: {rel_path}")
            dict_book_metadata = get_metadata_from_path(rel_path)
            valid_book = check_if_valid_book(dict_book_metadata)

            if valid_book:
                pprint_dict(dict_book_metadata)
                ls_path_desired = get_desired_path_for_book(
                    dict_book_metadata,
                    rel_path.split(".")[-1],
                )
                print("Would move book to:")
                pprint_ls(ls_path_desired)
                print()
                files_done += 1
                if files_done >= max_files_to_do:
                    break


# %%
