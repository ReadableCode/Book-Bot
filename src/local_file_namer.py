# %%
# Imports #

import json
import os
import re

from ai_helper import extract_json_from_ai_output, query_ai_for_book_metadata
from local_database_postgres import get_authors_list, get_books_by_author
from utils.display_tools import pprint_df, pprint_dict, pprint_ls  # noqa

# %%
# Constants #

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# %%
# Settings #

MOVE_FILES = True
COPY_FILES = False
STUB_OUTPUT = True

PATH_LIST_POSSIBLE_MEDIA_LOCS = [
    os.path.join("Y:\\", "Books to ai move"),
    os.path.join("U:\\", "Books to ai move"),
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

PATH_OUTPUT = os.path.join(local_books_dir, "book_bot_output")


# %%
# Functions: Files #


def recursive_rm_empty_dirs(path):
    """
    Recursively remove empty directories.
    """
    if os.path.isdir(path):
        # Get a list of all files and directories in the current directory
        items = os.listdir(path)

        # If the directory is empty, remove it
        if not items:
            os.rmdir(path)
            print(f"Removed empty directory: {path}")
            return True

        # If not empty, check each item
        for item in items:
            item_path = os.path.join(path, item)
            if os.path.isdir(item_path):
                # Recursively check subdirectories
                if recursive_rm_empty_dirs(item_path):
                    print(f"Removed empty directory: {item_path}")

        # After checking all items, if the directory is empty, remove it
        if not os.listdir(path):
            os.rmdir(path)
            print(f"Removed empty directory: {path}")
            return True

    return False


def sanitize_filename(path: str) -> str:
    # Replace invalid Windows filename characters with underscore
    return re.sub(r'[<>:"/\\|?*]', "_", path)


def get_desired_path_for_book(dict_meta_data, extension):
    """
    Get the desired path for a book.
    """
    author = dict_meta_data.get("author", "")
    series = dict_meta_data.get("series", "")
    series_number = dict_meta_data.get("series_number", "")
    title = dict_meta_data.get("title", "")

    # series number to 2 digit padded string
    if series_number and series_number != "":
        series_number = str(float(series_number)).zfill(2)

    # sanitize the title
    title = sanitize_filename(title)
    author = sanitize_filename(author)
    series = sanitize_filename(series)
    extension = sanitize_filename(extension)

    if series:
        series_num_title_string = (
            f"{series_number} - {title}.{extension}"
            if series_number
            else f"{title}.{extension}"
        )
        return [
            author,
            series,
            series_num_title_string,
        ]
    else:
        return [
            author,
            f"{title}.{extension}",
        ]


# test_book_dict = {
#   "author": "Sarah J. Maas",
#   "series": "A Court of Thorns and Roses Series",
#   "series_number": "3",
#   "title": "A Court of Frost and Starlight"
# }
# extension = "epub"

# print(get_desired_path_for_book(test_book_dict, extension))


# %%
# Functions: Metadata #


def check_if_valid_book(dict_meta_data):
    author = dict_meta_data.get("author", "")
    if author == "":
        return False
    title = dict_meta_data.get("title", "")
    if title == "":
        return False

    series_num = dict_meta_data.get("series_number", "")

    if series_num != "":
        try:
            series_num = float(series_num)
        except Exception as e:
            print(f"Series number is not a number: {series_num} caused: {e}")
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

    if use_ai:
        # Use AI to get the metadata
        ai_book_details = query_ai_for_book_metadata(
            linux_rel_path,
        )
        dict_meta_data = extract_json_from_ai_output(ai_book_details)
        author = dict_meta_data.get("author", "")
        title = dict_meta_data.get("title", "")
        series = dict_meta_data.get("series", "")
        series_number = dict_meta_data.get("series_number", "")

        # fix series number like "02 (of 5)"
        series_number = re.sub(r"\D.*", "", str(series_number))

        ls_invalid_series_data = [
            "",
            "none",
            "n/a",
            "m+f",
            " - ",
            "a novel",
            "a book",
            "a",
        ]
        ls_invalid_series_part_data = ["box set", "boxset", "complete works"]
        if str(series).lower() in ls_invalid_series_data or any(
            part in str(series).lower() for part in ls_invalid_series_part_data
        ):
            series = ""

        if str(series_number).lower() in ls_invalid_series_data or any(
            part in str(series_number).lower() for part in ls_invalid_series_part_data
        ):
            series_number = ""

        # correct casing
        author = author.title()
        title = title.title()
        series = series.title()
        # fix 'S being capitalized
        title = title.replace("'S", "'s")
        series = series.replace("'S", "'s")

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
# File Moves #


def get_file_paths_to_process():
    ls_skip_dirs = [
        "Calibre-library",
        "Calibre-books",
        "book_bot_output",
    ]

    ls_file_paths = []

    for root, dirs, files in os.walk(local_books_dir):
        # Skip this root entirely if it's a skip folder
        if os.path.basename(os.path.normpath(root)) in ls_skip_dirs:
            continue

        dirs[:] = [d for d in dirs if d not in ls_skip_dirs]

        if files:
            rel_path = os.path.relpath(os.path.join(root, files[0]), local_books_dir)
            ls_file_paths.append(rel_path)
    return ls_file_paths


def process_single_file_move_dict(dict_move):
    """
    Process a single file move dictionary.
    """
    print("Proccesing move command:")
    pprint_dict(dict_move)

    single_file_result_dict = {
        "move_status": "",
        "copy_status": "",
        "stub_json_status": "",
        "error": False,
    }

    old_path = dict_move.get("old_path", "")
    new_path = dict_move.get("new_path", "")

    if not old_path or not new_path:
        print("Invalid file move dictionary, skipping.")
        single_file_result_dict["error"] = True
        return single_file_result_dict

    if STUB_OUTPUT:
        # Create a stub json file at the destination path
        stub_json_path = new_path + ".json"
        print(f"Creating dest stub json file at {stub_json_path} and making dirs")
        # Create the new directory if it doesn't exist
        os.makedirs(os.path.dirname(new_path), exist_ok=True)
        # Create the stub json file
        with open(stub_json_path, "w") as f:
            json.dump(dict_move, f, indent=4)
        single_file_result_dict["stub_json_status"] = "created"
    else:
        print(f"Would create stub json file at {new_path} and make dirs")
        single_file_result_dict["stub_json_status"] = "not created, disabled"

    if not MOVE_FILES and not COPY_FILES:
        print(f"Would create dirs and move or copy file from {old_path} to {new_path}")
        return single_file_result_dict

    if MOVE_FILES and COPY_FILES:
        print("Both move and copy are enabled, not moving or copying.")
        single_file_result_dict = {
            "move_status": "not moved, both move and copy enabled",
            "copy_status": "not copied, both move and copy enabled",
            "error": True,
        }
        return single_file_result_dict

    if MOVE_FILES:
        if os.path.exists(new_path):
            print(f"Destination file already exists: {new_path}")
            print(f"Not moving file: {old_path}")
            single_file_result_dict["move_status"] = "moved to duplicate path"
            new_path = old_path.replace(
                "Books to ai move", "Books to ai move duplicates"
            )
            print(f"Moving file to: {new_path}")

            # Create the new directory if it doesn't exist
            os.makedirs(os.path.dirname(new_path), exist_ok=True)

            # Move the file
            os.rename(old_path, new_path)
            single_file_result_dict["move_status"] = "moved to duplicate path"

            print(f"Moved file from {old_path} to {new_path}")
            return single_file_result_dict
        else:
            print(f"Moving file to: {new_path}")

            # Create the new directory if it doesn't exist
            os.makedirs(os.path.dirname(new_path), exist_ok=True)

            # Move the file
            os.rename(old_path, new_path)
            single_file_result_dict["move_status"] = "moved"

            print(f"Moved file from {old_path} to {new_path}")
            return single_file_result_dict

    if COPY_FILES:
        if os.path.exists(new_path):
            print(f"Destination file already exists: {new_path}")
            print(f"Not copying file: {old_path}")
            single_file_result_dict["copy_status"] = "not copied, dest exists"
            return single_file_result_dict

        # Create the new directory if it doesn't exist
        os.makedirs(os.path.dirname(new_path), exist_ok=True)

        # Copy the file
        os.system(f"copy {old_path} {new_path}")
        single_file_result_dict["move_status"] = "copied"
        print(f"Copied file from {old_path} to {new_path}")
        return single_file_result_dict


def process_file_path(rel_path):
    try:
        dict_book_metadata = get_metadata_from_path(rel_path)
    except Exception as e:
        print(f"Error getting metadata from path: {rel_path}")
        print(e)
        return {
            "old_path": rel_path,
            "new_path": "",
            "book_metadata": {},
            "valid": False,
        }

    valid_book = check_if_valid_book(dict_book_metadata)

    print("Book Metadata:")
    pprint_dict(dict_book_metadata)

    if not valid_book:
        print("Book is invalid")
        dict_this_move = {
            "old_path": rel_path,
            "new_path": "",
            "book_metadata": dict_book_metadata,
            "valid": False,
            "processed": False,
        }
        return dict_this_move

    extension = rel_path.split(".")[-1]
    ls_path_desired = get_desired_path_for_book(
        dict_book_metadata,
        extension,
    )

    print("Would move book to:")
    print(ls_path_desired)

    dict_this_move = {
        "old_path": os.path.join(local_books_dir, rel_path),
        "new_path": os.path.join(
            PATH_OUTPUT,
            *ls_path_desired,
        ),
        "book_metadata": dict_book_metadata,
        "valid": True,
    }
    result_dict = process_single_file_move_dict(dict_this_move)
    dict_this_move["move result"] = result_dict
    return dict_this_move


# %%
# Main #


if __name__ == "__main__":
    max_files_to_do = 100
    files_done = 0
    ls_dict_failed_files = []

    ls_files_to_process = get_file_paths_to_process()
    print("==== FILES TO PROCESS (head) ====")
    pprint_ls(ls_files_to_process[:10])
    print(f"Total files to process: {len(ls_files_to_process)}")

    for path_num, rel_path in enumerate(ls_files_to_process):
        print("-" * 100)
        print(f"Checking file: {rel_path}\n({path_num + 1}/{len(ls_files_to_process)})")
        print("-" * 100)

        dict_this_move = process_file_path(rel_path)

        print("Action results:")
        pprint_dict(dict_this_move)

        if dict_this_move["valid"] and dict_this_move["move result"]["error"] is False:
            files_done += 1
        else:
            print("ERROR: Failed to process file")
            ls_dict_failed_files.append(dict_this_move)

        if files_done >= max_files_to_do:
            break

    print("==== FILE MOVES COMPLETE ====")
    print("==============================")
    print("==== FAILED FILE MOVES ====")
    pprint_dict(ls_dict_failed_files)
    print(f"Number of failed moves: {len(ls_dict_failed_files)}")
    print("==============================")


# %%
# Main: Extra Commands #


if __name__ == "__main__":
    print("==== Extra Commands ====")
    recursive_rm_empty_dirs(local_books_dir)
    print("==== END OF SCRIPT ====")


# %%
