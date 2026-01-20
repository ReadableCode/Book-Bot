# %%
# Imports #

import json
import os
from os.path import expanduser

from open_library_api import get_googlebooks_info_by_isbn
from utils.display_tools import pprint_df, pprint_dict, pprint_ls  # noqa

# %%
# Constants #

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

home_dir = expanduser("~")
book_data_synced_dir = os.path.join(home_dir, "SyncthingDB", "Book-Bot")

# list dir
os.listdir(book_data_synced_dir)


def get_states():
    ls_all_files = os.listdir(book_data_synced_dir)
    ls_states = []

    for file in ls_all_files:
        print(file)
        # read the jsonl file in and append all the items
        if file.endswith(".json"):
            # read jsonl data
            with open(os.path.join(book_data_synced_dir, file), "r") as f:
                for line in f:
                    ls_states.append(json.loads(line))

    return ls_states


ls_states = get_states()

pprint_dict(ls_states)


# %%


def get_dataframe_of_states():
    import pandas as pd

    df_states = pd.DataFrame(ls_states)
    return df_states


df_states = get_dataframe_of_states()
pprint_df(df_states)


# append columns for data from google books
"""
{
        "isbn": isbn,
        "title": vi.get("title"),
        "subtitle": vi.get("subtitle"),
        "authors": vi.get("authors") or [],
        "published_date": vi.get("publishedDate"),
        "description": vi.get("description"),
        "categories": vi.get("categories") or [],
        "page_count": vi.get("pageCount"),
        "publisher": vi.get("publisher"),
        "language": vi.get("language"),
        "google_volume_id": data["items"][0].get("id"),
        "info_link": vi.get("infoLink"),
    }
"""
for index, row in df_states.iterrows():
    isbn = row.get("ISBN")
    if isbn:
        googlebooks_data = get_googlebooks_info_by_isbn(isbn)
        if googlebooks_data:
            for key, value in googlebooks_data.items():
                df_states.at[index, key] = value

pprint_df(df_states)


# save as csv in same folder
output_csv_path = os.path.join(book_data_synced_dir, "book_states_with_googlebooks.csv")
df_states.to_csv(output_csv_path, index=False)


# %%
