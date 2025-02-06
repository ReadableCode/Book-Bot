# %%
# Imports #


import os

from local_database_postgres import query_postgres
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
# Functions #


df = query_postgres("SELECT * FROM authors LIMIT 10;")
print("Authors:")
pprint_df(df)

df = query_postgres("SELECT * FROM works LIMIT 10;")
print("Works:")
pprint_df(df)

df = query_postgres("SELECT * FROM work_authors LIMIT 10;")
print("Work Authors:")
pprint_df(df)


# %%
