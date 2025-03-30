# %%
# Imports #


import json

import requests

# %%
# Constants #


MODEL = "llama3"
AI_API_ENDPOINT = "http://192.168.86.197:11434/api/generate"


# %%
# Functions #


def get_book_folder_path_dict(
    current_book_path, desired_folder_struct_w_series, desired_folder_struct_wo_series
):

    prompt = f"""
    Given this book path:
    {current_book_path}

    And this desired directory tree:
    {desired_folder_struct_w_series}

    Or for books not in a series:
    {desired_folder_struct_wo_series}

    Please provide the author, series, series number, and book title as a single pythonic dictionary to be read in with json.loads()

    Don't include any other information, just the dictionary.
    The dictionary should have the following keys:  
    author, series, series_number, book_title
    The author should be the full name of the author, including middle names.
    The series should be the full name of the series, including the word "Series" and the number.
    The series number should be the number of the book in the series, including leading zeros.
    The book title should be the full title of the book, including the subtitle.
    The dictionary should be formatted as a single line of json.
    """  # noqa : E501

    response = requests.post(
        AI_API_ENDPOINT,
        json={
            "model": MODEL,
            "prompt": prompt,
        },
    )

    output = ""
    for line in response.iter_lines():
        if line:
            data = json.loads(line)
            output += data.get("response", "")
    print(output)

    # filter out lines that dont start with a {
    first_line_with_bracket = output.find("{")
    output = output[first_line_with_bracket:]
    # remove any trailing lines
    output = output.split("\n")[0]

    return json.loads(output)


# %%
# Main #

if __name__ == "__main__":
    DESIRED_FOLDER_STRUCT_W_SERIES = "{author}/{series}/{series_numer} - {book_title}"
    DESIRED_FOLDER_STRUCT_WO_SERIES = "{author}/{book_title}"
    current_book_path = "./Jack Reacher Series 01 - Killing Floor - Lee Child\Lee Child - Jack Reacher Series 01 - Killing Floor (2).epub"

    print(
        get_book_folder_path_dict(
            current_book_path,
            DESIRED_FOLDER_STRUCT_W_SERIES,
            DESIRED_FOLDER_STRUCT_WO_SERIES,
        )
    )


# %%
