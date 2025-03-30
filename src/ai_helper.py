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


def query_ai_for_book_metadata(current_book_path: str) -> str:
    prompt = f"""
    Given this book path:
    {current_book_path}

    Please provide the author, series, series number, and book title as a single pythonic dictionary to be read in with json.loads()

    Don't include any other information, just the dictionary.
    The dictionary should have the following keys:
        author,
        series,
        series_number,
        title

    The author should be the full name of the author, including middle names.
    The series should be the full name of the series, including the word "Series" and the number.
    The series number should be the number of the book in the series, including leading zeros.
    The book title should be the full title of the book, including the subtitle.
    The dictionary should be formatted as a single line of json.
    """  # noqa: E501

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

    print("==== AI RAW RESPONSE ====")
    for i, line in enumerate(output.splitlines(), 1):
        print(f"{i:02}: {line}")
    print("=========================")

    return output



# %%
