# %%
# Imports #


import json
import os
import re

import requests

# %%
# Constants #


MODEL = "llama3"
AI_API_ENDPOINT = "http://192.168.86.197:11434/api/generate"


# %%
# Cache #
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_PATH = os.path.join(PROJECT_ROOT, "ai_response_cache.json")

# Load cache once
if os.path.exists(CACHE_PATH):
    with open(CACHE_PATH, "r", encoding="utf-8") as f:
        _CACHE = json.load(f)
else:
    _CACHE = {}


# %%
# Functions #


def query_ai_for_book_metadata(current_book_path: str) -> str:
    # check for key without extension
    key = os.path.splitext(current_book_path)[0]
    if key in _CACHE:
        print(f"Cache hit for {key}")
        return _CACHE[key]

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
    The series should be the full name of the series, including the word "Series" but no numbers.
    The series number should be the number of the book in the series.
    The book title should be the full title of the book, including the subtitle.
    The dictionary should be formatted as a single line of json.

    The JSON should all be on the same line with not breaking characters in between at all, do not mess this up or it will break everything.

    Do not include any other text, just the JSON. Do not include any code tags or markdown, do not include anything other than numbers for the series number.
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
    print(output)
    print("=========================")

    # Cache the output
    _CACHE[key] = output

    # Save the cache to file
    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(_CACHE, f, indent=4)

    return output


def extract_json_from_ai_output(output: str) -> dict:
    print("Checking AI output for JSON:")
    for i, line in enumerate(output.splitlines(), 1):
        line = line.strip()

        # remove </code> if present
        if "</code>" in line:
            line = line.replace("</code>", "")

        if "{" not in line or "}" not in line:
            continue

        # Strip outer quotes if present
        if (line.startswith("'") and line.endswith("'")) or (
            line.startswith('"') and line.endswith('"')
        ):
            line = line[1:-1]

        # Clean excessive escaping
        line = line.encode().decode("unicode_escape")  # handles \\ and \'

        # Strip trailing junk
        line = line.rstrip("`.;,")

        # Fix leading-zero ints
        line = re.sub(r'("series_number"\s*:\s*)0+(\d+)', r'\1"\2"', line)

        try:
            parsed = json.loads(line)
            print(f"✅ Successfully parsed JSON on line {i:02}: {parsed}")
            return parsed
        except json.JSONDecodeError:
            print(f"❌ Failed to parse JSON on line {i:02}: {repr(line)}")

    raise json.JSONDecodeError("No valid JSON found", output, 0)


# %%
