# %%
# Imports #


import json

import requests

# %%
# Constants #


MODEL = "llama3"
AI_API_ENDPOINT = "http://192.168.86.197:11434/api/generate"


# %%
# Cache #

dict_cache = {}


# %%
# Functions #


def query_ai_for_book_metadata(current_book_path: str) -> str:
    key = f"{current_book_path}"
    if key in dict_cache:
        print(f"Cache hit for {key}")
        return dict_cache[key]

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
    
    # Cache the output
    dict_cache[key] = output
    print(f"Cache set for {key}")

    return output


def extract_json_from_ai_output(output: str) -> dict:
    for i, line in enumerate(output.splitlines(), 1):
        line = line.strip()
        print(f"Line {i:02} being checked: {repr(line)}")

        if "`" in line or "{" not in line or "}" not in line:
            continue

        # Remove wrapping quotes if present
        if (line.startswith("'") and line.endswith("'")) or (line.startswith('"') and line.endswith('"')):
            line = line[1:-1]

        # Unescape escaped quotes if needed
        line = line.replace('\\"', '"')

        try:
            parsed = json.loads(line)
            print(f"✅ Successfully parsed JSON on line {i:02}: {parsed}")
            return parsed
        except json.JSONDecodeError:
            print(f"❌ Failed to parse JSON on line {i:02}: {repr(line)}")

    raise json.JSONDecodeError("No valid JSON found", output, 0)



# %%
