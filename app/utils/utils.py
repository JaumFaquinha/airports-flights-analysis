import json
import polars as pl


def load_json_file(file_path) -> dict:
    with open(file=file_path, encoding="utf-8") as f:
        return json.load(f)
