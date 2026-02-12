# schema_config.py
import json

def get_schema(name: str, json_path: str) -> str:
    with open(json_path, "r") as f:
        data = json.load(f)
    return data["schema"][name]
