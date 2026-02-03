import json

def get_schema(name: str, json_path: str) -> str:
    with open(json_path, "r") as f:
        return json.load(f)["schema"][name]
