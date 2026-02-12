"""
Schema configuration utilities.
"""

import json
import logging


logger = logging.getLogger(__name__)


def get_schema(name: str, json_path: str) -> str:
    """
    Retrieve schema name from schema configuration file.

    Args:
        name (str): Schema key name
        json_path (str): Path to schema configuration JSON

    Returns:
        str: Schema name
    """
    with open(json_path, "r") as file:
        return json.load(file)["schema"][name]
