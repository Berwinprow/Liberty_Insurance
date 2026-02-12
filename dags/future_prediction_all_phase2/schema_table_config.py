"""
Schema configuration utilities for Future Prediction Phase-2.
"""

import json
import logging

logger = logging.getLogger(__name__)


def get_schema(name: str, json_path: str) -> str:
    """
    Retrieve schema name from schema configuration file.

    Parameters
    ----------
    name : str
        Schema key name.
    json_path : str
        Path to schema configuration JSON.

    Returns
    -------
    str
        Schema name.
    """
    with open(json_path, "r") as file:
        return json.load(file)["schema"][name]
