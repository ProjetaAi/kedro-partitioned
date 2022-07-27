"""Package for io interaction."""
import configparser
import json
from typing import List, cast
import tomli
import yaml


def writelines(filepath: str, lines: List[str]):
    """Writes a file given a list of strings.

    Args:
        filepath (str)
        lines (List[str])
    """
    with open(filepath, 'w') as f:
        f.write('\n'.join(lines))


def writeyml(filepath: str, dictionary: dict):
    """Writes a yaml given a dictionary.

    Args:
        filepath (str)
        dictionary (dict)
    """
    with open(filepath, 'w') as f:
        yaml.dump(dictionary, f, default_flow_style=False)


def writejson(filepath: str, dictionary: dict):
    """Writes a json given a dictionary.

    Args:
        filepath (str)
        dictionary (dict)
    """
    with open(filepath, 'w') as f:
        json.dump(dictionary, f)


def writestr(filepath: str, string: str):
    """Writes a string in a file.

    Args:
        filepath (str)
        string (str)
    """
    with open(filepath, 'w') as f:
        f.write(string)


def readtoml(filepath: str) -> dict:
    """Reads a `.toml` file as a dict.

    Args:
        filepath (str)

    Returns:
        dict
    """
    with open(filepath, 'rb') as f:
        return tomli.load(f)


def readcfg(filepath: str) -> dict:
    """Reads a `.cfg/.ini` file as a dict.

    Args:
        filepath (str)

    Returns:
        dict
    """
    parser = configparser.ConfigParser()
    parser.read(filepath)
    return cast(dict, parser._sections).copy()


def readyml(filepath: str) -> dict:
    """Reads a `.yml` file as a dict.

    Args:
        filepath (str)

    Returns:
        dict
    """
    with open(filepath, 'r') as f:
        return yaml.load(f, Loader=yaml.FullLoader)


def readlines(filepath: str) -> List[str]:
    """Reads any file and returns a list of its lines.

    Args:
        filepath (str)

    Returns:
        List[str]
    """
    with open(filepath, 'r') as f:
        return f.readlines()


def readstr(filepath: str) -> str:
    """Reads a file and returns a string representation of it.

    Args:
        filepath (str)

    Returns:
        str
    """
    with open(filepath, 'r') as f:
        return f.read()
