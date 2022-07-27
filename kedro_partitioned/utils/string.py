"""Utils for string manipulation."""
from string import Template as _Template
from pathlib import Path, PosixPath, PurePath, PurePosixPath, WindowsPath
import re
from typing import Any, Dict, Union
from upath import UPath as _UPath


class UPath(_UPath):
    """An universal Path that doesn't return WindowsPaths.

    if a Windows was returned by UPath, it converts to a PurePosixPath
    because Kedro always use PosixPath by default.

    Example:
        >>> UPath('C:/Users/user/Desktop/file.txt')
        PurePosixPath('C:/Users/user/Desktop/file.txt')

        >>> UPath('abfs://azure.com/file.txt')
        AzurePath('abfs://azure.com/file.txt')

        >>> UPath('a/b/c')
        PurePosixPath('a/b/c')

        >>> UPath('s3://test_bucket/file.txt')
        S3Path('s3://test_bucket/file.txt')
    """

    def __new__(cls, path: str, *args: Any, **kwargs: Any) -> PurePath:
        """Create a new UPath object.

        Args:
            path (str): The path to be converted.

        Returns:
            PurePath: The converted path.
        """
        path = super().__new__(cls, path, *args, **kwargs)
        if isinstance(path, WindowsPath):
            if path.is_absolute():
                parts = path.parts
                drive, parts = parts[0], (parts[1:] if len(parts) > 1 else [])
                drive = drive.strip('\\')
                return PurePosixPath(drive, *parts)
            else:
                return PurePosixPath(path)
        elif isinstance(path, PosixPath):
            return PurePosixPath(path)
        else:
            return path


def get_filepath_extension(filepath: str) -> str:
    """Returns the file extension given a filepath.

    Args:
        filepath (str)

    Returns:
        str

    Example:
        >>> get_filepath_extension('a.txt')
        '.txt'

        >>> get_filepath_extension('.gitignore')
        ''

        >>> get_filepath_extension('a/.gitignore')
        ''

        >>> get_filepath_extension('path/to.file/file.extension')
        '.extension'
    """
    return Path(filepath).suffix


def get_filepath_without_extension(filepath: str) -> str:
    """Returns the same filepath without extension.

    Args:
        filepath (str)

    Returns:
        str

    Example:
        >>> get_filepath_without_extension('/home/a.txt')
        '/home/a'

        >>> get_filepath_without_extension('a.txt')
        'a'

        >>> get_filepath_without_extension('a/.gitignore')
        'a/.gitignore'
    """
    ext = get_filepath_extension(filepath)
    if ext:
        return filepath.rsplit(ext, 1)[0]
    else:
        return filepath


def to_snake_case(string: str) -> str:
    """Converts a string to snake case.

    Args:
        string (str)

    Returns:
        str

    Example:
        >>> to_snake_case('CamelCase')
        'camel_case'
    """
    return re.sub(r'(?<!^)(?=[A-Z])', '_', string).lower()


def deepformat(x: Union[str, list, dict],
               replacements: Dict[str, str]) -> Union[str, list, dict, tuple]:
    """Replaces strings and nested strings recursively.

    Args:
        x (Union[str, list, dict])
        replacements (Dict[str, str])

    Returns:
        Union[str, list, dict, tuple]

    Example:
        >>> deepformat(3, {'b': '3'})
        3
        >>> deepformat('a{b}c{b}', {'b': '3'})
        'a3c3'
        >>> deepformat(['a{b}c{b}'], {'b': '3'})
        ['a3c3']
        >>> deepformat(('a{b}c{b}',), {'b': '3'})
        ('a3c3',)
        >>> deepformat({'b': 'a{b}c{b}'}, {'b': '3'})
        {'b': 'a3c3'}
        >>> deepformat({'b': [{'b': ('a{b}c{b}', 3)}]}, {'b': '3'})
        {'b': [{'b': ('a3c3', 3)}]}
    """
    if isinstance(x, str):
        return x.format_map(replacements)
    elif isinstance(x, list):
        return [deepformat(i, replacements) for i in x]
    elif isinstance(x, tuple):
        return tuple((deepformat(i, replacements) for i in x))
    elif isinstance(x, dict):
        return {k: deepformat(v, replacements) for k, v in x.items()}
    else:
        return x


class Template(_Template):
    """Copies 'string.Template' but with '$$' as its delimiter."""

    delimiter = "$$"
