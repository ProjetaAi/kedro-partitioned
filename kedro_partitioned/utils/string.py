"""Utils for string manipulation."""
from pathlib import Path, PosixPath, PurePath, PurePosixPath, WindowsPath
from typing import Any
from upath import UPath as _UPath


class UPath(_UPath):
    """An universal Path that doesn't return WindowsPaths.

    if a Windows was returned by UPath, it converts to a PurePosixPath
    because Kedro always use PosixPath by default.

    Args:
        *args (str): Paths to be parsed and joined (if multiple).

    Example:
        >>> UPath('C:/Users/user/Desktop/file.txt')
        PurePosixPath('C:/Users/user/Desktop/file.txt')

        >>> UPath('http://azure.com/file.txt')
        HTTPPath('http://azure.com/file.txt')

        >>> UPath('a/b/c', 'd')
        PurePosixPath('a/b/c/d')

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
