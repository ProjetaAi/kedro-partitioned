"""Non categorized utilitary functions."""
from typing import Any


def truthify(*args: Any, **kwargs: Any) -> bool:
    """Returns true.

    Args:
        x (Any)

    Returns:
        bool

    Example:
        >>> truthify(3)
        True
    """
    return True


def nonefy(*args: Any, **kwargs: Any) -> None:
    """Always returns None.

    Returns:
        None

    Example:
        >>> nonefy()

        >>> nonefy(3, what=5)
    """
    return None
