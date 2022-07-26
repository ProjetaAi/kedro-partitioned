"""Package for pipeline utilitary tools."""
import re


def normalize_node_name(name: str, replacement: str = '-') -> str:
    """Replaces all non valid characters in a string by `replacement`.

    Args:
        name (str): node name.
        replacement (str): Defaults to '-'.

    Returns:
        str

    Example:
        >>> normalize_node_name('apple_candy/Store&Sell')
        'apple_candy-Store-Sell'
    """
    return re.sub(r'[^a-zA-Z0-9-_.]', replacement, name)
