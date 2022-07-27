"""Utils for iterable manipulation."""
from typing import Iterable, List, Tuple, Union
from kedro_partitioned.utils.typing import T, IsFunction


def firstorlist(lst: List[T]) -> Union[T, List[T]]:
    """Return the first element if the list is a single element list.

    Args:
        lst (List[T])

    Returns:
        Union[T, List[T]]

    Example:
        >>> firstorlist([1])
        1
        >>> firstorlist([1,2])
        [1, 2]
    """
    if len(lst) == 1:
        return lst[0]
    else:
        return lst


def tolist(val: T) -> List[T]:
    """Transform an object into list if not list.

    Args:
        val (T)

    Returns:
        List[T]

    Example:
        >>> tolist(3)
        [3]
        >>> tolist([3])
        [3]
    """
    if isinstance(val, list):
        return val
    else:
        return [val]


def optionaltolist(val: T) -> List[T]:
    """Return a list of the value if it is not None, otherwise, an empty list.

    Args:
        val (T): The value to convert to list

    Returns:
        List[T]: The value as list

    Example:
        >>> optionaltolist(3)
        [3]

        >>> optionaltolist(None)
        []
    """
    if val is None:
        return []
    else:
        return tolist(val)


def partition(filter_fn: IsFunction[T],
              iterable: Iterable[T]) -> Tuple[List[T], List[T]]:
    """Split an iterable in two lists, given a boolean function.

    This function splits a list in two lists, one containing the ones that
    returned True from 'filter_fn', and the other the ones that returned False.

    Args:
        filter_fn (IsFunction[T])
        iterable (Iterable[T])

    Returns:
        Tuple[List[T], List[T]]

    Example:
        >>> t, f = partition(lambda x: x > 3, [1,4,2])
        >>> f
        [1, 2]
        >>> t
        [4]
    """
    trues = []
    falses = []
    for item in iterable:
        if filter_fn(item):
            trues.append(item)
        else:
            falses.append(item)
    return trues, falses


def unique(arg: List[T]) -> List[T]:
    """Filter repeated elements in a list.

    Args:
        arg (List[T])

    Returns:
        List[T]

    Example:
        >>> sorted(unique(['a', 'b', 'a', 'c']))
        ['a', 'b', 'c']
    """
    return list(set(arg))
