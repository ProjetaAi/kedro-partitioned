"""Function factories for creating filters."""
import re
from re import Pattern

import pandas as pd
from kedro_partitioned.utils.typing import IsFunction

DATE_FORMAT_ISO = '%Y-%m-%d'


def not_filter(fn: IsFunction[str]) -> IsFunction[str]:
    """Inverts a function return.

    Args:
        fn (IsFunction[str])

    Returns:
        IsFunction[str]

    Example:
        >>> def fn(x: str) -> bool: return x == 'a'
        >>> nfn = not_filter(fn)
        >>> fn('a')
        True
        >>> fn('b')
        False
        >>> nfn('a')
        False
        >>> nfn('b')
        True
    """

    def filter(path: str) -> bool:
        return not fn(path)

    return filter


def regex_filter(pattern: str) -> IsFunction[str]:
    """Converts a regex pattern into a boolean filter.

    Args:
        pattern (str)

    Returns:
        IsFunction[str]: A function that takes a string as input and checks
            if it matches a pattern, if matches return True, otherwise False.

    Example:
        >>> fn = regex_filter(r'ab?')
        >>> fn('ad')
        True
        >>> fn('ab')
        True
        >>> fn('db')
        False
    """
    regex = re.compile(pattern)

    def filter_fn(x: str) -> bool:
        return bool(regex.search(x))

    return filter_fn


def _date_format_to_regex(format: str) -> Pattern:
    r"""Converts a date format pattern into a regex.

    Args:
        format (str)

    Returns:
        Pattern

    Example:
        >>> _date_format_to_regex('%Y-%m-%d')
        re.compile('\\d{4}-((0[0-9])|(1[0-2]))-(([0-2][0-9])|(3[0-1]))')
        >>> _date_format_to_regex('%d/%m/%Y')
        re.compile('(([0-2][0-9])|(3[0-1]))\\/((0[0-9])|(1[0-2]))\\/\\d{4}')
    """
    return re.compile(
        format.replace('/', r'\/').replace('%Y', r'\d{4}').replace(
            '%m', r'((0[0-9])|(1[0-2]))').replace('%d',
                                                  r'(([0-2][0-9])|(3[0-1]))'))


def date_range_filter(min_date: str = pd.Timestamp.min,
                      max_date: str = pd.Timestamp.max,
                      format: str = DATE_FORMAT_ISO) -> IsFunction[str]:
    """Generates a date_range filter function.

    Args:
        min_date (str, optional): Defaults to pd.Timestamp.min.
        max_date (str, optional): Defaults to pd.Timestamp.max.
        format (str, optional): Defaults to DATE_FORMAT_ISO.

    Returns:
        IsFunction[str]: A function that takes a string as input, extracts
            a regex match of the specified format and returns True whether
            a string is in date range (both inclusive), otherwise False.

    Example:
        >>> upper = date_range_filter(max_date='2020-02-02', format='%Y-%m-%d')
        >>> upper('2020-02-03')
        False
        >>> upper('2020-02-02')
        True
        >>> upper('random/string/2020-01-01/suffix')
        True

        >>> lower = date_range_filter(min_date='20200202', format='%Y%m%d')
        >>> lower('20200201')
        False
        >>> lower('20200202')
        True
        >>> lower('random/string/20200301/suffix')
        True

        >>> bounded = date_range_filter(min_date='02-02/2020',
        ...     max_date='02-04/2020', format='%d-%m/%Y')
        >>> bounded('prefix/02-04/2020/suffix/')
        True
        >>> bounded('prefix-02-02/2020-suffix')
        True
        >>> bounded('prefix/24-03/2020-suffix')
        True
        >>> bounded('prefix-13-01/2020/suffix')
        False
        >>> bounded('prefix25-05/2020suffix')
        False

    Warning:
        the specified format format must not be ambiguous. for example, if
        a string format is '%Y%m%d', and another number of 8 digits appear,
        it will be recognized as date. if it is a path for example, this can
        be avoided by using '/%Y%m%d/' as the format pattern
    """
    pd_min = pd.to_datetime(min_date, format=format)
    pd_max = pd.to_datetime(max_date, format=format)
    regex = _date_format_to_regex(format)

    def filter(path: str) -> bool:
        match = re.search(regex, path)
        if match:
            str_date = match[0]
            pd_date = pd.to_datetime(str_date, format=format)
            return pd_date >= pd_min and pd_date <= pd_max
        return False

    return filter
