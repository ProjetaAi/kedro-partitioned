"""Decorators for node funcs."""
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
import posixpath
import re
from typing import Any, Callable, Iterable, Union, List, Dict

import pandas as pd

from kedro_partitioned.pipeline.decorators.helper_factory import regex_filter
from kedro_partitioned.utils.typing import IsFunction
from kedro_partitioned.utils.other import kwargs_only, identity
from kedro_partitioned.utils.iterable import tolist


def concat_partitions(
    partitioned_arg: str,
    filter: Union[str, IsFunction[str], List[IsFunction[str]]] = None,
    func: Callable[[pd.DataFrame], pd.DataFrame] = identity,
    func_args: List[str] = [],
) -> Callable[[Callable], Callable]:
    """Decorator that concatenates DataFrames in a partitioned dataset.

    Args:
        partitioned_arg (str): func's partitioned dataset argument
        filter (Union[str, Callable[[str], bool]]]): filter function for
            partition keys. Defaults to None
            * str: a regex
            * Callable[[str], bool]
        func (Callable[[pd.DataFrame], pd.DataFrame]): function applied to each
            partitions. Defaults to identity

    Returns:
        Callable[[Callable], Callable]

    Example:
        >>> fake_partitioned = {'a': lambda: pd.DataFrame({'a': [1]}),
        ...                     'ab': lambda: pd.DataFrame({'a': [2]}),
        ...                     'c': lambda: pd.DataFrame({'a': [3]})}
        >>> @concat_partitions(partitioned_arg='df')
        ... def foo(df):
        ...     return df

        >>> foo(fake_partitioned)
           a
        0  1
        1  2
        2  3

        >>> @concat_partitions(partitioned_arg='df',
        ...                    func=lambda x: x.assign(d=x['a']+10))
        ... def foo(df):
        ...     return df
        >>> foo(fake_partitioned)
           a   d
        0  1  11
        1  2  12
        2  3  13

        >>> @concat_partitions(partitioned_arg='df', filter='ab?')
        ... def foo(df):
        ...     return df
        >>> foo(fake_partitioned)
           a
        0  1
        1  2

        >>> @concat_partitions(partitioned_arg='df', filter='ab?',
        ...                    func=lambda x: x.assign(d=x['a']+10))
        ... def foo(df):
        ...     return df
        >>> foo(fake_partitioned)
           a   d
        0  1  11
        1  2  12

        >>> @concat_partitions(partitioned_arg='df', filter=lambda x: 'a' in x)
        ... def foo(df):
        ...     return df
        >>> foo(fake_partitioned)
           a
        0  1
        1  2

        >>> @concat_partitions(partitioned_arg='df', filter=lambda x: 'a' in x,
        ...                    func=lambda x: x.assign(d=x['a']+10))
        ... def foo(df):
        ...     return df
        >>> foo(fake_partitioned)
           a   d
        0  1  11
        1  2  12

        >>> @concat_partitions(partitioned_arg='df', filter=lambda x: 'a' in x,
        ...                    func=lambda x, arg1: x.assign(d=x['a']+arg1),
        ...                    func_args=['arg1'])
        ... def foo(df, arg1):
        ...     return df
        >>> foo(fake_partitioned, 20)
           a   d
        0  1  21
        1  2  22

        >>> @concat_partitions(partitioned_arg='df', filter='ggg')
        ... def foo(df):
        ...     return df
        >>> foo(fake_partitioned)
        Empty DataFrame
        Columns: []
        Index: []

        >>> @concat_partitions(partitioned_arg='df', filter='ggg')
        ... def foo(df):
        ...     return df
        >>> foo({})
        Empty DataFrame
        Columns: []
        Index: []

        Using helpers:

        >>> from kedro_partitioned.pipeline.decorators.helper_factory import (
        ...     date_range_filter)

        >>> dfn = date_range_filter(min_date='2020-02-02', format='%Y-%m-%d')
        >>> date_part = {'p1/2020-01-01/s': lambda: pd.DataFrame({'a': [1]}),
        ...              'p1/2020-02-03/s': lambda: pd.DataFrame({'a': [2]}),
        ...              'p2/2020-05-03/s': lambda: pd.DataFrame({'a': [3]})}
        >>> @concat_partitions(partitioned_arg='df', filter=dfn)
        ... def foo(df):
        ...     return df
        >>> foo(date_part)
           a
        0  2
        1  3

        Using multiple helpers:

        >>> from kedro_partitioned.utils.other import compose
        >>> from kedro_partitioned.pipeline.decorators.helper_factory import (
        ...     regex_filter)

        >>> @concat_partitions(partitioned_arg='df', filter=[dfn, r'p1.*'])
        ... def foo(df):
        ...     return df
        >>> foo(date_part)
           a
        0  2
    """
    if filter is None:
        def filter_fn(_: str) -> bool:
            return True
    elif isinstance(filter, str):
        regex = re.compile(filter)

        def filter_fn(x: str) -> bool:
            return bool(regex.search(x))
    else:
        filter_fns = [
            regex_filter(x) if isinstance(x, str) else x
            for x in tolist(filter)
        ]

        def filter_fn(x: str) -> bool:
            return all([fn(x) for fn in filter_fns])

    def decorator(f: Callable) -> Callable:

        @wraps(f)
        @kwargs_only(f)
        def wrapper(**kwargs: Any) -> Any:
            loaders_dict: Dict[str, Callable[[], pd.DataFrame]] =\
                kwargs[partitioned_arg]
            if len(loaders_dict) > 0:
                loaders_dict = {
                    k: v
                    for k, v in loaders_dict.items() if filter_fn(k)
                }

                if len(loaders_dict) == 0:  # filter removed everything
                    partitions = [pd.DataFrame()]
                else:  # reads in parallel
                    loaders = list(loaders_dict.values())
                    with ThreadPoolExecutor() as pool:
                        partitions = list(
                            pool.map(
                                lambda x: func(
                                    x(), **{
                                        k: v
                                        for k, v in kwargs.items()
                                        if k in func_args
                                    }), loaders))

                kwargs[partitioned_arg] =\
                    pd.concat(partitions).reset_index(drop=True)
            else:  # no partitions into the folder
                kwargs[partitioned_arg] = pd.DataFrame()
            return f(**kwargs)

        return wrapper

    return decorator


def list_output(f: Callable) -> Callable:
    """Turns an function output into a list.

    Args:
        f (Callable)

    Returns:
        Callable

    Example:
        >>> @list_output
        ... def foo():
        ...     return 3

        >>> foo()
        [3]
    """
    @wraps(f)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return [f(*args, **kwargs)]
    return wrapper


def split_into_partitions(
    keys: Union[str, Iterable[str]],
    folder_template: str = None,
    filename_template: str = None,
    output: Union[str, int] = 0
) -> Callable:
    """Splits a DataFrame function output into a dict <group_by_keys>: <group>.

    Args:
        keys (Iterable[str]): Columns names to group
        folder_template (str): Template name for folder. You can pass units of
            keys inside braces ({}). Defaults to None.
        filename_template (str): Template name for filename. You can pass units
            of keys inside braces ({}) Defaults to None
        output (Union[str, int], optional): Key or index of the output of the
            DataFrame. Defaults to 0.

    Returns:
        Callable

    Example:
        >>> df = pd.DataFrame({'name': ['Apple', 'Pear'], 'price': [10, 15]})
        >>> @split_into_partitions(
        ...     keys=['name', 'price'],
        ...     output=0)
        ... def foo(df):
        ...     return [df]

        >>> pprint(foo(df))  # doctest: +NORMALIZE_WHITESPACE
        [{'Apple/10/Apple_10':     name  price
        0  Apple     10,
        'Pear/15/Pear_15':    name  price
        1  Pear     15}]

        >>> @split_into_partitions(
        ...     keys=['name', 'price'],
        ...     folder_template='part/{name}/{price}',
        ...     filename_template='{name}_{price}',
        ...     output='out')
        ... def foo(df):
        ...     return {'out': df}

        >>> pprint(foo(df))  # doctest: +NORMALIZE_WHITESPACE
        {'out': {'part/Apple/10/Apple_10':     name  price
        0  Apple     10,
             'part/Pear/15/Pear_15':    name  price
        1  Pear     15}}

    """
    if isinstance(keys, str):
        keys = [keys]

    if folder_template is None:
        folder_template = posixpath.join(*['{' + str(k) + '}' for k in keys])
    if filename_template is None:
        filename_template = '_'.join(['{' + str(k) + '}' for k in keys])

    def decorator(f: Callable) -> Callable:
        @wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            r = f(*args, **kwargs)

            is_df = False
            if isinstance(r, (tuple, list, dict)):
                df = r[output]
            else:
                df = r
                is_df = True

            template = posixpath.join(folder_template, filename_template)

            splitted = {
                template.format(**{
                    unit: key[ind]
                    for ind, unit in enumerate(keys)
                }): group
                for key, group in df.groupby(keys)
            }

            if is_df:
                r = splitted
            else:
                r[output] = splitted
            return r
        return wrapper
    return decorator
