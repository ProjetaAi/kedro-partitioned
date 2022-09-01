"""A DataSet that concatenates partitioned datasets."""
from concurrent.futures import ThreadPoolExecutor
import importlib
from typing import Any, Callable, Dict, Generic, Iterable, Type, TypeVar, Union

import pandas as pd
from kedro_partitioned.io.path_safe_partitioned_dataset import (
    PathSafePartitionedDataSet
)
from kedro_partitioned.utils.other import filter_or_regex, identity, truthify
from kedro_partitioned.utils.typing import PandasDataSets

T = TypeVar('T')


class ConcatenatedDataSet(PathSafePartitionedDataSet, Generic[T]):
    """A partitioned DataSet that concatenates partitioned datasets.

    Args:
        path (str): Path to the folder where the data is stored.
        dataset (Union[str, Type[T], Dict[str, Any]]):
            DataSet class to wrap.
        concat_func (Callable[[Iterable[T]], T]):
            Function to concatenate data.
        filepath_arg (str, optional): dataset's path attribute.
            Defaults to "filepath".
        filename_suffix (str, optional): partitioned suffix.
            Defaults to "".
        credentials (Dict[str, Any], optional): credentials.
            Defaults to None.
        load_args (Dict[str, Any], optional): args for loading.
            Defaults to None.
        fs_args (Dict[str, Any], optional): args for fsspec.
            Defaults to None.
        overwrite (bool, optional): overwrite partitions.
            Defaults to False.
        preprocess (Union[Callable[[T], T], str], optional):
            applied to each partition before concatenating.
            this argument can be a function, a lambda as string, a
            python import path to a function, or a regex.
            Defaults to identity.
        preprocess_kwargs (Dict, optional):
            arguments for preprocess function.
            this argument can be a function, a lambda as string or a
            python import path to a function.
            Defaults to {}.
        filter (Union[Callable[[str], bool], str], optional):
            filter partitions by its relative paths. Defaults to truthify.
    """

    _IMPORTLIB_SEPARATOR = '.'

    def __init__(
        self,
        path: str,
        dataset: Union[str, Type[T], Dict[str, Any]],
        concat_func: Callable[[Iterable[T]], T],
        filepath_arg: str = "filepath",
        filename_suffix: str = "",
        credentials: Dict[str, Any] = None,
        load_args: Dict[str, Any] = None,
        fs_args: Dict[str, Any] = None,
        overwrite: bool = False,
        preprocess: Union[Callable[[T], T], str] = identity,
        preprocess_kwargs: Dict = {},
        filter: Union[Callable[[str], bool], str] = truthify,
    ):
        """Initialize a ConcatenatedDataSet."""
        super().__init__(path, dataset, filepath_arg, filename_suffix,
                         credentials, load_args, fs_args, overwrite)
        self.concat_func = self._parse_function(concat_func)
        self.preprocess = self._parse_function(preprocess)
        self.filter = filter_or_regex(self._parse_function(filter))
        self.preprocess_kwargs = preprocess_kwargs

    @classmethod
    def _parse_function(cls, fn: Union[T, str]) -> Union[T, str, Callable]:
        """Parses a string as lambda, or imports it.

        Parses a string type to a lambda function string, or imports it, or
        returns itself if not string or if it is not a suitable string.

        Args:
            fn (Union[T, str])

        Returns:
            Union[T, str, Callable]

        Example:
            >>> fn = ConcatenatedDataSet._parse_function('lambda x: x+3')
            >>> fn(3)
            6
            >>> fn = ConcatenatedDataSet._parse_function(
            ...     'kedro_partitioned.utils.other.falsify')
            >>> fn(True)
            False
            >>> fn = ConcatenatedDataSet._parse_function('invalid')
            >>> fn
            'invalid'
        """
        try:
            if isinstance(fn, str):
                if fn.startswith('lambda '):
                    return eval(fn)
                elif cls._IMPORTLIB_SEPARATOR in fn:
                    module, func = fn.rsplit(cls._IMPORTLIB_SEPARATOR, 1)
                    return getattr(importlib.import_module(module), func)
        except Exception:
            pass
        return fn

    def _apply_preprocess(self, data: T) -> T:
        return self.preprocess(data, **self.preprocess_kwargs)

    def _load(self) -> T:
        partitions = super()._load()
        with ThreadPoolExecutor() as pool:
            loaders = [v for k, v in partitions.items() if self.filter(k)]
            data_list = list(pool.map(lambda x: self._apply_preprocess(x()),
                                      loaders))
        return self.concat_func(data_list)


class PandasConcatenatedDataSet(ConcatenatedDataSet[PandasDataSets]):
    """A partitioned dataset that concatenates load pandas DataFrames.

    Args:
        path (str): Path to the folder where the data is stored.
        dataset (Union[str, Type[T], Dict[str, Any]]):
            DataSet class to wrap.
        filepath_arg (str, optional): dataset's path attribute.
            Defaults to "filepath".
        filename_suffix (str, optional): partitioned suffix.
            Defaults to "".
        credentials (Dict[str, Any], optional): credentials.
            Defaults to None.
        load_args (Dict[str, Any], optional): args for loading.
            Defaults to None.
        fs_args (Dict[str, Any], optional): args for fsspec.
            Defaults to None.
        overwrite (bool, optional): overwrite partitions.
            Defaults to False.
        preprocess (Callable[[T], T], optional):
            applied to each partition before concatenating.
            this argument can be a function, a lambda as string or a
            python import path to a function.
            Defaults to identity.
        preprocess_kwargs (Dict, optional):
            arguments for preprocess function. Defaults to {}.
        filter (Union[Callable[[str], bool], str], optional):
            filter partitions by its relative paths.
            this argument can be a function, a lambda as string, a
            python import path to a function, or a regex.
            Defaults to truthify.

    Example:
        >>> ds = PandasConcatenatedDataSet(
        ...     path='a/b/c',
        ...     dataset='pandas.CSVDataSet',
        ...     filter='lambda subpath: "test" in subpath',
        ...     preprocess='lambda df, col: df.rename(columns={"a": col})',
        ...     preprocess_kwargs={'col': 'b'})  # doctest: +ELLIPSIS
        >>> ds
        <...PandasConcatenatedDataSet object at 0x...>
        >>> ds.filter('a/test/c.csv')
        True
        >>> ds.filter('a/b/c.csv')
        False
        >>> df = pd.DataFrame({'a': [1]})
        >>> ds._apply_preprocess(df)
           b
        0  1

        With imports:

        >>> ds = PandasConcatenatedDataSet(
        ...     path='a/b/c',
        ...     dataset='pandas.CSVDataSet',
        ...     filter='kedro_partitioned.utils.other.falsify',
        ...     # the same can be done with preprocess
        ...     preprocess='lambda df, col: df.rename(columns={"a": col})',
        ...     preprocess_kwargs={'col': 'b'})  # doctest: +ELLIPSIS
        >>> ds.filter('a/test/c.csv')
        False

        With regexes:

        >>> ds = PandasConcatenatedDataSet(
        ...     path='a/b/c',
        ...     dataset='pandas.CSVDataSet',
        ...     filter='.+test.csv$',
        ...     preprocess='lambda df, col: df.rename(columns={"a": col})',
        ...     preprocess_kwargs={'col': 'b'})  # doctest: +ELLIPSIS
        >>> ds.filter('a/test.csv/test.csv')
        True
        >>> ds.filter('a/test.csv/a.csv')
        False
    """

    def __init__(
        self,
        path: str,
        dataset: Union[str, Type[T], Dict[str, Any]],
        filepath_arg: str = "filepath",
        filename_suffix: str = "",
        credentials: Dict[str, Any] = None,
        load_args: Dict[str, Any] = None,
        fs_args: Dict[str, Any] = None,
        overwrite: bool = False,
        preprocess: Callable[[T], T] = identity,
        preprocess_kwargs: Dict = {},
        filter: Union[Callable[[str], bool], str] = truthify,
    ):
        """Initialize a PandasConcatenatedDataSet."""
        super().__init__(path, dataset, pd.concat, filepath_arg,
                         filename_suffix, credentials, load_args,
                         fs_args, overwrite, preprocess, preprocess_kwargs,
                         filter)
