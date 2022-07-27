"""Package for type annotations."""
from typing import Callable, Tuple, TypeVar, Union
from kedro.extras.datasets.pandas import (
    CSVDataSet,
    ExcelDataSet,
    ParquetDataSet
)

T = TypeVar('T')
Args = Tuple[T]
IsFunction = Callable[[T], bool]
PandasDataSets = Union[CSVDataSet, ExcelDataSet, ParquetDataSet]
