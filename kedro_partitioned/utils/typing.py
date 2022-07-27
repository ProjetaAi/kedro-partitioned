"""Package for type annotations."""
from typing import Callable, Tuple, TypeVar, Dict, List, Union
from kedro.extras.datasets.pandas import (
    CSVDataSet,
    ExcelDataSet,
    ParquetDataSet
)

T = TypeVar('T')
Args = Tuple[T]
Kwargs = Dict[str, T]
OutputsSpec = Union[List[str], Dict[str, str]]
InputsSpec = Union[List[str], Dict[str, str]]
Outputs = Union[List[T], Dict[str, T]]
Inputs = Union[Args[T], Kwargs[T]]

PandasDataSets = Union[CSVDataSet, ExcelDataSet, ParquetDataSet]

IsFunction = Callable[[T], bool]
PreparationFunction = Callable[[T], T]
