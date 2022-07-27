"""Package for type annotations."""
from typing import Callable, Tuple, TypeVar

T = TypeVar('T')
Args = Tuple[T]
IsFunction = Callable[[T], bool]
