"""Non categorized utilitary functions."""
from functools import reduce
import inspect
import re
from typing import Any, Callable, Dict, Union

from kedro_partitioned.utils.typing import T


class FlagType:
    """Base class for building Flag like objects e.g. None and NoneType.

    Example:
        >>> class TestType(FlagType):
        ...     pass
        >>> Test = TestType()
        >>> Test
        Test
        >>> None is Test
        False
        >>> Test is Test
        True
    """

    def __init__(self):
        """Initializes the object."""
        self.name = self.__class__.__name__
        if self.name.endswith('Type'):
            self.name = self.name.split('Type')[0]

    def __str__(self) -> str:
        """Returns the name of the object.

        Returns:
            str: Name of the object.
        """
        return self.name

    def __repr__(self) -> str:
        """Returns the name of the object.

        Returns:
            str: Name of the object.
        """
        return str(self)


def get_varargs_as_kwargs_parser(f: Callable) -> Callable[..., Dict[str, Any]]:
    """Turns *args and **kwargs into **kwargs only.

    Args:
        f (Callable)

    Returns:
        Callable
    """
    sign = inspect.signature(f)
    return lambda args, kwargs: sign.bind(*args, **kwargs).arguments


def kwargs_only(original: Callable) -> Callable:
    """Decorator that turns *args and **kwargs into **kwargs only.

    Args:
        original (Callable)

    Returns:
        Callable

    Example:
        >>> def foo(a, b, c=4, d=5, **kwargs):
        ...     pass
        >>> @kwargs_only(foo)
        ... def bar(**kwargs):
        ...     print(kwargs)
        >>> bar(1,2,3,4)
        {'a': 1, 'b': 2, 'c': 3, 'd': 4}
        >>> bar(b=2,a=1,d=4,c=3)
        {'a': 1, 'b': 2, 'c': 3, 'd': 4}
        >>> bar(b=2,a=1,d=4,c=3, other=10)
        {'a': 1, 'b': 2, 'c': 3, 'd': 4, 'other': 10}
    """
    parser = get_varargs_as_kwargs_parser(original)

    def decorator(f: Callable) -> Callable:

        def wrapper(*args: Any, **kwargs: Any) -> Any:
            parse = parser(args, kwargs)

            if "kwargs" in parse:
                kw = parse.pop("kwargs")
                parse.update(kw)

            return f(**parse)

        return wrapper

    return decorator


def identity(x: T) -> T:
    """Returns the input parameter.

    Args:
        x (A)

    Returns:
        A

    Example:
        >>> identity(3)
        3
    """
    return x


def filter_or_regex(
    func: Union[Callable[[str], bool], str]
) -> Callable[[str], bool]:
    """Returns the input function or creates a regex match function.

    Args:
        func (Union[Callable[[str], bool], str])

    Returns:
        Callable[[str], bool]

    Example:
        >>> func1 = filter_or_regex(lambda x: x == 'abc')
        >>> func1('abc')
        True
        >>> func1('def')
        False
        >>> func = filter_or_regex('abc?')
        >>> func('abc')
        True
        >>> func('ab')
        True
        >>> func('def')
        False
        >>> func1('ab')
        False
    """
    if isinstance(func, str):
        regex = re.compile(func)

        def filter_func(x: str) -> bool:
            return bool(re.match(regex, x))

        return filter_func
    else:
        return func


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


def falsify(*args: Any, **kwargs: Any) -> bool:
    """Returns false.

    Args:
        x (Any)

    Returns:
        bool

    Example:
        >>> falsify(3)
        False
    """
    return False


def nonefy(*args: Any, **kwargs: Any) -> None:
    """Always returns None.

    Returns:
        None

    Example:
        >>> nonefy()

        >>> nonefy(3, what=5)
    """
    return None


def compose(*fns: Callable) -> Callable:
    """Pipes multiple function into one.

    Returns:
        Callable

    Example:
        >>> chained = compose(lambda x: x+10, lambda x: x*2)
        >>> chained(0)
        20
    """

    def chainit(x: Any) -> Callable:
        return reduce(lambda r, fn: fn(r), fns, x)

    return chainit
