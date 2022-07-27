"""Package for storing constants."""
import argparse
import os
from typing import Type
from kedro_partitioned.utils.string import UPath
from kedro_partitioned.utils.typing import (T)

def _get_from_argv(arg: str, type: Type[T], default: T) -> T:
    parser = argparse.ArgumentParser()
    normalized_flag = arg.replace('-', '_')
    parser.add_argument(
        f'--{normalized_flag}',
        metavar=f'--{arg}',
        type=type,
        default=default,
        required=False,
    )
    return getattr(parser.parse_known_args()[0], normalized_flag)


MAX_WORKERS = (
    os.environ.get('MAX_WORKERS') or _get_from_argv(
        arg='max-workers',
        type=int,
        default=(os.cpu_count() or 1),
    )
)
MAX_NODES = (os.environ.get('MAX_NODES') or 1)
CWD = UPath(os.getcwd())
