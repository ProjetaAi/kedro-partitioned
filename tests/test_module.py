"""Tests if module imports are correct."""
import pytest
import importlib

_MODULES = [
    'config', 'deploy', 'extras', 'framework', 'hooks', 'io', 'pipeline',
    'runner', 'utils'
]


@pytest.fixture(autouse=True)
def test_imports():
    """Test if modules are imported correctly."""
    for module in _MODULES:
        importlib.import_module(f'kedro_partitioned.{module}')
