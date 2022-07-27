"""Autouse fixtures for tests."""
import logging
import pytest
from pprint import pprint


@pytest.fixture(autouse=True)
def disable_logging(doctest_namespace: dict):
    """Disable logging for all doctests."""
    logging.disable(logging.ERROR)


@pytest.fixture(autouse=True)
def add_libs(doctest_namespace: dict):
    """Add libraries to doctest namespace."""
    doctest_namespace['pprint'] = pprint
