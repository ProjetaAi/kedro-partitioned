"""Tests for concatenated dataset."""
from copy import deepcopy
from functools import partial
from typing import List
import pandas as pd
import pytest
from pytest_mock import MockFixture
from kedro_partitioned.extras.datasets.concatenated_dataset import (
    PandasConcatenatedDataSet
)

from .mocked_dataset import MockedDataSet

PARTITIONS = ['a/a', 'a/b', 'a/c']


@pytest.fixture(autouse=True)
def mock_load(mocker: MockFixture):
    """Overwrites PandasConcatenatedDataSet load.

    Args:
        mocker (MockFixture): pytest-mock fixture
    """
    def _list(self: PandasConcatenatedDataSet) -> List[str]:
        return PARTITIONS

    mocker.patch.object(PandasConcatenatedDataSet, '_list_partitions', _list)


@pytest.fixture()
def setup() -> partial:
    """Returns a partial function for setup.

    Returns:
        partial: partial function for setup
    """
    return partial(
        PandasConcatenatedDataSet, path='a/', dataset=MockedDataSet
    )


def test_load(setup: partial):
    """Test load method.

    Args:
        setup (partial): partial function for setup
    """
    dataset = setup()
    data: pd.DataFrame = dataset.load()
    assert len(data) == len(PARTITIONS) * len(MockedDataSet.EXAMPLE_DATA)
    assert set(data['fruits'].unique().tolist()
               ).issubset(set(MockedDataSet.EXAMPLE_DATA['fruits'].tolist()))


def test_load_preprocess(setup: partial):
    """Test load method with preprocess.

    Args:
        setup (partial): partial function for setup
    """
    dataset = setup(preprocess=lambda x: x.assign(test=10))
    data: pd.DataFrame = dataset.load()
    assert len(data) == len(PARTITIONS) * len(MockedDataSet.EXAMPLE_DATA)
    assert all(data['test'] == 10)


def test_preprocess_lambda(setup: partial):
    """Test load method with preprocess.

    Args:
        setup (partial): partial function for setup
    """
    dataset = setup(preprocess='lambda x: x.assign(test=10)')
    data: pd.DataFrame = dataset.load()
    assert len(data) == len(PARTITIONS) * len(MockedDataSet.EXAMPLE_DATA)
    assert all(data['test'] == 10)


def _add_test_col(x: pd.DataFrame) -> pd.DataFrame:
    """Adds a test column to the dataframe.

    Args:
        x (pd.DataFrame): dataframe to add test column to

    Returns:
        pd.DataFrame: dataframe with test column added
    """
    return x.assign(test=10)


def test_preprocess_import(setup: partial):
    """Test load method with preprocess importing function.

    Args:
        setup (partial): partial function for setup
    """
    dataset = setup(
        preprocess='.'.join([
            'tests', 'extras', 'datasets', 'test_concatened_dataset',
            '_add_test_col'
        ])
    )
    data: pd.DataFrame = dataset.load()
    assert len(data) == len(PARTITIONS) * len(MockedDataSet.EXAMPLE_DATA)
    assert all(data['test'] == 10)


def test_filter_regex(setup: partial):
    """Test load method with preprocess using regex.

    Args:
        setup (partial): partial function for setup
    """
    dataset = setup(filter='[ac]')
    data: pd.DataFrame = dataset.load()
    assert len(data) == (len(PARTITIONS) - 1) * len(MockedDataSet.EXAMPLE_DATA)


def test_filter_lambda(setup: partial):
    """Test load method with preprocess using lambda.

    Args:
        setup (partial): partial function for setup
    """
    dataset = setup(filter='lambda x: \'b\' not in x')
    data: pd.DataFrame = dataset.load()
    assert len(data) == (len(PARTITIONS) - 1) * len(MockedDataSet.EXAMPLE_DATA)


def _filter_b(x: str) -> bool:
    """Filter function for test.

    Args:
        x (str): string to filter

    Returns:
        bool: True if string does not contain 'b'
    """
    return 'b' not in x


def test_filter_import(setup: partial):
    """Test load method with preprocess importing function.

    Args:
        setup (partial): partial function for setup
    """
    dataset = setup(
        filter='.'.join([
            'tests', 'extras', 'datasets', 'test_concatened_dataset',
            '_filter_b'
        ])
    )
    data: pd.DataFrame = dataset.load()
    assert len(data) == (len(PARTITIONS) - 1) * len(MockedDataSet.EXAMPLE_DATA)


def test_load_preprocess_filter(setup: partial):
    """Test load method with preprocess and filter.

    Args:
        setup (partial): partial function for setup
    """
    dataset = setup(preprocess=lambda x: x.assign(test=10), filter='[ac]')
    data: pd.DataFrame = dataset.load()
    assert len(data) == (len(PARTITIONS) - 1) * len(MockedDataSet.EXAMPLE_DATA)
    assert all(data['test'] == 10)


def test_no_partitions(setup: partial):
    """Test load method with no partitions.

    Args:
        setup (partial): partial function for setup
    """
    old = deepcopy(PARTITIONS)
    PARTITIONS.clear()
    dataset = setup()
    try:
        dataset.load()
        assert False
    except Exception:
        pass
    PARTITIONS.extend(old)
