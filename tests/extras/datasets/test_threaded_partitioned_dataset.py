"""Threaded partitioned dataset tests."""
import pathlib
from typing import List
import pytest
from pytest_mock import MockerFixture
from .mocked_dataset import MockedDataSet
from kedro_partitioned.extras.datasets.threaded_partitioned_dataset import (
    ThreadedPartitionedDataSet)


BASE_PATH = pathlib.Path.cwd() / 'a'


@pytest.fixture(autouse=True)
def mock_load(mocker: MockerFixture):
    """Overwrites ThreadedPartitionedDataSet load.

    Args:
        mocker (MockerFixture): pytest-mock fixture
    """
    def _list_partitions(self: ThreadedPartitionedDataSet) -> List[str]:
        return [(BASE_PATH / 'a' / sp).as_posix()
                for sp in ['a', 'b', 'c']]

    mocker.patch.object(ThreadedPartitionedDataSet,
                        '_list_partitions',
                        _list_partitions)


@pytest.fixture()
def setup() -> ThreadedPartitionedDataSet:
    """Returns ThreadedPartitionedDataSet instance.

    Returns:
        ThreadedPartitionedDataSet: threaded partitioned dataset
    """
    return ThreadedPartitionedDataSet(path=(BASE_PATH / 'a').as_posix(),
                                      dataset=MockedDataSet)


def test_save(setup: ThreadedPartitionedDataSet):
    """Test save method.

    Args:
        setup (ThreadedPartitionedDataSet): threaded partitioned dataset
    """
    to_save = {path: loader().assign(cnt=i)
               for i, (path, loader) in enumerate(setup.load().items())}
    setup.save(to_save)
    assert all(['cnt' in loader() for loader in setup.load().values()])


def test_save_callable(setup: ThreadedPartitionedDataSet):
    """Test save method with callable.

    Args:
        setup (ThreadedPartitionedDataSet): threaded partitioned dataset
    """
    to_save = {path: lambda: loader().assign(cnt=i)
               for i, (path, loader) in enumerate(setup.load().items())}
    setup.save(to_save)
    assert all(['cnt' in loader() for loader in setup.load().values()])
