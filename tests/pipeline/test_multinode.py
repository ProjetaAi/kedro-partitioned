"""Contents of hello_kedro.py"""
from functools import partial
import pytest
from pytest_mock import MockerFixture
from typing import List

from kedro.io import DataCatalog, PartitionedDataSet
from kedro.runner import SequentialRunner
from kedro.framework.session import KedroSession
from kedro_partitioned.pipeline import multinode
from ..extras.datasets.mocked_dataset import MockedDataSet

PARTITIONS = ['a/a', 'a/b', 'a/c']


@pytest.fixture(autouse=True)
def mock_load(mocker: MockerFixture):
    """Overwrites PandasConcatenatedDataSet load.

    Args:
        mocker (MockerFixture): pytest-mock fixture
    """
    def _list(self: PartitionedDataSet) -> List[str]:
        return PARTITIONS

    mocker.patch.object(PartitionedDataSet, '_list_partitions', _list)


@pytest.fixture()
def setup() -> partial:
    """Returns a partial function for setup.

    Returns:
        partial: partial function for setup
    """
    return partial(
        PartitionedDataSet, path='a/', dataset=MockedDataSet
    )


# Prepare a data catalog
@pytest.fixture()
def catalog(setup):
    data_catalog = DataCatalog({"mocked_input": setup(), "mocked_output": setup()})
    return data_catalog

# Prepare a data catalog
@pytest.fixture()
def runner() -> SequentialRunner:
    seq_runner = SequentialRunner()
    return seq_runner

# Prepare a data catalog
@pytest.fixture()
def hook_manager():
    _hook_manager = KedroSession('kedro-hello-world')._hook_manager
    return _hook_manager



# Prepare first node
def return_df(df):
    df['test'] = 1
    return df


def test_multinode(catalog: DataCatalog, runner: SequentialRunner, hook_manager):
    return_greeting_node = multinode(func=return_df, name='multinode',
                                     partitioned_input="mocked_input",
                                     partitioned_output="mocked_output")
    runner.run(return_greeting_node, catalog, hook_manager)
    data = catalog._get_dataset(data_set_name='mocked_output')._load()
    data = all('test' in value() for value in data.values())

    assert data