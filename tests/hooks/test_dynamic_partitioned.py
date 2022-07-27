"""Tests for dynamic partitioned."""
import pytest
from kedro_partitioned._hooks import DynamicPartitioned
from kedro.pipeline import Pipeline, node
from kedro.io import DataCatalog
from kedro.io import PartitionedDataSet
from kedro.extras.datasets.pandas import CSVDataSet

from ..extras.datasets.mocked_dataset import MockedDataSet


@pytest.fixture()
def hook() -> DynamicPartitioned:
    """Fixture for DynamicPartitioned hook.

    Returns:
        DynamicPartitioned: DynamicPartitioned hook.
    """
    return DynamicPartitioned()


def test_type_not_supported(hook: DynamicPartitioned):
    """Test if type is not supported.

    Args:
        hook (DynamicPartitioned): DynamicPartitioned hook.
    """
    pipe = Pipeline([node(func=min, inputs=['a/a'], outputs=['b/a'])])
    catalog = DataCatalog({
        'a':
        PartitionedDataSet(path='/a/', dataset=MockedDataSet),
        'b':
        PartitionedDataSet(path='/b/', dataset=MockedDataSet),
    })

    with pytest.raises(AssertionError):
        hook.before_pipeline_run({}, pipe, catalog)

    catalog = DataCatalog({
        'a':
        PartitionedDataSet(path='/a/',
                           dataset=MockedDataSet,
                           filename_suffix='.csv'),
        'b':
        PartitionedDataSet(path='/b/',
                           dataset=MockedDataSet,
                           filename_suffix='.csv'),
    })

    hook.before_pipeline_run({}, pipe, catalog)
    assert 'a/a' in catalog.list()


def test_multiple_datasets(hook: DynamicPartitioned):
    """Test if multiple datasets are supported.

    Args:
        hook (DynamicPartitioned): DynamicPartitioned hook.
    """
    pipe = Pipeline([node(func=min, inputs=['a/a'], outputs=['b/a'])])
    catalog = DataCatalog({
        'a':
        PartitionedDataSet(path='/a/', dataset=CSVDataSet),
        'b':
        PartitionedDataSet(path='/b/', dataset=CSVDataSet),
    })
    hook.before_pipeline_run({}, pipe, catalog)
    assert 'a/a' in catalog.list()
