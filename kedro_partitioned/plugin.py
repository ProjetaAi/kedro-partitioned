"""Hook to enable MultiNode."""
from copy import deepcopy
from typing import Dict, Any
from kedro.pipeline import Pipeline
from kedro.io import DataCatalog
from kedro.framework.hooks import hook_impl
from kedro.extras.datasets.json import JSONDataSet
import kedro.io.partitioned_dataset

from kedro_partitioned.pipeline.multinode import _SlicerNode, _MultiNode
from kedro_partitioned.utils.string import UPath
from kedro.io.partitioned_dataset import PartitionedDataSet
from kedro_partitioned.io.partitioned_dataset import (
    PartitionedDataSet as FixedPartitionedDataSet
)


class PartitionedDataSetPartialSubpathFixer:
    """Overrides Kedro partitioned in order to fix the subpath issue."""

    def __init__(self):
        """Initialize the class."""
        kedro.io.partitioned_dataset.PartitionedDataSet =\
            FixedPartitionedDataSet


class MultiNodeEnabler:
    """Performs required changes in kedro in order to enable MultiNodes.

    >>> from kedro.io import DataCatalog, PartitionedDataSet
    >>> from kedro.pipeline import Pipeline, node
    >>> from kedro_partitioned.pipeline import multipipeline
    >>> pipe = multipipeline(Pipeline([
    ...     node(func=lambda x: x, name='node', inputs='a', outputs='b'),]),
    ...     'a', 'pipe', n_slices=2)
    >>> catalog = DataCatalog(data_sets={
    ...     'a': PartitionedDataSet('a', 'pandas.CSVDataSet'),
    ...     'b': PartitionedDataSet('b', 'pandas.CSVDataSet')})
    >>> hook = MultiNodeEnabler()
    >>> hook.before_pipeline_run({}, pipe, catalog)

    >>> pprint(catalog._data_sets)  # doctest: +ELLIPSIS
    {'a': <kedro.io.partitioned_dataset.PartitionedDataSet ...>,
     'b': <kedro.io.partitioned_dataset.PartitionedDataSet ...>,
     'b-slice-0': <kedro.io.partitioned_dataset.PartitionedDataSet ...>,
     'b-slice-1': <kedro.io.partitioned_dataset.PartitionedDataSet ...>,
     'b-slicer': <kedro.extras.datasets.json.json_dataset.JSONDataSet ...>}

    >>> catalog._data_sets['b-slicer']._filepath
    PurePosixPath('b/b-slicer.json')

    Azure Blob Storage:


    >>> credentials = {'account_name': 'test'}
    >>> catalog = DataCatalog(data_sets={
    ...     'a': PartitionedDataSet('abfs://a/a', 'pandas.CSVDataSet',
    ...         credentials=credentials),
    ...     'b': PartitionedDataSet('abfs://a/b', 'pandas.CSVDataSet',
    ...         credentials=credentials)})
    >>> hook.before_pipeline_run({}, pipe, catalog)

    >>> catalog._data_sets['b-slicer']._filepath
    PurePosixPath('a/b/b-slicer.json')

    >>> catalog._data_sets['b-slicer']._protocol
    'abfs'
    """

    @hook_impl
    def before_pipeline_run(
        self,
        run_params: Dict[str, Any],
        pipeline: Pipeline,
        catalog: DataCatalog,
    ):
        """Performs required changes in kedro in order to enable MultiNodes.

        Args:
            run_params (Dict[str, Any]): Dictionary of parameters to be fed.
            pipeline (Pipeline): Pipeline to be run.
            catalog (DataCatalog): Catalog of data sources.
        """
        for node in pipeline.nodes:
            if isinstance(node, _MultiNode):
                for original, slice in zip(
                    node.original_partitioned_outputs, node.partitioned_outputs
                ):
                    partitioned = catalog._get_dataset(original)
                    assert isinstance(partitioned, PartitionedDataSet),\
                        'multinode cannot have non partitioned outputs'
                    catalog.add(slice, deepcopy(partitioned))

                for input in node.original_partitioned_inputs:
                    partitioned = catalog._get_dataset(input)
                    assert isinstance(partitioned, PartitionedDataSet),\
                        f'multinode received "{input}" as a '\
                        f'`PartitionedDataSet`, although it is a '\
                        f'`{type(partitioned)}`'

            elif isinstance(node, _SlicerNode):
                partitioned = catalog._get_dataset(node.original_output)
                assert isinstance(partitioned, PartitionedDataSet),\
                    f'multinode received "{node.original_output}" as a '\
                    f'`PartitionedDataSet`, although it is a '\
                    f'`{type(partitioned)}`'
                catalog.add(
                    node.json_output,
                    JSONDataSet(
                        filepath=str(
                            UPath(partitioned._path)
                            / f'{node.json_output}.json'
                        ),
                        credentials=partitioned._credentials
                    )
                )


hooks = (MultiNodeEnabler(), PartitionedDataSetPartialSubpathFixer())
