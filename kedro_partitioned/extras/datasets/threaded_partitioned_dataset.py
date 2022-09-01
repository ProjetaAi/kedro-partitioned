"""A PartitionedDataSet that saves asynchronously."""

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from typing import Any, Dict, Tuple
from kedro_partitioned.io.path_safe_partitioned_dataset import (
    PathSafePartitionedDataSet
)


class ThreadedPartitionedDataSet(PathSafePartitionedDataSet):
    """Same implementation as the PartitionedDataSet, but using threads."""

    def _save_partition(self, partition: Tuple[str, Any]):
        partition_id, partition_data = partition
        kwargs = deepcopy(self._dataset_config)
        partition_path = self._partition_to_path(partition_id)
        # join the protocol back since tools like PySpark may rely on it
        kwargs[self._filepath_arg] = self._join_protocol(partition_path)
        dataset = self._dataset_type(**kwargs)  # type: ignore
        if callable(partition_data):
            partition_data = partition_data()
        dataset.save(partition_data)

    def _save(self, data: Dict[str, Any]):
        if self._overwrite and self._filesystem.exists(self._normalized_path):
            self._filesystem.rm(self._normalized_path, recursive=True)

        with ThreadPoolExecutor() as pool:
            pool.map(self._save_partition, data.items())

        self._invalidate_caches()
