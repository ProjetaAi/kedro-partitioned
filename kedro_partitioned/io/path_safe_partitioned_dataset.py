"""A DataSet that is partitioned into multiple DataSets."""
from pathlib import PurePosixPath
import posixpath
from kedro.io import PartitionedDataSet as _PartitionedDataSet


class PathSafePartitionedDataSet(_PartitionedDataSet):
    """Same as the regular Partitioned DataSet, but handles absolute paths.

    For example, if the ffspec package you are using returns the absolute path
    from a glob, this dataset will be able to handle it.

    Example:
        >>> ds = PathSafePartitionedDataSet(
        ...          path="http://abc.core/path/to",
        ...          dataset="pandas.CSVDataSet",)
        >>> ds._path_to_partition("http://abc.core/path/to/partition1.csv")
        'partition1.csv'
    """

    def _path_to_partition(self, path: str) -> str:
        """Takes only the relative subpath from the partitioned dataset path.

        Args:
            path (str): path to a partition

        Returns:
            str: relative subpath from the partitioned dataset path

        Example:
            >>> ds = PathSafePartitionedDataSet(
            ...          path="http://abc.core/path/to",
            ...          dataset="pandas.CSVDataSet",)
            >>> ds._path_to_partition("http://abc.core/path/to/partition1.csv")
            'partition1.csv'

            >>> ds = PathSafePartitionedDataSet(
            ...          path="data/path",
            ...          dataset="pandas.CSVDataSet",)
            >>> ds._path_to_partition("data/path/partition1.csv")
            'partition1.csv'

        Note:
            this dataset differs from the original one because it treats non
            absolute paths too. An example of non absolute path is the adlfs
            outputs. it returns the path relative to the container, while to
            declare the dataset, you'll have to pass the full uri to the
            folder. This makes Kedro's partitioned dataset to not rsplit
            its output correctly.
        """
        subpath = super()._path_to_partition(path)

        subpath_parts = PurePosixPath(path).parts
        path_parts = PurePosixPath(self._normalized_path).parts

        common_index = next((i for i, part in enumerate(path_parts)
                             if part == subpath_parts[0]), 0)
        suffix = str(PurePosixPath(*path_parts[common_index:])) + posixpath.sep
        return subpath.replace(suffix, '', 1)
