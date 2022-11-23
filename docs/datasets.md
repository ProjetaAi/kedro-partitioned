# Datasets

## Concatenated DataSet

```{eval-rst}
.. autoclass::
   kedro_partitioned.extras.datasets.concatenated_dataset.ConcatenatedDataSet
```

## Pandas Concatenated DataSet

Pandas concatenated dataset is a sugar for the `PartitionedDataSet` that concatenates all dataframe partitions into a single dataframe.

For example, let's say you have a folder structure like this:

```md
clients/
├── brazil.csv
├── canada.csv
└── united_states.csv
```

And you wan't to load all the files as a single dataset. In this case, you could do something like this:

```yaml
clients:
  type: kedro_partitioned.dataset.PandasConcatenatedDataSet
  path: clients
  dataset:
    type: pandas.CSVDataSet
```

Then, the clients dataset will be all the concatenated dataframes from the `clients/*.csv` files.

```{eval-rst}
.. autoclass::
   kedro_partitioned.extras.datasets.concatenated_dataset.PandasConcatenatedDataSet
```

## Path Safe Partitioned DataSet

```{eval-rst}
.. autoclass::
   kedro_partitioned.io.PathSafePartitionedDataSet
```

```{note}
it is recommended to use PathSafePartitionedDataSet instead of PartitionedDataSet, for every step parallelism scenario. This is important because handling path safely is mandatory for the multinode partitioned dataset zip feature to work properly.
```
