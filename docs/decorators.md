# Decorators

## Concat Partitions

This decorator is used to concatenate the partitions of a dataset into a single dataset. It is similar to the `ConcatenatedDataSet`, but can be used as a decorator in a node.

```{eval-rst}
.. autofunction::
   kedro_partitioned.pipeline.decorators.concat_partitions
```

## Split Into Partitions

This decorator is used to split a dataset into partitions. It does the opposite of the `ConcatPartitions` decorator.

```{eval-rst}
.. autofunction::
   kedro_partitioned.pipeline.decorators.split_into_partitions
```

## List output

This decorator converts the output of a node into a list. Useful to standardize the declaration with lists.

```{eval-rst}
.. autofunction::
   kedro_partitioned.pipeline.decorators.list_output
```
