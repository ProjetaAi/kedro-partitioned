# Parallelism

## Step Parallelism

Step Parallelism is a feature that allows the user to replicate a step into multiple nodes, each one processing a batch  data. This is useful when the step is computationally expensive and the user wants to speed up the execution time, without having to worry about load balancing.

### Why Step Parallelism?

Because Kedro may be used in multiple machines, i.e. clusters, local machines, etc...it may be hard to find a solution that can scale both horizontally and vertically with the same code. In order to solve this problem, we developed a solution that passes the responsibility of load balancing to the current runner. For example, if you are using our step parallel solution with the `SequentialRunner`, Kedro will run it sequentially, but if you are using the `ParallelRunner`, Kedro will run it in parallel without using a single line of multiprocessing or multithreading code. The same applies to the Kedro to cloud converters, like `kedro-azureml`.

### How Step Parallelism works?

```{mermaid}
%%{init: {'theme':'dark'}}%%
graph LR
  subgraph Input
    PartitionedInput[(Partitioned Input)] --> Slicer
    Slicer --> JSON[(JSON)]
  end
  subgraph Compute
    PartitionedInput --> Slice0[Slice 0]
    PartitionedInput --> Slice1[Slice 1]
    PartitionedInput --> Slice...[Slice ...]
    PartitionedInput --> Slicen[Slice n]
    JSON --> Slice0
    JSON --> Slice1
    JSON --> Slice...
    JSON --> Slicen
    Slice0 --> PartitionedOutputCopy0[(Partitioned Output Copy 0)]
    Slice1 --> PartitionedOutputCopy1[(Partitioned Output Copy 1)]
    Slice... --> PartitionedOutputCopy...[(Partitioned Output Copy ...)]
    Slicen --> PartitionedOutputCopyn[(Partitioned Output Copy n)]
  end
  subgraph Barrier
    PartitionedOutputCopy0 --> Synchronization
    PartitionedOutputCopy1 --> Synchronization
    PartitionedOutputCopy... --> Synchronization
    PartitionedOutputCopyn --> Synchronization
    Synchronization --> PartitionedOutput[(Partitioned Output)]
  end
```

There three main layers in the step parallelism solution:

1. Input Layer: Responsible for slicing the input data into a JSON that specifies what partitions will be processed by each node.
2. Compute Layer: Responsible for processing each partition and saving it in a copy of the output (Since no nodes are going to process the same partition, the nodes won't incur into race condition).
3. Barrier Layer: Responsible for blocking the Kedro pipeline nodes that depend on the output of the step parallelism until all the compute nodes have finished processing the data (it doesn't perform any updates in the partitioned output).

### How to use it?

In order to use the step parallelism feature, you need to create your nodes using the `multinode` or the `multipipeline` functions. These functions return a pipeline with the structure stated above. The `multinode` function is used when you want to replicate a node into multiple nodes, while the `multipipeline` function is used when you want to replicate a pipeline into multiple pipelines, i.e. slices are replaced by a copy of the pipeline.

See the function docstrings below for more usage examples:

#### Multinode

```{eval-rst}
.. autofunction:: kedro_partitioned.pipeline.multinode
```

#### Multipipeline

```{eval-rst}
.. autofunction:: kedro_partitioned.pipeline.multipipeline
```

```{note}
Prefer using the `multipipeline` over the `multinode`, since it decreases the IO cost and it is more readable.
```
