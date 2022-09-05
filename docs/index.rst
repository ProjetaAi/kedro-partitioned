.. Kedro Multinode documentation master file, created by
   sphinx-quickstart on Wed Jul 27 13:25:35 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Kedro Partitioned
======================

.. image:: https://img.shields.io/github/workflow/status/ProjetaAi/kedro-partitioned/Build
   :alt: GitHub Workflow Status
.. image:: https://img.shields.io/codecov/c/gh/projetaai/kedro-partitioned
   :alt: Codecov
.. image:: https://img.shields.io/github/workflow/status/ProjetaAi/kedro-partitioned/Release?label=release
   :alt: GitHub Workflow Status
.. image:: https://img.shields.io/pypi/v/kedro-partitioned
   :alt: PyPI

Kedro Plugin focused on extending and helping the user to process partitioned data.

Installation
==================

Execute this command in your terminal:

.. code-block:: bash

   pip install kedro-partitioned

Contents
==================

Step Parallelism
******************

Step Parallelism is a feature that allows the user to replicate a step into
multiple nodes, each one processing a batch of data. This is useful when the
step is computationally expensive and the user wants to speed up the execution
time, without having to worry about load balancing. 

.. autosummary::
   kedro_partitioned.pipeline.multinode.multinode
   kedro_partitioned.pipeline.multinode.multipipeline

DataSets
******************

.. autosummary::
   kedro_partitioned.extras.datasets.concatenated_dataset.ConcatenatedDataSet
   kedro_partitioned.extras.datasets.concatenated_dataset.PandasConcatenatedDataSet
   kedro_partitioned.io.path_safe_partitioned_dataset.PathSafePartitionedDataSet

.. note::
   it is recommended to use PathSafePartitionedDataSet instead of PartitionedDataSet,
   for every step parallelism scenario. This is because, handling path safety is
   mandatory for the multinode feature to work properly.

Decorators
******************

.. autosummary::
   kedro_partitioned.pipeline.decorators.concat_partitions
   kedro_partitioned.pipeline.decorators.split_into_partitions
   kedro_partitioned.pipeline.decorators.list_output

Helpers
******************

Helpers are filter functions generated according to a specification.
They can be used in multiple string filter scenarios, such as glob filtering.
These functions are designed to be used with the decorators above, but can be
used in other scenarios if needed.

.. autosummary::
   kedro_partitioned.pipeline.decorators.helper_factory.date_range_filter
   kedro_partitioned.pipeline.decorators.helper_factory.regex_filter
   kedro_partitioned.pipeline.decorators.helper_factory.not_filter

Utils
******************
There are more relevant utilitary functions, but only the most important ones
are listed below:

.. autosummary::
   kedro_partitioned.utils.string.UPath

API Reference
==================

* :ref:`modindex`
* :ref:`genindex`

Credits
==================
.. _@gabrieldaiha: https://github.com/gabrieldaiha
.. _@nickolasrm: https://github.com/nickolasrm

This package was created by:

* Gabriel Daiha Alves `@gabrieldaiha`_
* Nickolas da Rocha Machado `@nickolasrm`_
