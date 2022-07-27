.. Kedro Multinode documentation master file, created by
   sphinx-quickstart on Wed Jul 27 13:25:35 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Introduction
===========================================
Kedro Plugin focused on extending and helping the user to process partitioned
data.

Installation
==================

Execute this command in your terminal:

.. code-block:: bash

   pip install kedro-partitioned

Contents
==================

Step Parallelism
******************

.. autosummary::
   kedro_partitioned.pipeline.multinode.multinode
   kedro_partitioned.pipeline.multinode.multipipeline

DataSets
******************

.. autosummary::
   kedro_partitioned.extras.datasets.concatenated_dataset.ConcatenatedDataSet
   kedro_partitioned.extras.datasets.concatenated_dataset.PandasConcatenatedDataSet

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
