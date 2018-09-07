.. Sparsity documentation master file, created by
   sphinx-quickstart.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Sparsity - sparse data processing toolbox
=========================================

Sparsity builds on top of Pandas and Scipy to
provide DataFrame-like API to work with numerical homogeneous sparse data.

Sparsity provides Pandas-like indexing capabilities and group transformations
on Scipy csr matrices. This has proven to be extremely efficient as
shown below.

Furthermore we provide a distributed implementation of this data structure by
relying on the Dask_ framework. This includes distributed sorting,
partitioning, grouping and much more.

Although we try to mimic the Pandas DataFrame API, some
operations and parameters don't make sense on sparse or homogeneous data. Thus
some interfaces might be changed slightly in their semantics and/or inputs.

.. _Dask: https://dask.pydata.org/

Install
-------
Sparsity is available from PyPi (coming soon)::

   # Install using pip
   $ pip install sparsity

Contents
-----

.. toctree::
   :maxdepth: 2

   sources/about
   sources/user_guide
