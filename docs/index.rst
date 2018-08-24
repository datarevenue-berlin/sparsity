.. Sparsity documentation master file, created by
   sphinx-quickstart.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Sparsity
========

Sparse data processing toolbox. It builds on top of pandas and scipy to
provide DataFrame like API to work with sparse categorical data.

In combination with dask it provides support to execute complex operations on
a concurrent/distributed level.

Install
-------
Sparsity is available from PyPi (coming soon)::

   # Install using pip
   $ pip install sparsity

Motivation
----------
Many tasks especially in data analytics and machine learning domain make use of
sparse data structures to support the input of high dimensional data.

This project was started to build an efficient homogenuos sparse data
processing pipeline. As of today dask has no support for something as an sparse
dataframe. We process big amounts of highdimensional data on a daily basis at
Datarevenue_ and our favourite language and ETL
framework are python and dask. After chaining many function calls on
scipy.sparse csr matrices that involved handling of indices and column names to
produce a sparse data pipeline we decided to start this project.

This package might be especially useful to you if you have very big amounts of
sparse data such as clickstream data, categorical timeseries, log data or
similarly sparse data.

.. _Datarevenue: https://datarevenue.com

Comparison to Pandas SparseFrame
--------------------------------
Pandas has it's own implementation of sparse data structures. Unfortunately this
structures performs quite badly with a groupby sum aggregation which we use
frequently. Furthermore doing a groupby on a pandas SparseDataFrame returns a
dense DataFrame. This makes chaining many groupby operations over multiple
files cumbersome and less efficient. Consider following example::

   In [1]: import sparsity
      ...: import pandas as pd
      ...: import numpy as np
      ...:

   In [2]: data = np.random.random(size=(1000,10))
      ...: data[data < 0.95] = 0
      ...: uids = np.random.randint(0,100,1000)
      ...: combined_data = np.hstack([uids.reshape(-1,1),data])
      ...: columns = ['id'] + list(map(str, range(10)))
      ...:
      ...: sdf = pd.SparseDataFrame(combined_data, columns = columns, default_fill_value=0)
      ...:

   In [3]: %%timeit
      ...: sdf.groupby('id').sum()
      ...:
   1 loop, best of 3: 462 ms per loop

   In [4]: res = sdf.groupby('id').sum()
      ...: res.values.nbytes
      ...:
   Out[4]: 7920

   In [5]: data = np.random.random(size=(1000,10))
      ...: data[data < 0.95] = 0
      ...: uids = np.random.randint(0,100,1000)
      ...: sdf = sparsity.SparseFrame(data, columns=np.asarray(list(map(str, range(10)))), index=uids)
      ...:

   In [6]: %%timeit
      ...: sdf.groupby_sum()
      ...:
   The slowest run took 4.20 times longer than the fastest.
   1000 loops, best of 3: 1.25 ms per loop

   In [7]: res = sdf.groupby_sum()
      ...: res.__sizeof__()
      ...:
   Out[7]: 6128

Index
-----

.. toctree::
   :maxdepth: 2

   api/sparsity



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
