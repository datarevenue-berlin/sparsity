# Sparsity
[![CircleCI](https://circleci.com/gh/datarevenue-berlin/sparsity.svg?style=svg)](https://circleci.com/gh/datarevenue-berlin/sparsity)
[![Codecov](https://img.shields.io/codecov/c/github/datarevenue-berlin/sparsity.svg)](https://codecov.io/gh/datarevenue-berlin/sparsity)

Sparse data processing toolbox. It builds on top of pandas and scipy to provide DataFrame
like API to work with sparse categorical data. 

It also provides a extremly fast C level 
interface to read from traildb databases. This make it a highly performant package to use
for dataprocessing jobs especially such as log processing and/or clickstream ot click through data. 

In combination with dask it provides support to execute complex operations on 
a concurrent/distributed level.

## Contents
```eval_rst
.. toctree::
    :maxdepth: 2
    
    sources/user_guide.md
    api/sparseframe-api.rst
    api/dask-sparseframe-api.rst
    api/modules
```

## Attention
Please enjoy with carefulness it is a new project so it might still contain some bugs.


# Motivation
Many tasks especially in data analytics and machine learning domain make use of sparse
data structures to support the input of high dimensional data. 

This project was started
to build an efficient homogen sparse data processing pipeline. As of today dask has no
support for something as an sparse dataframe. We process big amounts of highdimensional data
on a daily basis at [datarevenue](http://datarevenue.com) and our favourite language 
and ETL framework are python and dask. After chaining many function calls on scipy.sparse 
csr matrices that involved handling of indices and column names to produce a sparse data
pipeline I decided to start this project.

This package might be especially usefull to you if you have very big amounts of 
sparse data such as clickstream data, categorical timeseries, log data or similarly sparse data.

# But wait pandas has SparseDataFrames and SparseSeries
Pandas has it's own implementation of sparse datastructures. Unfortuantely this structures
performs quite badly with a groupby sum aggregation which we also often use. Furthermore
 doing a groupby on a pandasSparseDataFrame returns a dense DataFrame. This makes chaining
  many groupby operations over multiple files cumbersome and less efficient. Consider 
following example:

```
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
The slowest run took 4.20 times longer than the fastest. This could mean that an intermediate result is being cached.
1000 loops, best of 3: 1.25 ms per loop

In [7]: res = sdf.groupby_sum()
   ...: res.__sizeof__()
   ...: 
Out[7]: 6128
```

I'm not quite sure if there is some cached result but I don't think so. This only uses a 
smart csr matrix multiplication to do the operation.