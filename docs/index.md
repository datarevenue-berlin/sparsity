# Sparsity - sparse data processing toolbox
[![CircleCI](https://circleci.com/gh/datarevenue-berlin/sparsity.svg?style=svg)](https://circleci.com/gh/datarevenue-berlin/sparsity)
[![Codecov](https://img.shields.io/codecov/c/github/datarevenue-berlin/sparsity.svg)](https://codecov.io/gh/datarevenue-berlin/sparsity)

Sparsity builds on top of Pandas and Scipy to provide DataFrame-like API 
to work with numerical homogeneous sparse data.

Sparsity provides Pandas-like indexing capabilities and group transformations
on Scipy csr matrices. This has proven to be extremely efficient as
shown below.

Furthermore we provide a distributed implementation of this data structure by
relying on the [Dask](https://dask.pydata.org) framework. This includes 
distributed sorting, partitioning, grouping and much more.

Although we try to mimic the Pandas DataFrame API, some operations 
and parameters don't make sense on sparse or homogeneous data. Thus
some interfaces might be changed slightly in their semantics and/or inputs.

## Install
Sparsity is available from PyPi:
```
# Install using pip
$ pip install sparsity
```

## Contents
```eval_rst
.. toctree::
    :maxdepth: 2
    
    sources/about
    sources/user_guide
    api/sparseframe-api
    api/dask-sparseframe-api
    api/reference
```

## Attention
Please enjoy with carefulness as it is a new project and might still contain 
some bugs.
