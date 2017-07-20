import datetime as dt
import os

import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest
import sparsity as sp
import sparsity.dask as dsp
from dask.async import get_sync
from sparsity import sparse_one_hot
from sparsity.dask.reshape import one_hot_encode

from .conftest import tmpdir

dask.context.set_options(get=dask.async.get_sync)


@pytest.fixture
def dsf():
    return dsp.from_pandas(pd.DataFrame(np.random.rand(10,2)),
                           npartitions=3)

def test_from_pandas():
    dsf = dsp.from_pandas(pd.DataFrame(np.random.rand(10,2)),
                          npartitions=3)
    res = dsf.compute()

    assert isinstance(res, sp.SparseFrame)
    assert res.shape == (10,2)


def test_map_partitions():
    data = pd.DataFrame(np.random.rand(10, 2))
    dsf = dsp.from_pandas(data,
                          npartitions=3)
    dsf = dsf.map_partitions(lambda x: x, dsf._meta)

    res = dsf.compute()

    assert isinstance(res, sp.SparseFrame)
    assert res.shape == (10, 2)


@pytest.mark.parametrize('iindexer, correct_shape', [
    (slice('A', 'B'), (2, 2)),
    (slice('C', None), (8, 2)),
    (slice(None, 'C'), (3, 2)),
])
def test_loc(iindexer, correct_shape):
    df = pd.DataFrame(np.random.rand(10, 2),
                      index=list('ABCDEFGHIJ'))
    dsf = dsp.from_pandas(df, npartitions=2)
    fut = dsf.loc[iindexer]
    assert fut._meta.empty
    res = fut.compute()

    assert isinstance(res, sp.SparseFrame)
    assert res.shape == correct_shape

def test_dask_loc(clickstream):
    sf = dd.from_pandas(clickstream, npartitions=10) \
        .map_partitions(
        sparse_one_hot,
        categories={'page_id': list('ABCDE')},
        meta=list
    )

    res = sf.loc['2016-01-15':'2016-02-15']
    res = sp.SparseFrame.concat(res.compute(get=get_sync).tolist())
    assert res.index.date.max() == dt.date(2016, 2, 15)
    assert res.index.date.min() == dt.date(2016, 1, 15)


def test_dask_multi_index_loc(clickstream):
    sf = dd.from_pandas(clickstream, npartitions=10) \
        .map_partitions(
            sparse_one_hot,
            index_col=['index', 'id'],
            categories={'page_id': list('ABCDE')},
            meta=list
    )
    res = sf.loc['2016-01-15':'2016-02-15']
    res = sp.SparseFrame.vstack(res.compute(get=get_sync).tolist())
    assert res.index.get_level_values(0).date.min() == dt.date(2016, 1, 15)
    assert res.index.get_level_values(0).date.max() == dt.date(2016, 2, 15)

def test_repr():
    dsf = dsp.from_pandas(pd.DataFrame(np.random.rand(10, 2)),
                          npartitions=3)
    assert isinstance(dsf.__repr__(), str)

    dsf = dsp.from_pandas(pd.DataFrame(np.random.rand(10, 100)),
                          npartitions=3)
    assert isinstance(dsf.__repr__(), str)


def test_one_hot_legacy_args(clickstream):
    ddf = dd.from_pandas(clickstream, npartitions=10)
    dsf = one_hot_encode(ddf, 'page_id', list('ABCDE'), ['index', 'id'])
    assert dsf._meta.empty
    sf = dsf.compute()
    assert sf.shape == (100, 5)
    assert isinstance(sf.index, pd.MultiIndex)


def test_one_hot_legacy_kwargs(clickstream):
    ddf = dd.from_pandas(clickstream, npartitions=10)
    dsf = one_hot_encode(ddf, column='page_id',
                         categories=list('ABCDE'),
                         index_col=['index', 'id'])
    assert dsf._meta.empty
    sf = dsf.compute()
    assert sf.shape == (100, 5)
    assert isinstance(sf.index, pd.MultiIndex)


def test_one_hot_no_order(clickstream):
    ddf = dd.from_pandas(clickstream, npartitions=10)
    dsf = one_hot_encode(ddf,
                         categories={'page_id': list('ABCDE'),
                                     'other_categorical': list('FGHIJ')},
                         index_col=['index', 'id'])
    assert dsf._meta.empty
    assert sorted(dsf.columns) == list('ABCDEFGHIJ')
    sf = dsf.compute()
    assert sf.shape == (100, 10)
    assert isinstance(sf.index, pd.MultiIndex)
    assert sorted(sf.columns) == list('ABCDEFGHIJ')


def test_one_hot_order1(clickstream):
    ddf = dd.from_pandas(clickstream, npartitions=10)
    dsf = one_hot_encode(ddf,
                         categories={'page_id': list('ABCDE'),
                                     'other_categorical': list('FGHIJ')},
                         order=['page_id', 'other_categorical'],
                         index_col=['index', 'id'])
    assert dsf._meta.empty
    assert all(dsf.columns == list('ABCDEFGHIJ'))
    sf = dsf.compute()
    assert sf.shape == (100, 10)
    assert isinstance(sf.index, pd.MultiIndex)
    assert all(sf.columns == list('ABCDEFGHIJ'))


def test_one_hot_order2(clickstream):
    ddf = dd.from_pandas(clickstream, npartitions=10)
    dsf = one_hot_encode(ddf,
                         categories={'page_id': list('ABCDE'),
                                     'other_categorical': list('FGHIJ')},
                         order=['other_categorical', 'page_id'],
                         index_col=['index', 'id'])
    assert dsf._meta.empty
    assert all(dsf.columns == list('FGHIJABCDE'))
    sf = dsf.compute()
    assert sf.shape == (100, 10)
    assert isinstance(sf.index, pd.MultiIndex)
    assert all(sf.columns == list('FGHIJABCDE'))


def test_one_hot_disk_categories(clickstream):
    with tmpdir() as tmp:
        cat_path = os.path.join(tmp, 'cat.pickle')
        pd.Series(list('ABCDE')).to_pickle(cat_path)
        ddf = dd.from_pandas(clickstream, npartitions=10)
        dsf = one_hot_encode(ddf,
                             categories={'page_id': cat_path},
                             index_col=['index', 'id'])
        assert dsf._meta.empty
        sf = dsf.compute()
        assert sf.shape == (100, 5)
        assert isinstance(sf.index, pd.MultiIndex)


def test_read_npz():
    sf = sp.SparseFrame(np.identity(100))
    with tmpdir() as tmp:
        sf.iloc[:25].to_npz(os.path.join(tmp, '1'))
        sf.iloc[25:50].to_npz(os.path.join(tmp, '2'))
        sf.iloc[50:75].to_npz(os.path.join(tmp, '3'))
        sf.iloc[75:].to_npz(os.path.join(tmp, '4'))

        dsf = dsp.read_npz(os.path.join(tmp, '*.npz'))
        sf = dsf.compute()
    assert np.all(sf.data.toarray() == np.identity(100))


def test_assign_column():
    s = pd.Series(np.arange(10))
    ds = dd.from_pandas(s, npartitions=2)

    f = pd.DataFrame(np.random.rand(10, 2), columns=['a', 'b'])
    dsf = dsp.from_pandas(f, npartitions=2)

    dsf = dsf.assign(new=ds)
    assert dsf._meta.empty
    sf = dsf.compute()
    assert np.all(sf.todense() == f.assign(new=s))
