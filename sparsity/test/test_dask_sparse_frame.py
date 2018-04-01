import datetime as dt
import os

import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest
import sparsity as sp
import sparsity.dask as dsp
from dask.local import get_sync
from sparsity import sparse_one_hot
from sparsity.dask.reshape import one_hot_encode
import pandas.util.testing as pdt

from .conftest import tmpdir

dask.context.set_options(get=dask.local.get_sync)


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


def test_one_hot_legacy(clickstream):
    ddf = dd.from_pandas(clickstream, npartitions=10)
    dsf = one_hot_encode(ddf, 'page_id', list('ABCDE'), ['index', 'id'])
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


def test_one_hot_no_order_categorical(clickstream):
    clickstream['other_categorical'] = clickstream['other_categorical'] \
        .astype('category')
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


def test_one_hot_prefixes(clickstream):
    ddf = dd.from_pandas(clickstream, npartitions=10)
    dsf = one_hot_encode(ddf,
                         categories={'page_id': list('ABCDE'),
                                     'other_categorical': list('FGHIJ')},
                         index_col=['index', 'id'],
                         prefixes=True)
    correct_columns = list(map(lambda x: 'page_id_' + x, list('ABCDE'))) \
        + list(map(lambda x: 'other_categorical_' + x, list('FGHIJ')))
    assert dsf._meta.empty
    assert sorted(dsf.columns) == sorted(correct_columns)
    sf = dsf.compute()
    assert sf.shape == (100, 10)
    assert isinstance(sf.index, pd.MultiIndex)
    assert sorted(sf.columns) == sorted(correct_columns)


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

@pytest.mark.parametrize('arg_dict', [
    dict(divisions=[0, 30, 50, 70, 99]),
    dict(npartitions=6),
    dict(npartitions=2),
])
def test_repartition_divisions(arg_dict):
    df = pd.DataFrame(np.identity(100))
    dsf = dsp.from_pandas(df, npartitions=4)

    dsf2 = dsf.repartition(**arg_dict)

    assert isinstance(dsf2, dsp.SparseFrame)
    if 'divisions' in arg_dict:
        assert tuple(dsf2.divisions) == tuple(arg_dict['divisions'])

    df2 = dsf2.compute().todense()
    pdt.assert_frame_equal(df, df2)


@pytest.mark.parametrize('start_part, end_part', [
    (2, 4),
    (3, 2),
    (3, 3),
])
def test_repartition_n_divisions(start_part, end_part):
    df = pd.DataFrame(np.identity(10))
    dsf = dsp.from_pandas(df, npartitions=start_part)

    dsf2 = dsf.repartition(npartitions=end_part)

    assert isinstance(dsf2, dsp.SparseFrame)
    assert dsf2.npartitions == end_part

    df2 = dsf2.compute().todense()
    pdt.assert_frame_equal(df, df2)


@pytest.mark.parametrize('how', ['left', 'right', 'inner', 'outer'])
def test_distributed_join(how):
    left = pd.DataFrame(np.identity(10),
                        index=np.arange(10),
                        columns=list('ABCDEFGHIJ'))
    right = pd.DataFrame(np.identity(10),
                         index=np.arange(5, 15),
                         columns=list('KLMNOPQRST'))
    correct = left.join(right, how=how).fillna(0)

    d_left = dsp.from_pandas(left, npartitions=2)
    d_right = dsp.from_pandas(right, npartitions=2)

    joined = d_left.join(d_right, how=how)

    res = joined.compute().todense()

    pdt.assert_frame_equal(correct, res)

@pytest.mark.parametrize('idx', [
    list('ABCD'*25),
    np.array(list('0123'*25)).astype(int),
    np.array(list('0123'*25)).astype(float),
])
def test_groupby_sum(idx):

    df = pd.DataFrame(dict(A=np.ones(100), B=np.ones(100)),
                      index=idx)
    correct = df.groupby(level=0).sum()
    correct.sort_index(inplace=True)

    spf = dsp.from_pandas(df, npartitions=2)
    assert spf.npartitions == 2
    grouped = spf.groupby_sum(split_out=4)

    assert grouped.npartitions == 4
    res = grouped.compute().todense()
    res.sort_index(inplace=True)

    pdt.assert_frame_equal(res, correct)


def test_from_ddf():
    ddf = dd.from_pandas(
        pd.DataFrame(np.random.rand(20, 4),
                     columns=list('ABCD')),
        npartitions=4
    )
    correct = ddf.compute()

    dsf = dsp.from_ddf(ddf)

    res = dsf.compute().todense()

    pdt.assert_frame_equal(correct, res)

    with pytest.raises(ValueError):
        ddf = ddf.assign(A="some str value")
        dsf = dsp.from_ddf(ddf)


def test_sdf_sort_index():
    data = pd.DataFrame(np.random.rand(20, 4),
                        columns=list('ABCD'),
                        index=np.random.choice([1,2,3,4,5,6], 20))
    ddf = dd.from_pandas(data,
        npartitions=4,
        sort=False,
    )

    dsf = dsp.from_ddf(ddf)
    dsf = dsf.sort_index()

    assert dsf.known_divisions

    res = dsf.compute()
    assert res.index.is_monotonic
    assert res.columns.tolist() == list('ABCD')


def test_sdf_sort_index_auto_partition():
    data = pd.DataFrame(np.random.rand(20000, 4),
                        columns=list('ABCD'),
                        index=np.random.choice(list(range(5000)), 20000))
    ddf = dd.from_pandas(data,
        npartitions=20,
        sort=False,
    )

    dsf = dsp.from_ddf(ddf)
    dsf = dsf.sort_index(npartitions='auto', partition_size=80000)

    assert dsf.known_divisions
    assert dsf.npartitions == 16

    res = dsf.compute()
    assert res.index.is_monotonic
    assert res.columns.tolist() == list('ABCD')


def test_get_partition(dsf):
    correct = dsf.compute().todense()
    parts = [dsf.get_partition(i).compute().todense()
             for i in range(dsf.npartitions)]
    res = pd.concat(parts, axis=0)
    pdt.assert_frame_equal(res, correct)