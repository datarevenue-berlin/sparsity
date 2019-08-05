import datetime as dt
import os
from uuid import uuid4

import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd
import pandas.util.testing as pdt
import pytest
from dask import delayed
from distributed import Client

import sparsity as sp
import sparsity.dask as dsp
from sparsity.dask.reshape import one_hot_encode
from .conftest import tmpdir

dask.config.set(scheduler=dask.local.get_sync)


@pytest.fixture
def dsf():
    return dsp.from_pandas(pd.DataFrame(np.random.rand(10,2),
                                        columns=['A', 'B']),
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


def test_map_partitions_mappable():
    data = pd.DataFrame(np.ones((10, 2)))
    dsf = dsp.from_pandas(data, chunksize=5)
    
    def foo(sf, x, y):
        return sp.SparseFrame(sf.data * x * y,
                              index=sf.index, columns=sf.columns)
    
    dsf = dsf.map_partitions(foo, dsf._meta, x=(i for i in range(2, 4)), y=2)
    res = dsf.compute().todense()

    assert res.shape == (10, 2)
    assert (res.iloc[:5, :] == 4).all().all()
    assert (res.iloc[5:, :] == 6).all().all()


def test_todense():
    data = pd.DataFrame(np.random.rand(10, 2))
    dsf = dsp.from_pandas(data, npartitions=3)
    res = dsf.todense()
    assert isinstance(res, dd.DataFrame)
    computed = res.compute()
    pdt.assert_frame_equal(computed, data, check_dtype=False)


def test_todense_series():
    data = pd.DataFrame(np.random.rand(10, 2))
    dsf = dsp.from_pandas(data, npartitions=3)[0]
    res = dsf.todense()
    assert isinstance(res, dd.Series)
    computed = res.compute()
    pdt.assert_series_equal(computed, data[0], check_dtype=False)


# noinspection PyStatementEffect
@pytest.mark.parametrize('item, raises', [
    ('X', False),
    (['X', 'Y'], False),
    ('A', True),
    (['A'], True),
    (['X', 'A'], True),
    (['A', 'B'], True),
])
def test_getitem(item, raises):
    df = pd.DataFrame(np.random.rand(10, 3), columns=list('XYZ'),
                      index=list('ABCDEFGHIJ'))
    dsf = dsp.from_pandas(df, npartitions=2)
    
    correct_cols = item if isinstance(item, list) else [item]
    
    if raises:
        with pytest.raises(KeyError):
            dsf[item]
        return
    
    res = dsf[item]
    assert res.columns.tolist() == correct_cols
    res_computed = res.compute()
    assert res_computed.columns.tolist() == correct_cols
    if not isinstance(item, list):
        pdt.assert_series_equal(df[item], res_computed.todense())
    else:
        pdt.assert_frame_equal(df[item], res_computed.todense())


@pytest.mark.parametrize('item', [
    'X',
    ['X', 'Y'],
])
def test_getitem_empty(item):
    df = pd.DataFrame([], columns=list('XYZ'), dtype=int)
    dsf = dsp.from_ddf(dd.from_pandas(df, npartitions=1))
    
    correct_cols = item if isinstance(item, list) else [item]
    res = dsf[item]
    assert res.columns.tolist() == correct_cols
    res_computed = res.compute()
    assert res_computed.columns.tolist() == correct_cols
    if not isinstance(item, list):
        pdt.assert_series_equal(df[item], res_computed.todense())
    else:
        pdt.assert_frame_equal(df[item], res_computed.todense())
    

@pytest.mark.parametrize('iindexer, correct_shape', [
    (slice('A', 'B'), (2, 2)),
    (slice('C', None), (8, 2)),
    (slice(None, 'C'), (3, 2)),
])
def test_loc(iindexer, correct_shape):
    df = pd.DataFrame(np.random.rand(10, 2),
                      index=list('ABCDEFGHIJ'))
    ddf = dd.from_pandas(df, npartitions=2)
    ddf.loc[iindexer]

    dsf = dsp.from_pandas(df, npartitions=2)
    fut = dsf.loc[iindexer]
    assert fut._meta.empty
    res = fut.compute()

    assert isinstance(res, sp.SparseFrame)
    assert res.shape == correct_shape


def test_dask_loc(clickstream):
    sf = one_hot_encode(dd.from_pandas(clickstream, npartitions=10),
                        categories={'page_id': list('ABCDE'),
                                    'other_categorical': list('FGHIJ')},
                        index_col=['index', 'id'])
    res = sf.loc['2016-01-15':'2016-02-15']
    res = res.compute()
    assert res.index.levels[0].max().date() == dt.date(2016, 2, 15)
    assert res.index.levels[0].min().date() == dt.date(2016, 1, 15)


def test_dask_multi_index_loc(clickstream):
    sf = one_hot_encode(dd.from_pandas(clickstream, npartitions=10),
                        categories={'page_id': list('ABCDE'),
                                    'other_categorical': list('FGHIJ')},
                        index_col=['index', 'id'])
    res = sf.loc['2016-01-15':'2016-02-15']
    res = res.compute()
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


def test_one_hot_prefixes_sep(clickstream):
    ddf = dd.from_pandas(clickstream, npartitions=10)
    dsf = one_hot_encode(ddf,
                         categories={'page_id': list('ABCDE'),
                                     'other_categorical': list('FGHIJ')},
                         index_col=['index', 'id'],
                         prefixes=True, sep='=')
    correct_columns = list(map(lambda x: 'page_id=' + x, list('ABCDE'))) \
        + list(map(lambda x: 'other_categorical=' + x, list('FGHIJ')))
    assert sorted(dsf.columns) == sorted(correct_columns)


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


def test_one_hot_dense_column(clickstream):
    ddf = dd.from_pandas(clickstream, npartitions=10)
    dsf = one_hot_encode(ddf,
                         categories={'page_id': list('ABCDE'),
                                     'other_categorical': list('FGHIJ'),
                                     'id': False},
                         )
    assert dsf._meta.empty
    assert set(dsf.columns) == set('ABCDEFGHIJ') | {'id'}
    sf = dsf.compute()
    assert sf.shape == (100, 11)
    assert set(sf.columns) == set('ABCDEFGHIJ') | {'id'}


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

        dsf = dsp.read_npz(os.path.join(tmp, '*.npz'), read_divisions=True)
        sf = dsf.compute()
        assert dsf.known_divisions
    assert np.all(sf.data.toarray() == np.identity(100))


def test_to_npz(dsf):
    dense = dsf.compute().todense()
    with tmpdir() as tmp:
        path = os.path.join(tmp, '*.npz')
        dsf.to_npz(path)
        loaded = dsp.read_npz(path)
        assert loaded.known_divisions
        res = loaded.compute().todense()
    pdt.assert_frame_equal(dense, res)


def test_from_delayed(dsf_arange, sf_arange):
    partitions = dsf_arange.to_delayed()
    res = dsp.from_delayed(partitions)
    
    assert isinstance(res, dsp.SparseFrame)
    assert res.npartitions == dsf_arange.npartitions
    pdt.assert_index_equal(res.columns, dsf_arange.columns)
    cmp = res.compute()
    assert isinstance(cmp, sp.SparseFrame)
    pdt.assert_frame_equal(cmp.todense(), sf_arange.todense())


def test_from_delayed_alter_delayed(dsf_arange, sf_arange):
    @delayed
    def foo(sf):
        return sf[['A', 'B']]
    
    partitions = dsf_arange.to_delayed()
    partitions = list(map(foo, partitions))
    res = dsp.from_delayed(partitions)
    
    assert isinstance(res, dsp.SparseFrame)
    assert res.npartitions == dsf_arange.npartitions
    assert res.columns.tolist() == ['A', 'B']
    cmp = res.compute()
    assert isinstance(cmp, sp.SparseFrame)
    pdt.assert_frame_equal(cmp.todense(), sf_arange[['A', 'B']].todense())


def test_from_delayed_meta(dsf_arange, sf_arange):
    @delayed
    def foo(sf):
        return sf[['A', 'B']]
    
    partitions = dsf_arange.to_delayed()
    partitions = list(map(foo, partitions))
    res = dsp.from_delayed(partitions, meta=dsf_arange._meta[['A', 'B']])
    
    assert isinstance(res, dsp.SparseFrame)
    assert res.npartitions == dsf_arange.npartitions
    assert res.columns.tolist() == ['A', 'B']
    cmp = res.compute()
    assert isinstance(cmp, sp.SparseFrame)
    pdt.assert_frame_equal(cmp.todense(), sf_arange[['A', 'B']].todense())
    

def test_assign_column():
    s = pd.Series(np.arange(10))
    ds = dd.from_pandas(s, npartitions=2)

    f = pd.DataFrame(np.random.rand(10, 2), columns=['a', 'b'])
    dsf = dsp.from_pandas(f, npartitions=2)

    dsf = dsf.assign(new=ds)
    assert dsf._meta.empty
    sf = dsf.compute()
    assert np.all((sf.todense() == f.assign(new=s)).values)


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


def test_add():
    df = pd.DataFrame(np.identity(12))
    df2 = df.copy()
    df2.index += 1

    sf1 = sp.SparseFrame(df)
    sf2 = sp.SparseFrame(df2)
    correct = sf1.add(sf2).todense()

    dsf = dsp.from_pandas(df, npartitions=4)
    dsf2 = dsp.from_pandas(df2, npartitions=4)

    res = dsf.add(dsf2).compute().todense()
    pdt.assert_frame_equal(res, correct)


@pytest.mark.parametrize('idx', [
    np.random.choice([uuid4() for i in range(1000)], size=10000),
    np.random.randint(0, 10000, 10000),
    np.random.randint(0, 10000, 10000).astype(float),
    pd.date_range('01-01-1970', periods=10000, freq='s'),
])
def test_groupby_sum(idx):
    for sorted in [True, False]:
        df = pd.DataFrame(dict(A=np.ones(len(idx)), B=np.arange(len(idx))),
                          index=idx, dtype=np.float)
        correct = df.groupby(level=0).sum()
        correct.sort_index(inplace=True)

        spf = dsp.from_ddf(dd.from_pandas(df, npartitions=10, sort=sorted))
        assert spf.npartitions == 10
        grouped = spf.groupby_sum(split_out=4)
        grouped2 = spf.groupby_sum(split_out=12)

        assert grouped.npartitions == 4
        res1 = grouped.compute().todense()
        res1.sort_index(inplace=True)

        assert grouped2.npartitions == 12
        res2 = grouped2.compute().todense()
        res2.sort_index(inplace=True)

        pdt.assert_frame_equal(res1, correct)
        pdt.assert_frame_equal(res2, correct)


@pytest.mark.parametrize('how', ['left', 'inner'])
def test_distributed_join_shortcut(how):
    left = pd.DataFrame(np.identity(10),
                        index=np.arange(10),
                        columns=list('ABCDEFGHIJ'))
    right = pd.DataFrame(np.identity(10),
                         index=np.arange(5, 15),
                         columns=list('KLMNOPQRST'))
    correct = left.join(right, how=how).fillna(0)

    d_left = dsp.from_pandas(left, npartitions=2)
    d_right = sp.SparseFrame(right)

    joined = d_left.join(d_right, how=how)

    res = joined.compute().todense()

    pdt.assert_frame_equal(correct, res)


@pytest.mark.parametrize('idx, sorted', [
    (list('ABCD'*25), True),
    (np.array(list('0123'*25)).astype(int), True),
    (np.array(list('0123'*25)).astype(float), True),
    (list('ABCD'*25), False),
    (np.array(list('0123'*25)).astype(int), False),
    (np.array(list('0123'*25)).astype(float), False),
])
def test_groupby_sum(idx, sorted):

    df = pd.DataFrame(dict(A=np.ones(100), B=np.ones(100)),
                      index=idx)
    correct = df.groupby(level=0).sum()
    correct.sort_index(inplace=True)

    spf = dsp.from_pandas(df, npartitions=2)
    if not sorted:
        spf.divisions = [None] * (spf.npartitions + 1)
    assert spf.npartitions == 2
    grouped = spf.groupby_sum(split_out=3)

    assert grouped.npartitions == 3
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


def test_set_index(clickstream):
    ddf = dd.from_pandas(clickstream, npartitions=10)
    dsf = one_hot_encode(ddf,
                         categories={'page_id': list('ABCDE'),
                                     'other_categorical': list('FGHIJ')},
                         order=['other_categorical', 'page_id'],
                         index_col=['index', 'id'])
    dense = dsf.compute().set_index(level=1).todense()
    res = dsf.set_index(level=1).compute().todense()

    pdt.assert_frame_equal(dense, res)


def test_persist(dsf):
    correct = dsf.compute().todense()
    client = Client()
    persisted = client.persist(dsf)

    res = persisted.compute().todense()

    pdt.assert_frame_equal(res, correct)


@pytest.fixture(scope='session')
def dsf_arange(sf_arange):
    return dsp.from_pandas(sf_arange.todense(), chunksize=5)


def test_sample(dsf_arange):
    res = dsf_arange.sample(frac=0.2)
    assert res.todense().map_partitions(len).compute().tolist() == [1, 1]
    cmp = res.compute()
    assert cmp.shape == (2, 3)
    assert not cmp.todense().duplicated().any()


def test_sample_axis(dsf_arange):
    res = dsf_arange.sample(n=2, axis=1)
    cmp = res.compute()
    assert cmp.shape == (10, 2)


def test_sample_errors(dsf_arange):
    with pytest.raises(NotImplementedError):
        dsf_arange.sample(n=5)
    with pytest.raises(ValueError):
        dsf_arange.sample(n=5, frac=0.5, axis=1)
    with pytest.raises(ValueError):
        dsf_arange.sample()
    with pytest.raises(NotImplementedError):
        dsf_arange.sample(n=5, random_state=123)
    with pytest.raises(NotImplementedError):
        dsf_arange.sample(n=5, weights='asd')


def test_sample_replace(dsf_arange):
    res = dsf_arange.sample(frac=1.2, replace=True)
    cmp = res.compute()
    assert cmp.shape == (12, 3)
    assert cmp.todense().duplicated().any()


def test_sample_compute_twice(dsf_arange):
    for _ in range(5):  # increase chance of failing
        res = dsf_arange.sample(frac=0.2, replace=False)
        cmp1 = res.compute().todense()
        cmp2 = res.compute().todense()
        pdt.assert_frame_equal(cmp1, cmp2)
