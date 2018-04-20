import math
from operator import getitem

from dask import base, delayed
from dask.sizeof import sizeof
from dask.base import tokenize
from dask.dataframe.shuffle import shuffle_group_get, set_partitions_pre, \
    remove_nans, set_index_post_series
from pandas._libs.algos import groupsort_indexer
from toolz import merge
from dask.utils import digit, insert

import sparsity as sp
import pandas as pd
import numpy as np

from sparsity.dask import SparseFrame


def sort_index(df, npartitions=None, shuffle='tasks',
               drop=True, upsample=1.0, divisions=None,
               partition_size=128e6, **kwargs):
    """ See _Frame.set_index for docstring """
    if npartitions == 'auto':
        repartition = True
        npartitions = max(100, df.npartitions)
    else:
        if npartitions is None:
            npartitions = df.npartitions
        repartition = False

    index2 = index_to_series(df.index)

    if divisions is None:
        divisions = index2._repartition_quantiles(npartitions, upsample=upsample)
        if repartition:
            parts = df.to_delayed()
            sizes = [delayed(sizeof)(part) for part in parts]
        else:
            sizes = []
        iparts = index2.to_delayed()
        mins = [ipart.min() for ipart in iparts]
        maxes = [ipart.max() for ipart in iparts]
        divisions, sizes, mins, maxes = base.compute(divisions, sizes, mins, maxes)
        divisions = divisions.tolist()

        empty_dataframe_detected = pd.isnull(divisions).all()
        if repartition or empty_dataframe_detected:
            total = sum(sizes)
            npartitions = max(math.ceil(total / partition_size), 1)
            npartitions = min(npartitions, df.npartitions)
            n = len(divisions)
            try:
                divisions = np.interp(x=np.linspace(0, n - 1, npartitions + 1),
                                      xp=np.linspace(0, n - 1, n),
                                      fp=divisions).tolist()
            except (TypeError, ValueError):  # str type
                indexes = np.linspace(0, n - 1, npartitions + 1).astype(int)
                divisions = [divisions[i] for i in indexes]

    return set_partition(df, divisions, shuffle=shuffle, drop=drop,
                         **kwargs)


def index_to_series(idx):
    return idx.map_partitions(lambda x: x.to_series(),
                              meta=idx._meta.to_series())


def set_partition(sf: SparseFrame, divisions: list,
                  max_branch=32, drop=True, shuffle=None):
    """ Group DataFrame by index

    Sets a new index and partitions data along that index according to
    divisions.  Divisions are often found by computing approximate quantiles.
    The function ``set_index`` will do both of these steps.

    Parameters
    ----------
    sf: DataFrame/Series
        Data that we want to re-partition
    index: string or Series
        Column to become the new index
    divisions: list
        Values to form new divisions between partitions
    drop: bool, default True
        Whether to delete columns to be used as the new index
    shuffle: str (optional)
        Either 'disk' for an on-disk shuffle or 'tasks' to use the task
        scheduling framework.  Use 'disk' if you are on a single machine
        and 'tasks' if you are on a distributed cluster.
    max_branch: int (optional)
        If using the task-based shuffle, the amount of splitting each
        partition undergoes.  Increase this for fewer copies but more
        scheduler overhead.

    See Also
    --------
    set_index
    shuffle
    partd
    """
    index = index_to_series(sf.index)
    partitions = index.map_partitions(set_partitions_pre,
                                      divisions=divisions,
                                      meta=pd.Series([0]))
    sf2 = sf.assign(_partitions=partitions)

    df3 = rearrange_by_index(sf2, max_branch=max_branch,
                             npartitions=len(divisions) - 1, shuffle=shuffle)

    df4 = df3.map_partitions(sort_index_post_series,
                             index_name=index.name,
                             meta=sort_index_post_series(df3._meta, index.name))

    df4.divisions = divisions

    return df4.map_partitions(sp.SparseFrame.sort_index, df4._meta)


def sort_index_post_series(df, index_name):
    df2 = df.drop('_partitions', axis=1)
    df2.index.name = index_name
    return df2


def rearrange_by_index(df, npartitions=None, max_branch=None,
                       shuffle='tasks'):
    if shuffle == 'tasks':
        return rearrange_by_index_tasks(df, max_branch, npartitions)
    else:
        raise NotImplementedError("Unknown shuffle method %s" % shuffle)


def rearrange_by_index_tasks(df, max_branch=32, npartitions=None):
    """ Order divisions of DataFrame so that all values within index align

    This enacts a task-based shuffle

    See also:
        rearrange_by_column_disk
        set_partitions_tasks
        shuffle_tasks
    """
    max_branch = max_branch or 32
    n = df.npartitions

    stages = int(np.math.ceil(math.log(n) / math.log(max_branch)))
    if stages > 1:
        k = int(math.ceil(n ** (1 / stages)))
    else:
        k = n

    groups = []
    splits = []
    joins = []

    inputs = [tuple(digit(i, j, k) for j in range(stages))
              for i in range(k**stages)]

    token = tokenize(df, max_branch)

    start = dict((('shuffle-join-' + token, 0, inp),
                  (df._name, i) if i < df.npartitions else df._meta)
                 for i, inp in enumerate(inputs))

    for stage in range(1, stages + 1):
        group = dict((('shuffle-group-' + token, stage, inp),
                      (shuffle_index, ('shuffle-join-' + token, stage - 1, inp),
                       stage - 1, k, n))
                     for inp in inputs)

        split = dict((('shuffle-split-' + token, stage, i, inp),
                      (getitem, ('shuffle-group-' + token, stage, inp), i))
                     for i in range(k)
                     for inp in inputs)

        join = dict((('shuffle-join-' + token, stage, inp),
                     (sp.SparseFrame.vstack,
                      [('shuffle-split-' + token, stage, inp[stage - 1],
                       insert(inp, stage - 1, j)) for j in range(k)]))
                    for inp in inputs)
        groups.append(group)
        splits.append(split)
        joins.append(join)

    end = dict((('shuffle-' + token, i),
                ('shuffle-join-' + token, stages, inp))
               for i, inp in enumerate(inputs))

    dsk = merge(df.dask, start, end, *(groups + splits + joins))
    df2 = SparseFrame(dsk, 'shuffle-' + token, df, df.divisions)

    if npartitions is not None and npartitions != df.npartitions:
        parts = [i % df.npartitions for i in range(npartitions)]
        token = tokenize(df2, npartitions)
        dsk = {('repartition-group-' + token, i): (shuffle_group_2, k)
               for i, k in enumerate(df2.__dask_keys__())}
        for p in range(npartitions):
            dsk[('repartition-get-' + token, p)] = \
                (shuffle_group_get, ('repartition-group-' + token, parts[p]), p)

        df3 = SparseFrame(merge(df2.dask, dsk), 'repartition-get-' + token, df2,
                        [None] * (npartitions + 1))
    else:
        df3 = df2
        df3.divisions = (None,) * (df.npartitions + 1)

    return df3


def shuffle_index(sf: sp.SparseFrame, stage, k, npartitions):
    ind = sf['_partitions'].todense().astype(np.int)
    c = ind._values
    typ = np.min_scalar_type(npartitions * 2)
    c = c.astype(typ)

    npartitions, k, stage = [np.array(x, dtype=np.min_scalar_type(x))[()]
                             for x in [npartitions, k, stage]]

    c = np.mod(c, npartitions, out=c)
    c = np.floor_divide(c, k ** stage, out=c)
    c = np.mod(c, k, out=c)

    indexer, locations = groupsort_indexer(c.astype(np.int64), k)
    df2 = sf.take(indexer)
    locations = locations.cumsum()
    parts = [df2.iloc[a:b] for a, b in zip(locations[:-1], locations[1:])]

    return dict(zip(range(k), parts))


def shuffle_group_2(sf: sp.SparseFrame):
    if not len(sf):
        return {}, sf
    ind = sf['_partitions'].todense()._values.astype(np.int64)
    n = ind.max() + 1
    indexer, locations = groupsort_indexer(ind.view(np.int64), n)
    df2 = sf.take(indexer)
    locations = locations.cumsum()
    parts = [df2.iloc[a:b] for a, b in zip(locations[:-1], locations[1:])]
    result2 = dict(zip(range(n), parts))
    return result2, sf.iloc[:0]
