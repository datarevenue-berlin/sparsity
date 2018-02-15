import toolz
from dask.base import tokenize

import sparsity.sparse_frame as sp
import pandas as pd
from dask.dataframe.multi import require, required
from sparsity.dask.core import SparseFrame
from functools import partial
from dask.dataframe.core import is_broadcastable, _Frame
from toolz import unique, merge_sorted



def join_indexed_sparseframes(lhs, rhs, how='left'):
    """ Join two partitioned sparseframes along their index """

    (lhs, rhs), divisions, parts = align_partitions(lhs, rhs)
    divisions, parts = require(divisions, parts, required[how])

    left_empty = lhs._meta
    right_empty = rhs._meta

    name = 'join-indexed-' + tokenize(lhs, rhs, how)

    dsk = dict()
    for i, (a, b) in enumerate(parts):
        if a is None and how in ('right', 'outer'):
            a = left_empty
        if b is None and how in ('left', 'outer'):
            b = right_empty

        dsk[(name, i)] = (sp.SparseFrame.join, a, b, 1, how)

    meta = sp.SparseFrame.join(lhs._meta_nonempty, rhs._meta_nonempty, how=how)
    return SparseFrame(toolz.merge(lhs.dask, rhs.dask, dsk),
                       name, meta, divisions)


def align_partitions(*dfs):
    """ Mutually partition and align DataFrame blocks

    This serves as precursor to multi-dataframe operations like join, concat,
    or merge.

    Parameters
    ----------
    dfs: sequence of dd.DataFrame, dd.Series and dd.base.Scalar
        Sequence of dataframes to be aligned on their index

    Returns
    -------
    dfs: sequence of dd.DataFrame, dd.Series and dd.base.Scalar
        These must have consistent divisions with each other
    divisions: tuple
        Full divisions sequence of the entire result
    result: list
        A list of lists of keys that show which data exist on which
        divisions
    """
    _is_broadcastable = partial(is_broadcastable, dfs)
    dfs1 = [df for df in dfs
            if isinstance(df, (_Frame, SparseFrame)) and
            not _is_broadcastable(df)]
    if len(dfs) == 0:
        raise ValueError("dfs contains no DataFrame and Series")
    if not all(df.known_divisions for df in dfs1):
        raise ValueError("Not all divisions are known, can't align "
                         "partitions. Please use `set_index` or "
                         "`set_partition` to set the index.")

    divisions = list(unique(merge_sorted(*[df.divisions for df in dfs1])))
    dfs2 = [df.repartition(divisions, force=True)
            if isinstance(df, (SparseFrame)) else df for df in dfs]

    result = list()
    inds = [0 for df in dfs]
    for d in divisions[:-1]:
        L = list()
        for i, df in enumerate(dfs2):
            if isinstance(df, (_Frame, SparseFrame)):
                j = inds[i]
                divs = df.divisions
                if j < len(divs) - 1 and divs[j] == d:
                    L.append((df._name, inds[i]))
                    inds[i] += 1
                else:
                    L.append(None)
            else:    # Scalar has no divisions
                L.append(None)
        result.append(L)
    return dfs2, tuple(divisions), result


def _maybe_align_partitions(args):
    """Align DataFrame blocks if divisions are different.

    Note that if all divisions are unknown, but have equal npartitions, then
    they will be passed through unchanged. This is different than
    `align_partitions`, which will fail if divisions aren't all known"""
    _is_broadcastable = partial(is_broadcastable, args)
    dfs = [df for df in args
           if isinstance(df, (_Frame, SparseFrame)) and
           not _is_broadcastable(df)]
    if not dfs:
        return args

    divisions = dfs[0].divisions
    if not all(df.divisions == divisions for df in dfs):
        dfs2 = iter(align_partitions(*dfs)[0])
        return [a if not isinstance(a, (_Frame, SparseFrame))
                else next(dfs2) for a in args]
    return args