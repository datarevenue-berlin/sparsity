from operator import getitem

import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd
import toolz
from dask import threaded
from dask.base import normalize_token, tokenize
from dask.dataframe import methods
from dask.dataframe.core import (Scalar, Series, _emulate, _extract_meta,
                                 _Frame, _maybe_from_pandas, apply, funcname,
                                 no_default, partial, partial_by_order,
                                 repartition_divisions, repartition_npartitions, split_evenly)
from dask.dataframe.utils import make_meta as dd_make_meta
from dask.dataframe.utils import _nonempty_index
from dask.dataframe.multi import require
from dask.delayed import Delayed
from dask.optimize import cull
from scipy import sparse
from toolz import merge, remove

import sparsity as sp
from sparsity.dask.indexing import _LocIndexer
from .multi import align_partitions

def _make_meta(inp):
    if isinstance(inp, sp.SparseFrame) and inp.empty:
        return inp
    if isinstance(inp, sp.SparseFrame):
        return inp.iloc[:0]
    else:
        meta = dd_make_meta(inp)
        if isinstance(meta, pd.core.generic.NDFrame):
            return sp.SparseFrame(meta)
        return meta

def _meta_nonempty(x):
    idx = _nonempty_index(x.index)
    return sp.SparseFrame(sparse.csr_matrix((len(idx), len(x.columns))),
                     index=idx, columns=x.columns)

def optimize(dsk, keys, **kwargs):
    dsk, _ = cull(dsk, keys)
    return dsk

def finalize(results):
    results = [r for r in results if not r.empty]
    return sp.SparseFrame.vstack(results)


class SparseFrame(dask.base.DaskMethodsMixin):

    def __init__(self, dsk, name, meta, divisions=None):
        self.dask = dsk
        self._name = name
        self._meta = _make_meta(meta)

        if divisions:
            self.known_divisions = True
        else:
            self.known_divisions = False

        self.divisions = tuple(divisions)
        self.ndim = 2

        self.loc = _LocIndexer(self)

    def __dask_graph__(self):
        return self.dask

    def __dask_keys__(self):
        return self._keys()

    __dask_scheduler__ = staticmethod(dask.threaded.get)

    @staticmethod
    def __dask_optimize__(dsk, keys, **kwargs):
        # We cull unnecessary tasks here. Note that this isn't necessary,
        # dask will do this automatically, this just shows one optimization
        # you could do.
        dsk2 = optimize(dsk, keys)
        return dsk2

    def __dask_postcompute__(self):
        return finalize, ()

    @property
    def npartitions(self):
        return len(self.divisions) - 1

    @property
    def _meta_nonempty(self):
        return _meta_nonempty(self._meta)

    @property
    def columns(self):
        return self._meta.columns

    @property
    def index(self):
        return self._meta.index

    def map_partitions(self, func, meta, *args, **kwargs):
        return map_partitions(func, self, meta, *args, **kwargs)

    def to_delayed(self):
        return [Delayed(k, self.dask) for k in self._keys()]

    def assign(self, **kwargs):
        for k, v in kwargs.items():
            if not (isinstance(v, (Series, Scalar, pd.Series)) or
                    np.isscalar(v)):
                raise TypeError("Column assignment doesn't support type "
                                "{0}".format(type(v).__name__))
        pairs = list(sum(kwargs.items(), ()))

        # Figure out columns of the output
        df2 = self._meta.assign(**_extract_meta(kwargs))
        return elemwise(methods.assign, self, *pairs, meta=df2)

    def _keys(self):
        return [(self._name, i) for i in range(self.npartitions)]

    @property
    def _repr_divisions(self):
        name = "npartitions={0}".format(self.npartitions)
        if self.known_divisions:
            divisions = pd.Index(self.divisions, name=name)
        else:
            # avoid to be converted to NaN
            divisions = pd.Index(['None'] * (self.npartitions + 1),
                                 name=name)
        return divisions

    @property
    def _repr_data(self):
        index = self._repr_divisions
        if len(self._meta._columns) > 50:
            cols = self._meta.columns[:25].append(self._meta.columns[-25:])
            data = [['...'] * 50] * len(index)
        else:
            cols = self._meta._columns
            data = [['...'] * len(cols)] * len(index)
        return pd.DataFrame(data, columns=cols, index=index)

    def repartition(self, npartitions=None, divisions=None, force=False):
        if divisions is not None:
            return repartition(self, divisions, force)
        elif npartitions is not None:
            return repartition_npartitions(self, npartitions)
        raise ValueError('Either divisions or npartitions must be supplied')

    def join(self, other, on=None, how='left', lsuffix='',
             rsuffix='', npartitions=None):

        if not isinstance(other, (SparseFrame)):
            raise ValueError('other must be SparseFrame')

    def __repr__(self):
        return \
            """
Dask SparseFrame Structure:
{data}
Dask Name: {name}, {task} tasks
            """.format(
                data=self._repr_data.to_string(max_rows=5,
                                               show_dimensions=False),
                name=self._name,
                task=len(self.dask)
            )


required = {'left': [0], 'right': [1], 'inner': [0, 1], 'outer': []}


def repartition(df, divisions=None, force=False):
    """ Repartition dataframe along new divisions
    Dask.DataFrame objects are partitioned along their index.  Often when
    multiple dataframes interact we need to align these partitionings.  The
    ``repartition`` function constructs a new DataFrame object holding the same
    data but partitioned on different values.  It does this by performing a
    sequence of ``loc`` and ``concat`` calls to split and merge the previous
    generation of partitions.
    Parameters
    ----------
    divisions : list
        List of partitions to be used
    force : bool, default False
        Allows the expansion of the existing divisions.
        If False then the new divisions lower and upper bounds must be
        the same as the old divisions.
    Examples
    --------
    >>> sf = sf.repartition([0, 5, 10, 20])  # doctest: +SKIP
    """

    token = tokenize(df, divisions)
    if isinstance(df, SparseFrame):
        tmp = 'repartition-split-' + token
        out = 'repartition-merge-' + token
        dsk = repartition_divisions(df.divisions, divisions,
                                    df._name, tmp, out, force=force)
        return SparseFrame(merge(df.dask, dsk), out,
                           df._meta, divisions)
    raise ValueError('Data must be DataFrame or Series')


def repartition_npartitions(df, npartitions):
    """ Repartition dataframe to a smaller number of partitions """
    new_name = 'repartition-%d-%s' % (npartitions, tokenize(df))
    if df.npartitions == npartitions:
        return df
    elif df.npartitions > npartitions:
        npartitions_ratio = df.npartitions / npartitions
        new_partitions_boundaries = [int(new_partition_index * npartitions_ratio)
                                     for new_partition_index in range(npartitions + 1)]
        dsk = {}
        for new_partition_index in range(npartitions):
            value = (sp.SparseFrame.vstack,
                     [(df._name, old_partition_index) for old_partition_index in
                      range(new_partitions_boundaries[new_partition_index],
                            new_partitions_boundaries[new_partition_index + 1])])
            dsk[new_name, new_partition_index] = value
        divisions = [df.divisions[new_partition_index]
                     for new_partition_index in new_partitions_boundaries]
        return SparseFrame(merge(df.dask, dsk), new_name, df._meta, divisions)
    else:
        original_divisions = divisions = pd.Series(df.divisions)
        if (df.known_divisions and (np.issubdtype(divisions.dtype, np.datetime64) or
                                    np.issubdtype(divisions.dtype, np.number))):
            if np.issubdtype(divisions.dtype, np.datetime64):
                divisions = divisions.values.astype('float64')

            if isinstance(divisions, pd.Series):
                divisions = divisions.values

            n = len(divisions)
            divisions = np.interp(x=np.linspace(0, n, npartitions + 1),
                                  xp=np.linspace(0, n, n),
                                  fp=divisions)
            if np.issubdtype(original_divisions.dtype, np.datetime64):
                divisions = pd.Series(divisions).astype(original_divisions.dtype).tolist()
            elif np.issubdtype(original_divisions.dtype, np.integer):
                divisions = divisions.astype(original_divisions.dtype)

            if isinstance(divisions, np.ndarray):
                divisions = divisions.tolist()

            divisions = list(divisions)
            divisions[0] = df.divisions[0]
            divisions[-1] = df.divisions[-1]

            return df.repartition(divisions=divisions)
        else:
            ratio = npartitions / df.npartitions
            split_name = 'split-%s' % tokenize(df, npartitions)
            dsk = {}
            last = 0
            j = 0
            for i in range(df.npartitions):
                new = last + ratio
                if i == df.npartitions - 1:
                    k = npartitions - j
                else:
                    k = int(new - last)
                dsk[(split_name, i)] = (split_evenly, (df._name, i), k)
                for jj in range(k):
                    dsk[(new_name, j)] = (getitem, (split_name, i), jj)
                    j += 1
                last = new

            divisions = [None] * (npartitions + 1)
            return SparseFrame(merge(df.dask, dsk), new_name, df._meta, divisions)



def is_broadcastable(dfs, s):
    """
    This Series is broadcastable against another dataframe in the sequence
    """
    return (isinstance(s, Series) and
            s.npartitions == 1 and
            s.known_divisions and
            any(s.divisions == (min(df.columns), max(df.columns))
                for df in dfs if isinstance(df, (SparseFrame, dd.DataFrame))))


def elemwise(op, *args, **kwargs):
    """ Elementwise operation for dask.Sparseframes

    Parameters
    ----------
    op: function
        Function that takes as first parameter the underlying df
    args:
        Contains Dataframes
    kwargs:
        Contains meta.
    """
    meta = kwargs.pop('meta', no_default)

    _name = funcname(op) + '-' + tokenize(op, kwargs, *args)

    # if pd.Series or pd.DataFrame change to dd.DataFrame
    args = _maybe_from_pandas(args)

    # Align DataFrame blocks if divisions are different.
    from .multi import _maybe_align_partitions  # to avoid cyclical import
    args = _maybe_align_partitions(args)

    # extract all dask instances
    dasks = [arg for arg in args if isinstance(arg, (SparseFrame, _Frame,
                                                     Scalar))]
    # extract all dask frames
    dfs = [df for df in dasks if isinstance(df, (_Frame, SparseFrame))]

    # We take divisions from the first dask frame
    divisions = dfs[0].divisions

    _is_broadcastable = partial(is_broadcastable, dfs)
    dfs = list(remove(_is_broadcastable, dfs))
    n = len(divisions) - 1

    other = [(i, arg) for i, arg in enumerate(args)
             if not isinstance(arg, (_Frame, Scalar, SparseFrame))]

    # Get dsks graph tuple keys and adjust the key length of Scalar
    keys = [d._keys() * n if isinstance(d, Scalar) or _is_broadcastable(d)
            else d._keys() for d in dasks]

    if other:
        dsk = {(_name, i):
               (apply, partial_by_order, list(frs),
                {'function': op, 'other': other})
               for i, frs in enumerate(zip(*keys))}
    else:
        dsk = {(_name, i): (op,) + frs for i, frs in enumerate(zip(*keys))}
    dsk = merge(dsk, *[d.dask for d in dasks])

    if meta is no_default:
        if len(dfs) >= 2 and len(dasks) != len(dfs):
            # should not occur in current funcs
            msg = 'elemwise with 2 or more DataFrames and Scalar is not supported'
            raise NotImplementedError(msg)
        meta = _emulate(op, *args, **kwargs)

    return SparseFrame(dsk, _name, meta, divisions)


def map_partitions(func, ddf, meta, **kwargs):
    dsk = {}
    name = func.__name__
    token = tokenize(func, meta, **kwargs)
    name = '{0}-{1}'.format(name, token)

    for i in range(ddf.npartitions):
        value = (ddf._name, i)
        dsk[(name, i)] = (apply_and_enforce, func, value, kwargs, meta)

    return SparseFrame(merge(dsk, ddf.dask), name, meta, ddf.divisions)


def apply_and_enforce(func, arg, kwargs, meta):
    sf = func(arg, **kwargs)
    columns = meta.columns
    if isinstance(sf, sp.SparseFrame):
        if len(sf.data.data) == 0:
            return meta
        if (len(columns) == len(sf.columns) and
                    type(columns) is type(sf.columns) and
                columns.equals(sf.columns)):
            # if target is identical, rename is not necessary
            return sf
        else:
            sf._columns = columns
    return sf


normalize_token.register((SparseFrame,), lambda a: a._name)
