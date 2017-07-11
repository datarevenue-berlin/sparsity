import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask import threaded
from dask.base import normalize_token, tokenize
from dask.dataframe import methods
from dask.dataframe.core import (Scalar, Series, _emulate, _extract_meta,
                                 _Frame, _maybe_from_pandas, apply, funcname,
                                 no_default, partial, partial_by_order)
from dask.dataframe.utils import make_meta as dd_make_meta
from dask.dataframe.utils import _nonempty_index
from dask.delayed import Delayed
from dask.optimize import cull
from scipy import sparse
from toolz import merge, remove

import sparsity as sp
from sparsity.dask.indexing import _LocIndexer


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

class SparseFrame(dask.base.Base):

    _optimize = staticmethod(optimize)
    _default_get = staticmethod(threaded.get)
    _finalize = staticmethod(finalize)

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
