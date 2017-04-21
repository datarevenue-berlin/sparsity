import dask
import dask.dataframe as dd
import pandas as pd
from dask import threaded
from dask.base import normalize_token, tokenize
from dask.dataframe import methods
from dask.dataframe.core import apply, partial_by_order
from dask.dataframe.utils import make_meta as dd_make_meta
from dask.dataframe.utils import _nonempty_index
from dask.delayed import Delayed
from dask.optimize import cull
from scipy import sparse
from toolz import merge

import sparsity as sp
from sparsity.dask.indexing import _LocIndexer


def _make_meta(inp):
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

    def assign(self, **kwargs):
        for k, v in kwargs.items():
            if not isinstance(v, dd.Series):
                raise TypeError("Column assignment doesn't support type "
                                "{0}".format(type(v).__name__))

        # Figure out columns of the output
        meta = self._meta.assign(**{k: v._meta for k, v in kwargs.items()})
        name = 'assign-' + tokenize(self.assign, self, kwargs)

        # All dask objects and their keys
        dasks = [self] + list(kwargs.values())
        keys = [d._keys() for d in dasks]
        cols = list(kwargs.keys())

        dsk = {(name, i): (assign, frs[0], list(frs[1:]), cols)
               for i, frs in enumerate(zip(*keys))}
        dsk = merge(dsk, *[d.dask for d in dasks])

        return SparseFrame(dsk, name, meta, self.divisions)

    def map_partitions(self, func, meta, *args, **kwargs):
        return map_partitions(func, self, meta, *args, **kwargs)

    def to_delayed(self):
        return [Delayed(k, self.dask) for k in self._keys()]

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


def assign(df, dfs, cols):
    kwargs = dict(zip(cols, dfs))
    return df.assign(**kwargs)


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
