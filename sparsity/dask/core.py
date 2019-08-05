from itertools import repeat
from operator import getitem, itemgetter
from pprint import pformat
from types import GeneratorType

import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask import threaded
from dask.base import normalize_token, tokenize
from dask.dataframe import methods
from dask.dataframe.core import (Index, Scalar, Series, _Frame, _emulate,
                                 _extract_meta, _maybe_from_pandas, apply,
                                 check_divisions, funcname, get_parallel_type,
                                 hash_shard, no_default, partial,
                                 partial_by_order, pd_split, split_evenly,
                                 split_out_on_index)
from dask.dataframe.utils import _nonempty_index, make_meta, meta_nonempty
from dask.delayed import Delayed, delayed
from dask.optimization import cull
from dask.utils import derived_from, random_state_data
from scipy import sparse
from toolz import merge, partition_all, remove

import sparsity as sp
from sparsity.dask.indexing import _LocIndexer


@get_parallel_type.register(sp.SparseFrame)
def get_parallel_type_sparsity(_):
    return SparseFrame


@make_meta.register(sp.SparseFrame)
def make_meta_sparsity(inp):
    if isinstance(inp, sp.SparseFrame) and inp.empty:
        return inp
    if isinstance(inp, sp.SparseFrame):
        return inp.iloc[:0]
    else:
        raise NotImplementedError("Can't make meta for type: {}"
                                  .format(str(type(inp))))

@meta_nonempty.register(sp.SparseFrame)
def meta_nonempty_sparsity(x):
    idx = _nonempty_index(x.index)
    return sp.SparseFrame(sparse.csr_matrix((len(idx), len(x.columns))),
                     index=idx, columns=x.columns)


def optimize(dsk, keys, **kwargs):
    dsk, _ = cull(dsk, keys)
    return dsk


def finalize(results):
    if all(map(lambda x: x.empty, results)):
        return results[0]
    results = [r for r in results if not r.empty]
    return sp.SparseFrame.vstack(results)


def from_delayed(dfs, meta=None, divisions=None, prefix="from-delayed"):
    for df in dfs:
        if not isinstance(df, Delayed):
            raise TypeError("Expected Delayed object, got %s" % type(df).__name__)

    if meta is None:
        meta = delayed(make_meta)(dfs[0]).compute()
    else:
        meta = make_meta(meta)

    name = prefix + "-" + tokenize(*dfs)
    dsk = merge(df.dask for df in dfs)
    dsk.update(
        {
            (name, i): (lambda x: x, df.key)
            for (i, df) in enumerate(dfs)
        }
    )
    
    if divisions is None or divisions == "sorted":
        divs = [None] * (len(dfs) + 1)
    else:
        divs = tuple(divisions)
        if len(divs) != len(dfs) + 1:
            raise ValueError("divisions should be a tuple of len(dfs) + 1")
    
    sf = SparseFrame(dsk, name, meta, divisions=divs)
    return sf


class SparseFrame(dask.base.DaskMethodsMixin):

    def __init__(self, dsk, name, meta, divisions=None):
        if isinstance(meta, SparseFrame):
            # TODO: remove this case once we subclass from dask._Frame
            meta = meta._meta
        if not isinstance(meta, sp.SparseFrame):
            meta = sp.SparseFrame(meta)

        self.dask = dsk
        self._name = name
        self._meta = make_meta(meta)

        self.divisions = tuple(divisions)
        self.ndim = 2

        self.loc = _LocIndexer(self)

    def __getitem__(self, item):
        return self.map_partitions(itemgetter(item), self._meta[item],
                                   name='__getitem__')

    def __dask_graph__(self):
        return self.dask

    __dask_scheduler__ = staticmethod(dask.threaded.get)

    @staticmethod
    def __dask_optimize__(dsk, keys, **kwargs):
        # We cull unnecessary tasks here. Note that this isn't necessary,
        # dask will do this automatically, this just shows one optimization
        # you could do.
        dsk2 = optimize(dsk, keys)
        return dsk2


    def __dask_postpersist__(self):
        def rebuild(dsk, *extra_args):
            return SparseFrame(dsk, name=self._name,
                               meta=self._meta,
                               divisions=self.divisions)
        return rebuild, ()

    def __dask_postcompute__(self):
        return finalize, ()

    @property
    def npartitions(self):
        return len(self.divisions) - 1

    @property
    def _meta_nonempty(self):
        return meta_nonempty_sparsity(self._meta)

    @property
    def columns(self):
        return self._meta.columns

    @property
    def known_divisions(self):
        """Whether divisions are already known"""
        return len(self.divisions) > 0 and self.divisions[0] is not None

    @property
    def index(self):
        """Return dask Index instance"""
        name = self._name + '-index'
        dsk = {(name, i): (getattr, key, 'index')
               for i, key in enumerate(self.__dask_keys__())}

        return Index(merge(dsk, self.dask), name,
                     self._meta.index, self.divisions)

    def map_partitions(self, func, meta, *args, **kwargs):
        return map_partitions(func, self, meta, *args, **kwargs)

    # noinspection PyTypeChecker
    def todense(self, pandas=True):
        """Convert into Dask DataFrame or Series
        
        Returns
        -------
        res: dd.DataFrame | dd.Series
        """
        if not pandas:
            raise NotImplementedError('Conversion to dask.array is '
                                      'currently not supported!')
        meta = self._meta.todense()

        dfs = [obj.todense(pandas=pandas) for obj in self.to_delayed()]

        return dd.from_delayed(dfs, meta=meta)

    def to_delayed(self):
        return [Delayed(k, self.dask) for k in self.__dask_keys__()]

    @derived_from(sp.SparseFrame)
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

    @derived_from(sp.SparseFrame)
    def add(self, other, how='outer', fill_value=0,):
        return elemwise(sp.SparseFrame.add, self, other, meta=self._meta)

    def __dask_keys__(self):
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

    def repartition(self, divisions=None, npartitions=None, force=False):
        if divisions is not None:
            return repartition(self, divisions, force)
        elif npartitions is not None:
            return repartition_npartitions(self, npartitions)
        raise ValueError('Either divisions or npartitions must be supplied')
    
    def sample(self, n=None, frac=None, replace=False, weights=None,
               random_state=None, axis=None):
        axis = axis or 0
        if axis not in [0, 1]:
            raise ValueError("Axis must be either 0 or 1.")
        if axis == 0 and n is not None:
            raise NotImplementedError("Only `frac` can be used to sample rows"
                                      " from Dask SparseFrame, not `n`.")
        if (n is None) == (frac is None):
            raise ValueError("Please specify either `n` or `frac`.")
        if weights is not None:
            raise NotImplementedError("`weights` argument is not supported.")

        if random_state is None:
            random_state = np.random.RandomState()
        state_data = random_state_data(self.npartitions, random_state)
        state_data = (x for x in state_data)
        
        if axis == 0:
            return self.map_partitions(sp.SparseFrame.sample, self._meta,
                                       frac=frac, replace=replace, axis=0,
                                       random_state=state_data)
        if axis == 1:
            cols = self._meta\
                .sample(n=n, frac=frac, replace=replace, axis=1)\
                .columns.tolist()
            return self[cols]

    def get_partition(self, n):
        """Get a sparse dask DataFrame/Series representing
           the `nth` partition."""
        if 0 <= n < self.npartitions:
            name = 'get-partition-%s-%s' % (str(n), self._name)
            dsk = {(name, 0): (self._name, n)}
            divisions = self.divisions[n:n + 2]
            return SparseFrame(merge(self.dask, dsk), name,
                                 self._meta, divisions)
        else:
            msg = "n must be 0 <= n < {0}".format(self.npartitions)
            raise ValueError(msg)

    def join(self, other, on=None, how='left', lsuffix='',
             rsuffix='', npartitions=None):
        from .multi import join_indexed_sparseframes

        if isinstance(other, sp.SparseFrame):
            meta = sp.SparseFrame.join(self._meta_nonempty,
                                       other,
                                       how=how)
            # make empty meta
            meta = meta.loc[[False] * meta.shape[0], :]
            join_func = partial(sp.SparseFrame.join, other=other,
                                how=how)
            return self.map_partitions(join_func, meta=meta, name='simplejoin')
        if not isinstance(other, (SparseFrame)):
            raise ValueError('other must be SparseFrame')

        return join_indexed_sparseframes(
            self, other, how=how)

    def to_npz(self, filename, blocksize=None,
               storage_options=None, compute=True):
        import sparsity.dask.io_ as dsp_io
        return dsp_io.to_npz(self, filename, blocksize, storage_options, compute)

    def groupby_sum(self, split_out=1, split_every=8):
        meta = self._meta
        if self.known_divisions:
            res = self.map_partitions(sp.SparseFrame.groupby_sum,
                                      meta=meta)
            res.divisions = self.divisions
            if split_out and split_out != self.npartitions:
                res = res.repartition(npartitions=split_out)
            return res
        token = 'groupby_sum'
        return apply_concat_apply(self,
                   chunk=sp.SparseFrame.groupby_sum,
                   aggregate=sp.SparseFrame.groupby_sum,
                   meta=meta, token=token, split_every=split_every,
                   split_out=split_out, split_out_setup=split_out_on_index)

    def sort_index(self,  npartitions=None, divisions=None, **kwargs):
        """Sort the DataFrame index (row labels)
        This realigns the dataset to be sorted by the index.  This can have a
        significant impact on performance, because joins, groupbys, lookups, etc.
        are all much faster on that column.  However, this performance increase
        comes with a cost, sorting a parallel dataset requires expensive shuffles.
        Often we ``sort_index`` once directly after data ingest and filtering and
        then perform many cheap computations off of the sorted dataset.
        This function operates exactly like ``pandas.sort_index`` except with
        different performance costs (it is much more expensive).  Under normal
        operation this function does an initial pass over the index column to
        compute approximate qunatiles to serve as future divisions.  It then passes
        over the data a second time, splitting up each input partition into several
        pieces and sharing those pieces to all of the output partitions now in
        sorted order.
        In some cases we can alleviate those costs, for example if your dataset is
        sorted already then we can avoid making many small pieces or if you know
        good values to split the new index column then we can avoid the initial
        pass over the data.  For example if your new index is a datetime index and
        your data is already sorted by day then this entire operation can be done
        for free.  You can control these options with the following parameters.

        Parameters
        ----------
        npartitions: int, None, or 'auto'
            The ideal number of output partitions.   If None use the same as
            the input.  If 'auto' then decide by memory use.
        divisions: list, optional
            Known values on which to separate index values of the partitions.
            See http://dask.pydata.org/en/latest/dataframe-design.html#partitions
            Defaults to computing this with a single pass over the data. Note
            that if ``sorted=True``, specified divisions are assumed to match
            the existing partitions in the data. If this is untrue, you should
            leave divisions empty and call ``repartition`` after ``set_index``.
        partition_size: int, optional
            if npartitions is set to auto repartition the dataframe into
            partitions of this size.
        """
        from .shuffle import sort_index
        return sort_index(self, npartitions=npartitions,
                          divisions=divisions, **kwargs)

    @derived_from(sp.SparseFrame)
    def set_index(self, column=None, idx=None, level=None):
        if column is None and idx is None and level is None:
            raise ValueError("Either column, idx or level should not be None")
        if idx is not None:
            raise NotImplementedError('Only column or level supported')
        new_name = self._meta.index.names[level] if level else column

        if level is not None:
            new_idx = self._meta.index.get_level_values(level)
        else:
            new_idx = pd.Index(np.empty((0,0), dtype=self._meta.values.dtype))
        new_idx.name = new_name

        meta = self._meta.set_index(idx=new_idx)
        res = self.map_partitions(sp.SparseFrame.set_index, meta=meta,
                                  column=column, idx=idx, level=level)
        res.divisions = tuple([None] * ( self.npartitions + 1))
        return res

    def rename(self, columns):
        _meta = self._meta.rename(columns=columns)
        return self.map_partitions(sp.SparseFrame.rename, meta=_meta,
                                   columns=columns)

    def drop(self, labels, axis=1):
        if axis != 1:
            raise NotImplementedError('Axis != 1 is currently not supported.')
        _meta = self._meta.drop(labels=labels)
        return self.map_partitions(sp.SparseFrame.drop, meta=_meta,
                                   labels=labels)

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


def repartition_divisions(a, b, name, out1, out2, force=False):
    """ dask graph to repartition dataframe by new divisions

    Parameters
    ----------
    a : tuple
        old divisions
    b : tuple, list
        new divisions
    name : str
        name of old dataframe
    out1 : str
        name of temporary splits
    out2 : str
        name of new dataframe
    force : bool, default False
        Allows the expansion of the existing divisions.
        If False then the new divisions lower and upper bounds must be
        the same as the old divisions.

    Examples
    --------
    >>> repartition_divisions([1, 3, 7], [1, 4, 6, 7], 'a', 'b', 'c')  # doctest: +SKIP
    {('b', 0): (<function boundary_slice at ...>, ('a', 0), 1, 3, False),
     ('b', 1): (<function boundary_slice at ...>, ('a', 1), 3, 4, False),
     ('b', 2): (<function boundary_slice at ...>, ('a', 1), 4, 6, False),
     ('b', 3): (<function boundary_slice at ...>, ('a', 1), 6, 7, False)
     ('c', 0): (<function concat at ...>,
                (<type 'list'>, [('b', 0), ('b', 1)])),
     ('c', 1): ('b', 2),
     ('c', 2): ('b', 3)}
    """
    check_divisions(b)

    if len(b) < 2:
        # minimum division is 2 elements, like [0, 0]
        raise ValueError('New division must be longer than 2 elements')

    if force:
        if a[0] < b[0]:
            msg = ('left side of the new division must be equal or smaller '
                   'than old division')
            raise ValueError(msg)
        if a[-1] > b[-1]:
            msg = ('right side of the new division must be equal or larger '
                   'than old division')
            raise ValueError(msg)
    else:
        if a[0] != b[0]:
            msg = 'left side of old and new divisions are different'
            raise ValueError(msg)
        if a[-1] != b[-1]:
            msg = 'right side of old and new divisions are different'
            raise ValueError(msg)

    def _is_single_last_div(x):
        """Whether last division only contains single label"""
        return len(x) >= 2 and x[-1] == x[-2]

    c = [a[0]]
    d = dict()
    low = a[0]

    i, j = 1, 1     # indices for old/new divisions
    k = 0           # index for temp divisions

    last_elem = _is_single_last_div(a)

    # process through old division
    # left part of new division can be processed in this loop
    while (i < len(a) and j < len(b)):
        if a[i] < b[j]:
            # tuple is something like:
            # (methods.boundary_slice, ('from_pandas-#', 0), 3, 4, False))
            d[(out1, k)] = (methods.boundary_slice, (name, i - 1), low, a[i], False)
            low = a[i]
            i += 1
        elif a[i] > b[j]:
            d[(out1, k)] = (methods.boundary_slice, (name, i - 1), low, b[j], False)
            low = b[j]
            j += 1
        else:
            d[(out1, k)] = (methods.boundary_slice, (name, i - 1), low, b[j], False)
            low = b[j]
            i += 1
            j += 1
        c.append(low)
        k += 1

    # right part of new division can remain
    if a[-1] < b[-1] or b[-1] == b[-2]:
        for _j in range(j, len(b)):
            # always use right-most of old division
            # because it may contain last element
            m = len(a) - 2
            d[(out1, k)] = (methods.boundary_slice, (name, m), low, b[_j], False)
            low = b[_j]
            c.append(low)
            k += 1
    else:
        # even if new division is processed through,
        # right-most element of old division can remain
        if last_elem and i < len(a):
            d[(out1, k)] = (methods.boundary_slice, (name, i - 1), a[i], a[i], False)
            k += 1
        c.append(a[-1])

    # replace last element of tuple with True
    d[(out1, k - 1)] = d[(out1, k - 1)][:-1] + (True,)

    i, j = 0, 1

    last_elem = _is_single_last_div(c)

    while j < len(b):
        tmp = []
        while c[i] < b[j]:
            tmp.append((out1, i))
            i += 1
        if last_elem and c[i] == b[-1] and (b[-1] != b[-2] or j == len(b) - 1) and i < k:
            # append if last split is not included
            tmp.append((out1, i))
            i += 1
        if len(tmp) == 0:
            # dummy slice to return empty DataFrame or Series,
            # which retain original data attributes (columns / name)
            d[(out2, j - 1)] = (methods.boundary_slice, (name, 0), a[0], a[0], False)
        elif len(tmp) == 1:
            d[(out2, j - 1)] = tmp[0]
        else:
            if not tmp:
                raise ValueError('check for duplicate partitions\nold:\n%s\n\n'
                                 'new:\n%s\n\ncombined:\n%s'
                                 % (pformat(a), pformat(b), pformat(c)))
            d[(out2, j - 1)] = (sp.SparseFrame.vstack, tmp)
        j += 1
    return d


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
    keys = [d.__dask_keys__() * n if isinstance(d, Scalar) or _is_broadcastable(d)
            else d.__dask_keys__() for d in dasks]

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


def _maybe_repeat(x):
    if isinstance(x, GeneratorType):
        return x
    return repeat(x)


def _make_mappable(d):
    """Convert each value to a generator, unless it already is one."""
    res = dict(zip(
        d.keys(),
        map(_maybe_repeat, d.values())
    ))
    return res


def map_partitions(func, ddf, meta, name=None, **kwargs):
    """Map a function onto partitions.
    
    If you want to pass different values to different partitions,
    pass a generator in kwargs. Subsequent values will be passed to
    the function while mapping onto subsequent partitions.
    """
    kwargs = _make_mappable(kwargs)
    
    dsk = {}
    name = name or func.__name__
    token = tokenize(func, meta, **kwargs)
    name = '{0}-{1}'.format(name, token)

    for i in range(ddf.npartitions):
        # for each partition take next value
        kw = {k: next(v) for k, v in kwargs.items()}
        value = (ddf._name, i)
        dsk[(name, i)] = (apply_and_enforce, func, value, kw, meta)

    return SparseFrame(merge(dsk, ddf.dask), name, meta, ddf.divisions)


def apply_and_enforce(func, arg, kwargs, meta):
    sf = func(arg, **kwargs)
    if isinstance(sf, sp.SparseFrame):
        if sf.empty:
            assert meta.empty, \
                "Computed empty result but received non-empty meta"
            assert isinstance(meta, sp.SparseFrame), \
                "Computed a SparseFrame but meta is of type {}"\
                .format(type(meta))
            return meta
        columns = meta.columns
        if (len(columns) == len(sf.columns) and
                    type(columns) is type(sf.columns) and
                columns.equals(sf.columns)):
            # if target is identical, rename is not necessary
            return sf
        else:
            sf._columns = columns
    return sf


def apply_concat_apply(args, chunk=None, aggregate=None, combine=None,
                       meta=no_default, token=None, chunk_kwargs=None,
                       aggregate_kwargs=None, combine_kwargs=None,
                       split_every=None, split_out=None, split_out_setup=None,
                       split_out_setup_kwargs=None, **kwargs):
    """Apply a function to blocks, then concat, then apply again

    Parameters
    ----------
    args :
        Positional arguments for the `chunk` function. All `dask.dataframe`
        objects should be partitioned and indexed equivalently.
    chunk : function [block-per-arg] -> block
        Function to operate on each block of data
    aggregate : function concatenated-block -> block
        Function to operate on the concatenated result of chunk
    combine : function concatenated-block -> block, optional
        Function to operate on intermediate concatenated results of chunk
        in a tree-reduction. If not provided, defaults to aggregate.
    token : str, optional
        The name to use for the output keys.
    chunk_kwargs : dict, optional
        Keywords for the chunk function only.
    aggregate_kwargs : dict, optional
        Keywords for the aggregate function only.
    combine_kwargs : dict, optional
        Keywords for the combine function only.
    split_every : int, optional
        Group partitions into groups of this size while performing a
        tree-reduction. If set to False, no tree-reduction will be used,
        and all intermediates will be concatenated and passed to ``aggregate``.
        Default is 8.
    split_out : int, optional
        Number of output partitions. Split occurs after first chunk reduction.
    split_out_setup : callable, optional
        If provided, this function is called on each chunk before performing
        the hash-split. It should return a pandas object, where each row
        (excluding the index) is hashed. If not provided, the chunk is hashed
        as is.
    split_out_setup_kwargs : dict, optional
        Keywords for the `split_out_setup` function only.
    kwargs :
        All remaining keywords will be passed to ``chunk``, ``aggregate``, and
        ``combine``.

    Examples
    --------
    >>> def chunk(a_block, b_block):
    ...     pass

    >>> def agg(df):
    ...     pass

    >>> apply_concat_apply([a, b], chunk=chunk, aggregate=agg)  # doctest: +SKIP
    """
    if chunk_kwargs is None:
        chunk_kwargs = dict()
    if aggregate_kwargs is None:
        aggregate_kwargs = dict()
    chunk_kwargs.update(kwargs)
    aggregate_kwargs.update(kwargs)

    if combine is None:
        if combine_kwargs:
            raise ValueError("`combine_kwargs` provided with no `combine`")
        combine = aggregate
        combine_kwargs = aggregate_kwargs
    else:
        if combine_kwargs is None:
            combine_kwargs = dict()
        combine_kwargs.update(kwargs)

    if not isinstance(args, (tuple, list)):
        args = [args]

    npartitions = set(arg.npartitions for arg in args
                      if isinstance(arg, SparseFrame))
    if len(npartitions) > 1:
        raise ValueError("All arguments must have same number of partitions")
    npartitions = npartitions.pop()

    if split_every is None:
        split_every = 8
    elif split_every is False:
        split_every = npartitions
    elif split_every < 2 or not isinstance(split_every, int):
        raise ValueError("split_every must be an integer >= 2")

    token_key = tokenize(token or (chunk, aggregate), meta, args,
                         chunk_kwargs, aggregate_kwargs, combine_kwargs,
                         split_every, split_out, split_out_setup,
                         split_out_setup_kwargs)

    # Chunk
    a = '{0}-chunk-{1}'.format(token or funcname(chunk), token_key)
    if len(args) == 1 and isinstance(args[0], SparseFrame) and not chunk_kwargs:
        dsk = {(a, 0, i, 0): (chunk, key)
               for i, key in enumerate(args[0].__dask_keys__())}
    else:
        dsk = {(a, 0, i, 0): (apply, chunk,
                              [(x._name, i) if isinstance(x, SparseFrame)
                               else x for x in args], chunk_kwargs)
               for i in range(args[0].npartitions)}

    # Split
    # this splits the blocks (usually) by their index and
    # basically performs a task sort such that the next tree
    # aggregation will result in the desired number of partitions
    # given by the split_out parameter
    if split_out and split_out > 1:
        split_prefix = 'split-%s' % token_key
        shard_prefix = 'shard-%s' % token_key
        for i in range(args[0].npartitions):
            # For now we assume that split_out_setup selects the index
            # as we will only support index groupbys for now. So we can
            # use the function provided by dask.
            dsk[(split_prefix, i)] = (hash_shard, (a, 0, i, 0), split_out,
                                      split_out_setup, split_out_setup_kwargs)
            # At this point we have dictionaries of dataframes. The dictionary keys
            # correspond to the hashed index value. Such that rows with the same index
            # have the same dictionary key.
            # The next line unpacks this dictionaries into pure dataframes again
            # now with the correct dask key for their partition. So at this point
            # we might have shards of a single row in the next step they are combined again.
            for j in range(split_out):
                dsk[(shard_prefix, 0, i, j)] = (getitem, (split_prefix, i), j)
        a = shard_prefix
    else:
        split_out = 1

    # Combine
    b = '{0}-combine-{1}'.format(token or funcname(combine), token_key)
    k = npartitions
    depth = 0
    while k > split_every:
        for part_i, inds in enumerate(partition_all(split_every, range(k))):
            for j in range(split_out):
                conc = (sp.SparseFrame.vstack, [(a, depth, i, j) for i in inds])
                # Finally we apply the combine function on the concatenated
                # results. This is usually the same as the aggregate
                # function.
                if combine_kwargs:
                    dsk[(b, depth + 1, part_i, j)] = (apply, combine, [conc], combine_kwargs)
                else:
                    dsk[(b, depth + 1, part_i, j)] = (combine, conc)
        k = part_i + 1
        a = b
        depth += 1

    # Aggregate
    for j in range(split_out):
        b = '{0}-agg-{1}'.format(token or funcname(aggregate), token_key)
        conc = (sp.SparseFrame.vstack, [(a, depth, i, j) for i in range(k)])
        if aggregate_kwargs:
            dsk[(b, j)] = (apply, aggregate, [conc], aggregate_kwargs)
        else:
            dsk[(b, j)] = (aggregate, conc)

    if meta is no_default:
        meta_chunk = _emulate(chunk, *args, **chunk_kwargs)
        meta = _emulate(aggregate, sp.SparseFrame.vstack([meta_chunk]),
                        **aggregate_kwargs)

    for arg in args:
        if isinstance(arg, SparseFrame):
            dsk.update(arg.dask)

    divisions = [None] * (split_out + 1)

    return SparseFrame(dsk, b, meta, divisions)


@get_parallel_type.register(SparseFrame)
def get_parallel_type_distributed(o):
    return get_parallel_type(o._meta)


normalize_token.register((SparseFrame,), lambda a: a._name)
