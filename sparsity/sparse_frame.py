# coding=utf-8
import traceback
import uuid
import warnings
from collections import OrderedDict
from functools import partial, reduce

import numpy as np
import pandas as pd
from pandas.api import types
from pandas.core.common import _default_index

try:
    from pandas.indexes.base import _ensure_index
except ImportError:
    from pandas.core.indexes.base import _ensure_index
from sparsity.io import to_npz, read_npz, _just_read_array
from scipy import sparse

try:
    from sparsity.io import traildb_to_coo
    trail_db = True
except:
    trail_db = False
from sparsity.indexing import _CsrILocationIndexer, _CsrLocIndexer


def _is_empty(data):
    try:
        if any(map(lambda x: x== 0, data.shape)):
            return True
        else:
            return False
    except:
        pass

    if len(data) == 0:
        return True
    elif isinstance(data, list) and sum(map(len, list)) == 0:
        return True
    return False

class SparseFrame(object):
    """
    Simple sparse table based on scipy.sparse.csr_matrix
    """

    __slots__ = ["_index", "_columns", "_data", "shape",
                 'ndim', 'iloc', 'loc', 'empty']

    def __init__(self, data, index=None, columns=None, **kwargs):
        if len(data.shape) > 2:
            raise ValueError("Only two dimensional data supported")

        if len(data.shape) == 1 and isinstance(data, pd.Series):
            data = data.to_frame()

        elif len(data.shape) == 1:
            data = data.reshape(-1,1)

        self.empty = False
        N, K = data.shape

        if index is None:
            self._index = _default_index(N)
        else:
            # assert len(index) == N
            self._index = _ensure_index(index)

        if columns is None:
            self._columns = _default_index(K)
        else:
            # assert len(columns) == K
            self._columns = _ensure_index(columns)

        if not sparse.isspmatrix_csr(data):
            try:
                self._init_values(data, kwargs)
            except TypeError:
                raise TypeError(traceback.format_exc() +
                                "\nThe error described above occurred while "
                                "converting data to sparse matrix.")
        else:
            self.empty = True if _is_empty(data) else False
            self._init_csr(data)

        # register indexers
        self.ndim = 2
        self.iloc = _CsrILocationIndexer(self, 'iloc')
        self.loc = _CsrLocIndexer(self, 'loc')

    def _init_values(self, data, kwargs):
        if isinstance(data, pd.DataFrame):
            self.empty = data.empty
            self._init_csr(sparse.csr_matrix(data.values))
            self._index = _ensure_index(data.index)
            self._columns = _ensure_index(data.columns)
        elif _is_empty(data):
            self.empty = True
            self._data = sparse.csr_matrix((len(self.index),
                                            len(self.columns)))
            self.shape = self._data.shape
        else:
            sparse_data = sparse.csr_matrix(data, **kwargs)
            self._init_csr(sparse_data)

    def toarray(self):
        return self.todense(pandas=False)

    def todense(self, pandas=True):
        if not self.empty:
            dense = np.asarray(self.data.toarray())
        else:
            dense = np.empty(shape=(0, len(self.columns)))

        if self.shape[0] == 1 or self.shape[1] == 1:
            dense = dense.reshape(-1)

        if pandas:
            if self.empty:
                dense = pd.DataFrame([], columns=self.columns,
                                     index=self._index[:0])
            elif len(dense.shape) == 1 and \
                    self.data.shape[1] == 1:
                dense = pd.Series(dense, index=self.index,
                                  name=self.columns[0])
            elif len(dense.shape) == 1 and \
                            self.data.shape[1] > 1:
                dense = pd.DataFrame(dense.reshape(1, -1), index=self.index,
                                     columns=self.columns)
            else:
                idx = np.broadcast_to(self.index, dense.shape[0])
                dense = pd.DataFrame(dense, index=idx,
                                     columns=self.columns)
        return dense

    def _init_csr(self, csr):
        """Keep a zero row at the end of the csr matrix for aligns."""
        self.shape = csr.shape
        if not self.empty:
            self._data = sparse.vstack(
                [csr,
                 sparse.coo_matrix((1,csr.shape[1])).tocsr()
                 ])
        else:
            self._data = csr

    def _get_axis(self, axis):
        """Rudimentary indexing support."""
        if axis == 0:
            return self._index
        if axis == 1:
            return self._columns

    def sum(self, *args, **kwargs):
        return self.data.sum(*args, **kwargs)

    def mean(self, *args, **kwargs):
        return self.data.mean(*args, **kwargs)

    def max(self, *args, **kwargs):
        return self.data.max(*args, **kwargs)

    def min(self, *args, **kwargs):
        return self.data.min(*args, **kwargs)

    def copy(self, *args, **kwargs):
        return SparseFrame(self.data.copy(*args, **kwargs),
                           self.index.copy(*args, **kwargs),
                           self.columns.copy(*args, **kwargs))

    def multiply(self, other, axis='columns'):
        """
        To multiply row-wise 'other' should be of shape: (self.shape[0], 1)
        To multiply col-wise 'other should be of shape: (1, self.shape[1])
        """
        try:
            other = other.toarray()
        except AttributeError:
            pass

        if axis in [0, 'index']:
            other = np.asarray(other).reshape(1, -1)
        else:
            other = np.asarray(other).reshape(-1, 1)

        data = self.data.multiply(other)
        assert data.shape == self.data.shape, \
            "Data shapes miss-match: {}, {}".format(data.shape,self.data.shape)
        return SparseFrame(data, self.index, self.columns)

    def nnz(self):
        return self.data.nnz

    def take(self, idx, axis=0, **kwargs):
        """Return data at integer locations."""
        if axis == 0:
            return SparseFrame(self.data[idx,:],
                               index=self.index[idx],
                               columns=self.columns)
        elif axis == 1:
            return SparseFrame(self.data[:,idx],
                               index=self.index,
                               columns=self.columns[idx])

    def _xs(self, key, *args, **kwargs):
        """Used for label based indexing."""
        loc = self.index.get_loc(key)
        return SparseFrame(self.data[loc], index=[key], columns=self.columns)

    @property
    def index(self):
        return self._index

    @property
    def columns(self):
        return self._columns

    @property
    def data(self):
        if self.empty:
            return self._data
        return self._data[:-1,:]

    # backwards compatibility
    def groupby(self, by=None, level=0):
        return self.groupby_sum(by, level)

    def groupby_agg(self, by=None, level=None, agg_func=None):
        by = self._get_groupby_col(by, level)
        groups = pd.Index(np.arange(self.shape[0])).groupby(by)
        res = sparse.csr_matrix((len(groups), self.shape[1]))
        new_idx = []
        for i, (name, indizes) in enumerate(groups.items()):
            new_idx.append(self.index.values[indizes[0]])
            res[i] = agg_func(self.data[indizes.values,:])
        return SparseFrame(res, index=new_idx)

    def groupby_sum(self, by=None, level=0):
        """
        Optimized sparse groupby sum aggregation.

        Simple operation using sparse matrix multiplication.
        Expects result to be sparse aswell.

        Parameters
        ----------
        by: np.ndarray
            (optional) alternative index.
        level: int
            Level of (multi-)index to group on.

        Returns
        -------
        df: sparsity.SparseFrame
            Grouped by and summed SparseFrame.
        """
        by = self._get_groupby_col(by, level)
        group_idx = by.argsort()
        gm = _create_group_matrix(by[group_idx])
        grouped_data = self._data[group_idx, :].T.dot(gm).T
        return SparseFrame(grouped_data, index=np.unique(by), columns=self._columns)


    def _get_groupby_col(self, by, level):
        if by is None and level is None:
            raise ValueError("You have to supply one of 'by' and 'level'")
        if by is not None:
            try:
                if by in self._columns:
                    by = self[by].toarray()
            except TypeError:
                assert len(by) == self.data.shape[0]
                by = np.array(by)
        else:
            if level and isinstance(self._index, pd.MultiIndex):
                by = self.index.get_level_values(level).values
            elif level == 0:
                by = np.asarray(self._index)
            elif level > 0:
                raise ValueError(
                    "Connot use level > 0 in a non MultiIndex Frame")
            else:
                by = self.index.values
        return by

    def join(self, other, axis=1, how='outer', level=None):
        """
        Join two tables along their indices

        Parameters
        ----------
        other: sparsity.SparseTable
            another SparseFrame
        axis: int
            along which axis to join
        how: str
            one of 'inner', 'outer', 'left', 'right'
        level: int
            if Multiindex join using this level

        Returns
        -------
            joined: sparsity.SparseFrame
        """
        if isinstance(self._index, pd.MultiIndex)\
            or isinstance(other._index, pd.MultiIndex):
            raise NotImplementedError('MultiIndex not supported.')
        if not isinstance(other, SparseFrame):
            other = SparseFrame(other)
        if axis not in set([0, 1]):
            raise ValueError("axis mut be either 0 or 1")
        if axis == 0:
            if np.all(other._columns.values == self._columns.values):
                # take short path if join axes are identical
                data = sparse.vstack([self.data, other.data])
                index = np.hstack([self.index, other.index])
                res = SparseFrame(data, index=index, columns=self._columns)
            else:
                raise NotImplementedError(
                    "Joining along axis 0 fails when column names differ."
                    "This is probably caused by adding all-zeros row.")
                data, new_index = _matrix_join(self._data.T.tocsr(), other._data.T.tocsr(),
                                               self._columns, other._columns,
                                               how=how)
                res = SparseFrame(data.T.tocsr(),
                                  index=np.concatenate([self.index, other.index]),
                                  columns=new_index)
        elif axis == 1:
            if np.all(self.index.values == other.index.values):
                # take short path if join axes are identical
                data = sparse.hstack([self.data, other.data])
                columns = np.hstack([self._columns, other._columns])
                res = SparseFrame(data, index=self.index, columns=columns)
            else:
                data, new_index = _matrix_join(self._data, other._data,
                                              self.index, other.index,
                                              how=how)
                res = SparseFrame(data,
                                  index=new_index,
                                  columns=np.concatenate([self._columns, other._columns]))
        return res

    def rename(self, columns, inplace=False):
        """
        Rename columns by applying a callable to every column name.
        """
        new_cols = self.columns.map(columns)
        if not inplace:
            return SparseFrame(self.data,
                               index=self.index,
                               columns=new_cols)
        else:
            self._columns = new_cols

    @property
    def values(self):
        return self.data

    def sort_index(self):
        """
        Sort table along index

        Returns
        -------
            sorted: sparsity.SparseFrame
        """
        passive_sort_idx = np.argsort(self._index)
        data = self._data[passive_sort_idx]
        index = self._index[passive_sort_idx]
        return SparseFrame(data, index=index)

    def fillna(self, value):
        """Replace NaN values in explicitly stored data with `value`."""
        _data = self._data.copy()
        _data.data[np.isnan(self._data.data)] = value
        return SparseFrame(data=_data[:-1, :],
                           index=self.index, columns=self.columns)

    def add(self, other, how='outer', **kwargs):
        """
        Aligned addition. Adds two tables by aligning them first.

        Parameters
        ----------
            other: sparsity.SparseFrame

        Returns
        -------
            added: sparsity.SparseFrame
        """
        assert np.all(self._columns == other.columns)
        data, new_idx = _aligned_csr_elop(self._data, other._data,
                                          self.index, other.index,
                                          how=how)
        # new_idx = self._index.join(other.index, how=how)
        res = SparseFrame(data, index=new_idx, columns=self._columns)
        return res

    def __sizeof__(self):
        return super().__sizeof__() + \
               self._index.memory_usage(deep=True) + \
               self._columns.memory_usage(deep=True) + \
               self._data.data.nbytes + \
               self._data.indptr.nbytes + self._data.indices.nbytes

    def _align_axis(self):
        raise NotImplementedError()

    def __repr__(self):
        nrows = min(5, self.shape[0])

        if len(self._columns) > 50:
            cols = self.columns[:25].append(self.columns[-25:])
            data = sparse.hstack([self.data[:nrows, :25],
                                  self.data[:nrows, -25:]])
            data = data.toarray()
        else:
            cols = self._columns
            data = self.data[:nrows,:].toarray()

        df = pd.DataFrame(data,
            columns=cols,
            index=self._index[:nrows]
        )
        df_str = df.__repr__().splitlines()[:-2]
        sparse_str = "[{nrows}x{ncols} SparseFrame of type '<class " \
                     "'{dtype}'>' \n with {nnz} stored elements " \
                     "in Compressed Sparse Row format]".format(
            nrows=self.shape[0],
            ncols=self.shape[1],
            dtype=self.data.dtype,
            nnz=self.data.nnz
        )
        repr = "{data}\n{sparse}"\
            .format(data='\n'.join(df_str),
                    sparse=sparse_str)
        return repr

    def __array__(self):
        return self.toarray()

    def head(self, n=1):
        """Display head of the sparsed frame."""
        n = min(n, len(self._index))
        return pd.SparseDataFrame(self.data[:n,:].todense(),
                                  index=self.index[:n],
                                  columns=self.columns)

    def _slice(self, sliceobj):
        return SparseFrame(self.data[sliceobj,:],
                           index=self.index[sliceobj],
                           columns=self.columns)

    @classmethod
    def concat(cls, tables, axis=0):
        """Concat a collection of SparseFrames along given axis."""
        func = partial(SparseFrame.join, axis=axis)
        return reduce(func, tables)

    def _ixs(self, key, axis=0):
        if axis != 0:
            raise NotImplementedError()
        new_idx = self.index[key]
        if not isinstance(new_idx, pd.Index):
            new_idx = [new_idx]
        return SparseFrame(self._data[key,:],
                           index=new_idx,
                           columns=self.columns)

    @classmethod
    def read_traildb(cls, file, field, ts_unit='s'):
        if not trail_db:
            raise ImportError("Traildb could not be imported")
        uuids, timestamps, cols, coo = traildb_to_coo(file, field)
        uuids = np.asarray([uuid.UUID(bytes=x.tobytes()) for x in
                            uuids])
        index = pd.MultiIndex.from_arrays \
            ([pd.CategoricalIndex(uuids),
              pd.to_datetime(timestamps, unit=ts_unit,)],
             names=('uuid', 'timestamp'))
        return cls(coo.tocsr(), index=index, columns=cols)

    def assign(self, **kwargs):
        sf = self
        for key, value in kwargs.items():
            sf = sf._single_assign(key, value)
        return sf

    def __setitem__(self, key, value):
        if key in self.columns:
            raise NotImplementedError("Assigning to an existing column "
                                      "is currently not implemented. You can "
                                      "only assign values to new columns.")
        new_cols, new_data = self._add_col(key, value)
        self._init_csr(new_data)
        self._columns = new_cols

    def _add_col(self, key, value):
        csc = self.data.tocsc()
        value = np.broadcast_to(np.atleast_1d(value), (self.shape[0],))
        val = value.reshape(-1, 1)
        new_data = sparse.hstack([csc, sparse.csc_matrix(val)]).tocsr()
        new_cols = self._columns.append(pd.Index([key]))
        return new_cols, new_data

    def _single_assign(self, key, value):
        if key in self.columns:
            raise NotImplementedError("Assigning to an existing column "
                                      "is currently not implemented. You can "
                                      "only assign values to new columns.")
        new_cols, new_data = self._add_col(key, value)
        return SparseFrame(new_data, index=self.index, columns=new_cols)

    def drop(self, labels, axis=0):
        """Drop label(s) from given axis. Currently works only for columns.
        """
        if not isinstance(labels, (list, tuple, set)):
            labels = [labels]
        if axis == 1:
            mask = np.logical_not(self.columns.isin(labels))
            sf = self[self.columns[mask].tolist()]
        else:
            raise NotImplementedError
        return sf

    def drop_duplicate_idx(self, **kwargs):
        """Drop rows with duplicated index."""
        mask = ~self.index.duplicated(**kwargs)
        return SparseFrame(self.data[mask], index=self.index.values[mask],
                           columns=self.columns)

    def __getitem__(self, item):
        if not isinstance(item, (tuple, list)):
            item = [item]
        idx = []
        for key in item:
            idx.append(self.columns.get_loc(key))
        return SparseFrame(self.data[:,idx], index=self.index,
                           columns=[item])

    def dropna(self):
        """Drop nans from index."""
        mask = np.isnan(self.index.values)
        new_data = self.data[~mask, :]
        new_index = self.index.values[~mask]
        return SparseFrame(new_data, index=new_index, columns=self.columns)

    def set_index(self, column=None, idx=None, level=None, inplace=False):
        """Set index from array, column or existing multi-index level."""
        if column is None and idx is None and level is None:
            raise ValueError("Either column, idx or level should not be None")
        elif idx is not None:
            assert len(idx) == self.data.shape[0]
            new_idx = idx
        elif level is not None and \
                isinstance(self._index, pd.MultiIndex):
            new_idx = self.index.get_level_values(level)
        elif column is not None:
            new_idx = np.asarray(self[column].data.todense()).reshape(-1)

        if inplace:
            self._index = _ensure_index(new_idx)
        else:
            return SparseFrame(self.data,
                               index=new_idx,
                               columns=self.columns)

    @classmethod
    def vstack(cls, frames):
        """Vertical stacking given collection of SparseFrames."""
        assert np.all([np.all(frames[0].columns == frame.columns)
                       for frame in frames[1:]]), "Columns don't match"
        data = list(map(lambda x: x.data, frames))
        new_idx = frames[0].index
        for f in frames[1:]:
            new_idx = new_idx.append(f.index)
        return SparseFrame(sparse.vstack(data),
                           index=new_idx,
                           columns=frames[0].columns)

    @classmethod
    def read_npz(cls, filename):
        """"Read from numpy npz format."""
        return cls(*read_npz(filename))

    def to_npz(self, filename):
        """Save to numpy npz format."""
        to_npz(self, filename)


def _axis_is_empty(csr, axis=0):
    return csr.shape[axis] == 0


def _aligned_csr_elop(a, b, a_idx, b_idx, op='_plus_', how='outer'):
    """Assume data == 0 at loc[-1]"""

    # handle emtpy cases
    if _axis_is_empty(a):
        return b[:-1,:], b_idx

    if _axis_is_empty(b):
        return a[:-1,:], a_idx

    join_idx, lidx, ridx = a_idx.join(b_idx, return_indexers=True, how=how)

    if lidx is None:
        a_new = a[:-1,:]
    else:
        a_new = sparse.csr_matrix(a[lidx])
    if ridx is None:
        b_new = b[:-1,:]
    else:
        b_new = sparse.csr_matrix(b[ridx])

    assert b_new.shape == a_new.shape
    added = a_new._binopt(b_new, op=op)
    return added, join_idx


def _matrix_join(a, b, a_idx, b_idx, how='outer'):
    """Assume data == 0 at loc[-1]"""
    join_idx, lidx, ridx = a_idx.join(b_idx, return_indexers=True,
                                      how=how)
    if lidx is None:
        a_new = a[:-1,:]
    else:
        a_new = sparse.csr_matrix(a[lidx])
    if ridx is None:
        b_new = b[:-1,:]
    else:
        b_new = sparse.csr_matrix(b[ridx])

    data = sparse.hstack([a_new, b_new])

    return data, join_idx


def _create_group_matrix(group_idx, dtype='f8'):
    """Create a matrix based on groupby index labels."""
    if not isinstance(group_idx, pd.Categorical):
        group_idx = pd.Categorical(group_idx, np.unique(group_idx))
    col_idx = group_idx.codes
    row_idx = np.arange(len(col_idx))
    data = np.ones(len(row_idx))
    return sparse.coo_matrix((data, (row_idx, col_idx)),
                             shape=(len(group_idx), len(group_idx.categories)),
                             dtype=dtype).tocsr()


def sparse_one_hot(df, column=None, categories=None, dtype='f8',
                   index_col=None, order=None, prefixes=False):
    """
    One-hot encode specified columns of a pandas.DataFrame.
    Returns a SparseFrame.

    See the documentation of :func:`sparsity.dask.reshape.one_hot_encode`.
    """
    if column is not None:
        warnings.warn(
            '`column` argument of sparsity.sparse_frame.sparse_one_hot '
            'function is deprecated.'
        )
        if order is not None:
            raise ValueError('`order` and `column` arguments cannot be used '
                             'together.')
        categories = {column: categories}

    if order is not None:
        categories = OrderedDict([(column, categories[column])
                                  for column in order])

    new_cols = []
    csrs = []
    for column, column_cat in categories.items():
        if isinstance(column_cat, str):
            column_cat = _just_read_array(column_cat)
        cols, csr = _one_hot_series_csr(column_cat, dtype, df[column])
        if prefixes:
            cols = list(map(lambda x: '{}_{}'.format(column, x), cols))
        new_cols.extend(cols)
        csrs.append(csr)
    if len(set(new_cols)) < len(new_cols):
        raise ValueError('Different columns have same categories. This would '
                         'result in duplicated column names. '
                         'Set `prefix` to True to manage this situation.')
    new_data = sparse.hstack(csrs, format='csr')

    if not isinstance(index_col, list):
        new_index = df[index_col] if index_col else df.index
    else:
        df = df.reset_index()
        new_index = pd.MultiIndex.from_arrays(df[index_col].values.T)
    return SparseFrame(new_data, index=new_index, columns=new_cols)


def _one_hot_series_csr(categories, dtype, oh_col):
    if types.is_categorical_dtype(oh_col):
        cat = oh_col
    else:
        s = oh_col
        cat = pd.Categorical(s, np.asarray(categories))
    codes = cat.codes
    n_features = len(cat.categories)
    n_samples = codes.size
    mask = codes != -1
    if np.any(~mask):
        raise ValueError("unknown categorical features present %s "
                         "during transform." % np.unique(s[~mask]))
    row_indices = np.arange(n_samples, dtype=np.int32)
    col_indices = codes
    data = np.ones(row_indices.size)
    data = sparse.coo_matrix((data, (row_indices, col_indices)),
                             shape=(n_samples, n_features),
                             dtype=dtype).tocsr()
    return cat.categories.values, data
