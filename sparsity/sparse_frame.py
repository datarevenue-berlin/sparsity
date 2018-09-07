# coding=utf-8
import functools
import traceback
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

from sparsity.indexing import get_indexers_list


def _is_empty(data):
    if any(map(lambda x: x == 0, data.shape)):
        return True
    return False


def _append_zero_row(csr):
    return sparse.vstack(
        [csr,
         sparse.coo_matrix((1, csr.shape[1])).tocsr()]
    )


class SparseFrame(object):
    """
    Simple sparse table implementation. It adds pandas indexing abilities to a
    compressed row sparse frame  based on scipy.sparse.csr_matrix. This makes
    indexing along the first axis extremely efficient and cheap. Indexing along
    the second axis should be avoided if possible though.

    For a distributed implementation see sparsity.dask.SparseFrame.
    """

    def __init__(self, data, index=None, columns=None, **kwargs):
        """Init SparseFrame

        Parameters
        ----------
        data: np.array | pd.DataFrame
            A numerical numpy array or a pandas DataFrame of homogeneous
            data type.
        index: array-like
            labels for first axis.
        columns: array-like
            labels for second axis.
        kwargs:
            are passed to scipy.sparse.csr_matrix
        """
        if len(data.shape) > 2:
            raise ValueError("Only two dimensional data supported")

        if len(data.shape) == 1 and isinstance(data, pd.Series):
            data = data.to_frame()

        elif len(data.shape) == 1:
            data = data.reshape(-1, 1)

        self.empty = False
        N, K = data.shape

        if index is None:
            self._index = _default_index(N)
        elif len(index) != N and data.size:
            if columns is not None:
                implied_axis_1 = len(columns)
            else:
                implied_axis_1 = data.shape[1]
            raise ValueError('Shape of passed values is {},'
                             'indices imply {}'
                             .format(data.shape, (len(index), implied_axis_1)))
        else:
            self._index = _ensure_index(index)

        if columns is None:
            self._columns = _default_index(K)
        elif len(columns) != K and data.size:
            if index is not None:
                implied_axis_0 = len(index)
            else:
                implied_axis_0 = data.shape[0]
            raise ValueError('Shape of passed values is {},'
                             'indices imply {}'
                             .format(data.shape, (implied_axis_0, len(columns))))
        else:
            self._columns = _ensure_index(columns)

        if not sparse.isspmatrix_csr(data):
            try:
                self._init_values(data,
                                  init_index=index is None,
                                  init_columns=columns is None,
                                  **kwargs)
            except TypeError:
                raise TypeError(traceback.format_exc() +
                                "\nThe error described above occurred while "
                                "converting data to sparse matrix.")
        else:
            self.empty = True if _is_empty(data) else False
            self._init_csr(data)

        self.ndim = 2

    @classmethod
    def _create_indexer(cls, name, indexer):
        """Create an indexer like _name in the class."""
        if getattr(cls, name, None) is None:
            _v = tuple(map(int, pd.__version__.split('.')))
            if _v >= (0, 23, 0):
                _indexer = functools.partial(indexer, name)
            else:
                _indexer = functools.partial(indexer, name=name)
            setattr(cls, name, property(_indexer, doc=indexer.__doc__))

    def _init_values(self, data, init_index=True, init_columns=True, **kwargs):
        if isinstance(data, pd.DataFrame):
            self.empty = data.empty
            self._init_csr(sparse.csr_matrix(data.values))
            if init_index:
                self._index = _ensure_index(data.index)
            else:
                warnings.warn("Passed index explicitly while initializing "
                              "from pd.DataFrame. Original DataFrame's index "
                              "will be ignored.", SyntaxWarning)
            if init_columns:
                self._columns = _ensure_index(data.columns)
            else:
                warnings.warn("Passed columns explicitly while initializing "
                              "from pd.DataFrame. Original DataFrame's columns"
                              " will be ignored.", SyntaxWarning)
        elif _is_empty(data):
            self.empty = True
            self._data = sparse.csr_matrix((len(self.index),
                                            len(self.columns)))
            self.shape = self._data.shape
        else:
            sparse_data = sparse.csr_matrix(data, **kwargs)
            self._init_csr(sparse_data)

    def toarray(self):
        """Return dense np.array representation."""
        return self.todense(pandas=False)

    def todense(self, pandas=True):
        """Return dense representation.

        Parameters
        ----------
        pandas: bool
            If true returns a pandas DataFrame (default),
            else a numpy array is returned.

        Returns
        -------
        dense: pd.DataFrame | np.ndarray
            dense representation
        """
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
                # need to copy as broadcast_to return read_only array
                idx = np.broadcast_to(self.index, dense.shape[0])\
                     .copy()
                dense = pd.DataFrame(dense, index=idx,
                                     columns=self.columns)
        return dense

    def _init_csr(self, csr):
        """Keep a zero row at the end of the csr matrix for aligns."""
        self.shape = csr.shape
        if not self.empty:
            self._data = _append_zero_row(csr)
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

    def copy(self, *args, deep=True, **kwargs):
        """Copy frame

        Parameters
        ----------
        args:
            are passed to indizes and values copy methods
        deep: bool
            if true (default) data will be copied as well.
        kwargs:
            are passed to indizes and values copy methods

        Returns
        -------
        copy: SparseFrame
        """
        if deep:
            return SparseFrame(self.data.copy(*args, **kwargs),
                               self.index.copy(*args, **kwargs),
                               self.columns.copy(*args, **kwargs))
        else:
            return SparseFrame(self.data,
                               self.index.copy(*args, **kwargs),
                               self.columns.copy(*args, **kwargs))

    def multiply(self, other, axis='columns'):
        """
        Multiply SparseFrame row-wise or column-wise.

        Parameters
        ----------
        other: array-like
            Vector of numbers to multiply columns/rows by.
        axis: int | str
            - 1 or 'columns' to multiply column-wise (default)
            - 0 or 'index' to multiply row-wise
        """
        try:
            other = other.toarray()
        except AttributeError:
            pass

        if axis in [0, 'index']:
            other = np.asarray(other).reshape(-1, 1)
        elif axis in [1, 'columns']:
            other = np.asarray(other).reshape(1, -1)
        else:
            raise ValueError("Axis should be one of 0, 1, 'index', 'columns'.")

        data = self.data.multiply(other)
        assert data.shape == self.data.shape, \
            "Data shapes mismatch: {}, {}".format(data.shape, self.data.shape)
        return SparseFrame(data, self.index, self.columns)

    def nnz(self):
        """Get the count of explicitly stored values (nonzeros)."""
        return self.data.nnz

    def take(self, idx, axis=0, **kwargs):
        """Return data at integer locations.

        Parameters
        ----------
        idx: array-like | int
            array of integer locations
        axis:
            which axis to index
        kwargs:
            not used

        Returns
        -------
        indexed: SparseFrame
            reindexed sparse frame
        """
        if axis == 0:
            return SparseFrame(self.data[idx, :],
                               index=self.index[idx],
                               columns=self.columns)
        elif axis == 1:
            return SparseFrame(self.data[:, idx],
                               index=self.index,
                               columns=self.columns[idx])

    def _take(self, *args, **kwargs):
        """
        This function is to mimic pandas api (0.21.0)
        and support indexing.

        See https://github.com/pandas-dev/pandas/commit/458c1dc81b7e6f90180b06179ac91d9ed868cb05
        """
        return self.take(*args, **kwargs)

    def _xs(self, key, *args, axis=0, **kwargs):
        """Used for label based indexing."""
        if axis == 0:
            loc = self.index.get_loc(key)
            new_data = self.data[loc]
            return SparseFrame(new_data,
                               index=[key] * new_data.shape[0],
                               columns=self.columns)
        else:
            loc = self.columns.get_loc(key)
            new_data = self.data[:, loc]
            return SparseFrame(new_data,
                               columns=[key] * new_data.shape[1],
                               index=self.index)


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
        return self._data[:-1, :]

    def groupby_agg(self, by=None, level=None, agg_func=None):
        """Groupby aggregation (experimental).

        Groups data and applies function on each group.

        Parameters
        ----------
        by: str | array-like
            column name or groupby labels
        level: int
            index level to use for groupby
        agg_func: callable
            Function to execute in individual groups of data. Must accept
            a SparseFrame and needs to return a vector of shape (1, n_cols).
        """
        by, cols = self._get_groupby_col(by, level)
        groups = pd.Index(np.arange(self.shape[0])).groupby(by)
        res = sparse.csr_matrix((len(groups), self.shape[1]))
        new_idx = []
        for i, (name, indices) in enumerate(groups.items()):
            new_idx.append(name)
            res[i] = agg_func(self.data[indices.values, :])
        res = SparseFrame(res, index=new_idx, columns=self.columns)
        return res[cols]

    def groupby_sum(self, by=None, level=0):
        """Optimized sparse groupby sum aggregation.

        Simple operation using sparse matrix multiplication.
        Expects result to be sparse as well.

        Parameters
        ----------
        by: np.ndarray (optional)
            Alternative index.
        level: int
            Level of (multi-)index to group on.

        Returns
        -------
        df: sparsity.SparseFrame
            Grouped by and summed SparseFrame.
        """
        by, cols = self._get_groupby_col(by, level)
        group_idx = by.argsort()
        gm = _create_group_matrix(by[group_idx])
        grouped_data = self._data[group_idx, :].T.dot(gm).T
        res = SparseFrame(grouped_data, index=np.unique(by),
                          columns=self._columns)
        return res[cols]

    def _get_groupby_col(self, by, level):
        if by is None and level is None:
            raise ValueError("You have to supply one of 'by' and 'level'.")
        other_cols = self._columns.tolist()
        if by is not None:
            try:
                if by in self._columns:
                    other_cols.remove(by)
                    by = self[by].toarray()
            except TypeError:
                assert len(by) == self.data.shape[0]
                by = np.array(by)
        else:
            if level and isinstance(self._index, pd.MultiIndex):
                by = self.index.get_level_values(level).values
            elif level > 0:
                raise ValueError(
                    "Cannot use level > 0 in a non-MultiIndex Frame.")
            else:  # level == 0
                by = np.asarray(self._index)
        return by, other_cols

    def join(self, other, axis=1, how='outer', level=None):
        """
        Join two tables along their indices.

        Parameters
        ----------
        other: sparsity.SparseTable
            another SparseFrame
        axis: int
            along which axis to join
        how: str
            one of 'inner', 'outer', 'left', 'right'
        level: int
            if axis is MultiIndex, join using this level
        Returns
        -------
            joined: sparsity.SparseFrame
        """
        if isinstance(self._index, pd.MultiIndex) \
                or isinstance(other._index, pd.MultiIndex):
            raise NotImplementedError('MultiIndex not supported.')
        if not isinstance(other, SparseFrame):
            other = SparseFrame(other)
        if axis not in {0, 1}:
            raise ValueError("Axis mut be either 0 or 1.")
        if axis == 0:
            if np.array_equal(other._columns.values, self._columns.values):
                # take short path if join axes are identical
                data = sparse.vstack([self.data, other.data])
                index = np.hstack([self.index, other.index])
                res = SparseFrame(data, index=index, columns=self._columns)
            else:
                data, new_index = _matrix_join(
                    _append_zero_row(self.data.T.tocsr()),
                    _append_zero_row(other.data.T.tocsr()),
                    self._columns,
                    other._columns,
                    how=how,
                )
                res = SparseFrame(data.T.tocsr(),
                                  index=np.concatenate([self.index, other.index]),
                                  columns=new_index)
        elif axis == 1:
            if np.array_equal(self.index.values, other.index.values):
                # take short path if join axes are identical
                data = sparse.hstack([self.data, other.data])
                columns = np.hstack([self._columns, other._columns])
                res = SparseFrame(data, index=self.index, columns=columns)
            else:
                if other.empty:
                    other_data = sparse.csr_matrix((1, other.shape[1]),
                                                   dtype=other.data.dtype)
                else:
                    other_data = other._data

                if self.empty:
                    self_data = sparse.csr_matrix((1, self.shape[1]),
                                                  dtype=self.data.dtype)
                else:
                    self_data = self._data

                data, new_index = _matrix_join(self_data, other_data,
                                               self.index, other.index,
                                               how=how)
                res = SparseFrame(data,
                                  index=new_index,
                                  columns=np.concatenate([self._columns,
                                                          other._columns]))
        return res

    def __len__(self):
        return self.shape[0]

    def rename(self, columns, inplace=False):
        """
        Rename columns by applying a callable to every column name.

        Parameters
        ----------
        columns: callable
            a callable that will accepts a column element and returns the
            new column label.
        inplace: bool
            if true the operation will be executed inplace

        Returns
        -------
        renamed: SparseFrame | None
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
        Sort table along index.

        Returns
        -------
            sorted: sparsity.SparseFrame
        """
        passive_sort_idx = np.argsort(self._index)
        data = self._data[passive_sort_idx]
        index = self._index[passive_sort_idx]
        return SparseFrame(data, index=index, columns=self.columns)

    def fillna(self, value):
        """Replace NaN values in explicitly stored data.

        Parameters
        ----------
        value:
            value must be of same dtype as the underlying SparseFrame's data.

        Returns
        -------
        filled: SparseFrame
        """
        _data = self._data.copy()
        _data.data[np.isnan(self._data.data)] = value
        return SparseFrame(data=_data[:-1, :],
                           index=self.index, columns=self.columns)

    def add(self, other, how='outer', fill_value=0, **kwargs):
        """
        Aligned addition. Adds two tables by aligning them first.

        Parameters
        ----------
        other: sparsity.SparseFrame
            Another SparseFrame.
        how: str
            How to join frames along their indexes. Default is 'outer' which
            makes the result contain labels from both frames.
        fill_value: float
            Fill value if other frame is not exactly the same shape.
            For sparse data the only sensible fill value is 0. Passing
            any other value will result in a ValueError.

        Returns
        -------
        added: sparsity.SparseFrame
        """
        if fill_value != 0:
            raise ValueError("Only 0 is accepted as fill_value "
                             "for sparse data.")
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
            data = self.data[:nrows, :].toarray()

        df = pd.DataFrame(data, columns=cols, index=self._index[:nrows])
        df_str = df.__repr__().splitlines()
        if df_str[-2] == '':
            df_str = df_str[:-2]

        sparse_str = "[{nrows}x{ncols} SparseFrame of type '<class " \
                     "'{dtype}'>' \n with {nnz} stored elements " \
                     "in Compressed Sparse Row format]".format(
                         nrows=self.shape[0],
                         ncols=self.shape[1],
                         dtype=self.data.dtype,
                         nnz=self.data.nnz
                     )
        repr = "{data}\n{sparse}" \
            .format(data='\n'.join(df_str), sparse=sparse_str)
        return repr

    def __array__(self):
        return self.toarray()

    def head(self, n=1):
        """Return rows from the top of the table.

        Parameters
        ----------
        n: int
            how many rows to return, default is 1

        Returns
        -------
        head: SparseFrame
        """
        n = min(n, len(self._index))
        return pd.SparseDataFrame(self.data[:n, :].todense(),
                                  index=self.index[:n],
                                  columns=self.columns)

    def _slice(self, sliceobj):
        return SparseFrame(self.data[sliceobj, :],
                           index=self.index[sliceobj],
                           columns=self.columns)

    @classmethod
    def concat(cls, tables, axis=0):
        """Concat a collection of SparseFrames along given axis.

        Uses join internally so it might not be very efficient.

        Parameters
        ----------
        tables: list
            a list of SparseFrames.
        axis:
            which axis to concatenate along.

        Returns
        -------

        """
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

    def assign(self, **kwargs):
        """Assign new columns.

        Parameters
        ----------
        kwargs: dict
            Mapping from column name to values. Values must be of correct shape
            to be inserted successfully.

        Returns
        -------
        assigned: SparseFrame
        """
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

    def drop(self, labels, axis=1):
        """Drop label(s) from given axis.

        Currently works only for columns.

        Parameters
        ----------
        labels: array-like
            labels to drop from the columns
        axis: int
            only columns are supported atm.

        Returns
        -------
        df: SparseFrame
        """
        if not isinstance(labels, (list, tuple, set)):
            labels = [labels]
        if axis == 1:
            mask = np.logical_not(self.columns.isin(labels))
            sf = self.loc[:, self.columns[mask].tolist()]
        else:
            raise NotImplementedError
        return sf

    def drop_duplicate_idx(self, **kwargs):
        """Drop rows with duplicated index.

        Parameters
        ----------
        kwargs:
            kwds are passed to pd.Index.duplicated

        Returns
        -------
        dropeed: SparseFrame
        """
        mask = ~self.index.duplicated(**kwargs)
        return SparseFrame(self.data[mask], index=self.index.values[mask],
                           columns=self.columns)

    def __getitem__(self, item):
        if item is None:
            raise ValueError('Cannot label index with a null key.')
        if not isinstance(item, (tuple, list)):
            item = [item]
        if len(item) > 0:
            return self.reindex_axis(item, axis=1)
        else:
            data = np.empty(shape=(self.shape[0], 0))
            return SparseFrame(data, index=self.index,
                               columns=self.columns[[]])

    def dropna(self):
        """Drop nans from index."""
        mask = np.isnan(self.index.values)
        new_data = self.data[~mask, :]
        new_index = self.index.values[~mask]
        return SparseFrame(new_data, index=new_index, columns=self.columns)

    def set_index(self, column=None, idx=None, level=None, inplace=False):
        """Set index from array, column or existing multi-index level.

        Parameters
        ----------
        column: str
            set index from existing column in data.
        idx: pd.Index, np.array
            Set the index directly with a pandas index object or array
        level: int
            set index from a multiindex level. useful for groupbys.
        inplace: bool
            perform data transformation inplace

        Returns
        -------
        sf: sp.SparseFrame | None
            the transformed sparse frame or None if inplace was True
        """
        if column is None and idx is None and level is None:
            raise ValueError("Either column, idx or level should not be None")
        elif idx is not None:
            assert len(idx) == self.data.shape[0]
            new_idx = idx
        elif level is not None and \
                isinstance(self._index, pd.MultiIndex):
            new_idx = self.index.get_level_values(level)
        elif column is not None:
            new_idx = np.asarray(self.loc[:, column].data.todense()).reshape(-1)

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
    def read_npz(cls, filename, storage_options=None):
        """Read from numpy npz format.

        Parameters
        ----------
        filename: str
            path to file
        storage_options: dict
            optional storage options for external filesystems like s3fs

        Returns
        -------
        loaded: SparseFrame
        """
        return cls(*read_npz(filename, storage_options))

    @property
    def axes(self):
        return [self.index, self.columns]

    def _get_axis_name(self, axis):
        try:
            return ['index', 'columns'][axis]
        except IndexError:
            raise ValueError('No axis named {} for {}'
                             .format(axis, self.__class__))

    def _reindex_with_indexers(self, reindexers, **kwargs):
        """allow_dups indicates an internal call here """

        # reindex doing multiple operations on different axes if indicated
        new_data = self.copy()
        for axis in sorted(reindexers.keys()):
            index, indexer = reindexers[axis]

            if index is None:
                continue

            if axis == 0:
                new_mat = new_data.data[indexer, :]
                new_data = SparseFrame(new_mat, index=index,
                                       columns=self.columns)
            elif axis == 1:
                new_mat = new_data.data[:, indexer]
                new_data = SparseFrame(new_mat, columns=index,
                                       index=self.index)
            else:
                raise ValueError('Only supported axes are 0 and 1.')

        return new_data

    def reindex(self, labels=None, index=None, columns=None, axis=None,
                *args, **kwargs):
        """Conform SparseFrame to new index.

        Missing values will be filled with zeroes.

        Parameters
        ----------
        labels: array-like
            New labels / index to conform the axis specified by ‘axis’ to.
        index, columns : array-like, optional
            New labels / index to conform to. Preferably an Index object to
            avoid duplicating data
        axis: int
            Axis to target. Can be either (0, 1).
        args, kwargs
            Will be passed to reindex_axis.

        Returns
        -------
        reindexed: SparseFrame
        """

        if labels is not None and index is None and columns is None:
            if axis is None:
                axis = 0
            return self.reindex_axis(labels, axis=axis, *args, **kwargs)
        elif columns is not None and index is None:
            return self.reindex_axis(columns, axis=1, *args, **kwargs)
        elif columns is None and index is not None:
            return self.reindex_axis(index, axis=0, *args, **kwargs)
        elif columns is not None and index is not None:
            obj = self.reindex_axis(columns, axis=1, *args, **kwargs)
            return obj.reindex_axis(index, axis=0, *args, **kwargs)
        else:
            raise ValueError('Label parameter is mutually exclusive '
                             'with both index or columns')

    def reindex_axis(self, labels, axis=0, method=None,
                     level=None, copy=True, limit=None, fill_value=0):
        """Conform SparseFrame to new index.

        Missing values will be filled with zeros.

        Parameters
        ----------
        labels: array-like
            New labels / index to conform the axis specified by ‘axis’ to.
        axis: int
            Axis to target. Can be either (0, 1).
        method: None
            unsupported
        level: None
            unsupported
        copy: None
            unsupported
        limit: None
            unsupported
        fill_value: None
            unsupported

        Returns
        -------
        reindexed: SparseFrame
        """
        if method is not None \
                or not copy \
                or level is not None \
                or fill_value != 0 \
                or limit is not None:
            raise NotImplementedError(
                'Error only labels, index, columns and/or axis are supported')
        if axis == 0:
            self.index._can_reindex(labels)
            reindex_axis = 'index'
            other_axis = 'columns'
            new_index, idx = self.index.reindex(labels)
            if idx is None:
                return self.copy()
            new_data = self._data[idx]
        elif axis == 1:
            self.columns._can_reindex(labels)
            reindex_axis = 'columns'
            other_axis = 'index'
            new_index, idx = self.columns.reindex(labels)
            if idx is None:
                return self.copy()
            # we have a hidden zero column to replace missing indices (-1)
            new_data = self._data.T[idx].T[:-1]
        else:
            raise ValueError("Only two dimensional data supported.")

        kwargs = {reindex_axis: new_index,
                  other_axis: getattr(self, other_axis)}

        return SparseFrame(new_data, **kwargs)

    def to_npz(self, filename, block_size=None, storage_options=None):
        """Save to numpy npz format.

        Parameters
        ----------
        filename: str
            path to local file ot s3 path starting with `s3://`
        block_size: int
            block size in bytes only has effect if writing to remote storage
            if set to None defaults to 100MB
        storage_options: dict
            additional parameters to pass to FileSystem class;
            only useful when writing to remote storages
        Returns
        -------
            None
        """
        to_npz(self, filename, block_size, storage_options)


def _axis_is_empty(csr, axis=0):
    return csr.shape[axis] == 0


def _aligned_csr_elop(a, b, a_idx, b_idx, op='_plus_', how='outer'):
    """Assume data == 0 at loc[-1]"""

    # handle emtpy cases
    if _axis_is_empty(a):
        return b[:-1, :], b_idx

    if _axis_is_empty(b):
        return a[:-1, :], a_idx

    join_idx, lidx, ridx = a_idx.join(b_idx, return_indexers=True, how=how)

    if lidx is None:
        a_new = a[:-1, :]
    else:
        a_new = sparse.csr_matrix(a[lidx])
    if ridx is None:
        b_new = b[:-1, :]
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
        a_new = a[:-1, :]
    else:
        a_new = sparse.csr_matrix(a[lidx])
    if ridx is None:
        b_new = b[:-1, :]
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
                   index_col=None, order=None, prefixes=False,
                   ignore_cat_order_mismatch=False):
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
        cols, csr = _one_hot_series_csr(
            column_cat, dtype, df[column],
            ignore_cat_order_mismatch=ignore_cat_order_mismatch
        )
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


def _one_hot_series_csr(categories, dtype, oh_col,
                        ignore_cat_order_mismatch=False):
    if types.is_categorical_dtype(oh_col):
        cat = oh_col.cat
        _check_categories_order(cat.categories, categories, oh_col.name,
                                ignore_cat_order_mismatch)

    else:
        s = oh_col
        cat = pd.Categorical(s, np.asarray(categories))
    codes = cat.codes
    n_features = len(cat.categories)
    n_samples = codes.size
    mask = codes != -1
    if np.any(~mask):
        raise ValueError("Unknown categorical features present "
                         "during transform: %s." % np.unique(s[~mask]))
    row_indices = np.arange(n_samples, dtype=np.int32)
    col_indices = codes
    data = np.ones(row_indices.size)
    data = sparse.coo_matrix((data, (row_indices, col_indices)),
                             shape=(n_samples, n_features),
                             dtype=dtype).tocsr()
    return cat.categories.values, data


def _check_categories_order(categories1, categories2, categorical_column_name,
                            ignore_cat_order_mismatch):
    """Check if two lists of categories differ. If they have different
    elements, raise an exception. If they differ only by order of elements,
    raise an exception unless ignore_cat_order_mismatch is set."""

    if categories2 is None or list(categories2) == list(categories1):
        return

    if set(categories2) == set(categories1):
        mismatch_type = 'order'
    else:
        mismatch_type = 'set'

    if mismatch_type == 'set' or not ignore_cat_order_mismatch:
        raise ValueError(
            "Got categorical column {column_name} whose categories "
            "{mismatch_type} doesn't match categories {mismatch_type} "
            "given as argument to this function.".format(
                column_name=categorical_column_name,
                mismatch_type=mismatch_type
            )
        )


for _name, _indexer in get_indexers_list():
    SparseFrame._create_indexer(_name, _indexer)
