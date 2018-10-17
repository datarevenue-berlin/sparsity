import warnings
from collections import OrderedDict

import numpy as np

import sparsity as sp
from sparsity import sparse_one_hot
from sparsity.dask import SparseFrame


def one_hot_encode(ddf, column=None, categories=None, index_col=None,
                   order=None, prefixes=False,
                   ignore_cat_order_mismatch=False):
    """
    Sparse one hot encoding of dask.DataFrame.

    Convert a dask.DataFrame into a series of SparseFrames by one-hot
    encoding specified columns.

    Parameters
    ----------
    ddf: dask.DataFrame
        e.g. the clickstream
    categories: dict
        Maps ``column name`` -> ``iterable of possible category values``.
        Can be also ``column name`` -> ``None`` if this column is already
        of categorical dtype.
        This argument decides which column(s) will be encoded.
        See description of `order` and `ignore_cat_order_mismatch`.
    index_col: str | iterable
        which columns to use as index
    order: iterable
        Specify order in which one-hot encoded columns should be aligned.

        If `order = [col_name1, col_name2]`
        and `categories = {col_name1: ['A', 'B'], col_name2: ['C', 'D']}`,
        then the resulting SparseFrame will have columns
        `['A', 'B', 'C', 'D']`.

        If you don't specify order, then output columns' order depends on
        iteration over `categories` dictionary. You can pass `categories`
        as an OrderedDict instead of providing `order` explicitly.
    prefixes: bool
        If False, column names will be the same as categories,
        so that new columns will be named like:
        [cat11, cat12, cat21, cat22, ...].

        If True, original column name followed by an underscore will be added
        in front of each category name, so that new columns will be named like:
        [col1_cat11, col1_cat12, col2_cat21, col2_cat22, ...].
    column: DEPRECATED
        Kept only for backward compatibility.
    ignore_cat_order_mismatch: bool
        If a column being one-hot encoded is of categorical dtype, it has
        its categories already predefined, so we don't need to explicitly pass
        them in `categories` argument (see this argument's description).
        However, if we pass them, they may be different than ones defined in
        column.cat.categories. In such a situation, a ValueError will be
        raised. However, if only orders of categories are different (but sets
        of elements are same), you may specify ignore_cat_order_mismatch=True
        to suppress this error. In such a situation, column's predefined
        categories will be used.

    Returns
    -------
        sparse_one_hot: sparsity.dask.SparseFrame
    """
    if column is not None:
        warnings.warn(
            '`column` argument of sparsity.dask.reshape.one_hot_encode '
            'function is deprecated.'
        )
        if order is not None:
            raise ValueError('`order` and `column` arguments cannot be used '
                             'together.')
        categories = {column: categories}

    idx_meta = ddf._meta.reset_index().set_index(index_col).index[:0] \
        if index_col else ddf._meta.index

    if order is not None:
        categories = OrderedDict([(column, categories[column])
                                  for column in order])

    columns = sparse_one_hot(ddf._meta,
                             categories=categories,
                             index_col=index_col,
                             prefixes=prefixes,
                             ignore_cat_order_mismatch=ignore_cat_order_mismatch
                             ).columns
    meta = sp.SparseFrame(np.empty(shape=(0, len(columns))), columns=columns,
                          index=idx_meta)

    dsf = ddf.map_partitions(sparse_one_hot,
                             categories=categories,
                             index_col=index_col,
                             prefixes=prefixes,
                             ignore_cat_order_mismatch=ignore_cat_order_mismatch,
                             meta=object)

    return SparseFrame(dsf.dask, dsf._name, meta, dsf.divisions)
