from collections import OrderedDict

import numpy as np

import sparsity as sp
from drtools.utils.log import get_logger
from sparsity import sparse_one_hot
from sparsity.dask import SparseFrame
from sparsity.io import _just_read_array

log = get_logger(__name__)


def _parse_legacy_ohe_interface(categories, index_col, order, column):
    """
    Old interface was: one_hot_encode(ddf, column, categories, index_col).

    This function does not guarantee correctness of the result.
    It should work when all arguments were passed as positional args
    and when all arguments (ddf doesn't matter) were passed as kwargs.
    """
    # Case when column was passed as a positional arg
    # (we assume other args were also passed as positional args):
    if column is None:
        new_categories = {categories: index_col}
        new_index_col = order
    # Case when column was passed as a kwarg:
    else:
        new_categories = {column: categories}
        new_index_col = index_col
    new_order = None
    new_column = None
    return new_categories, new_index_col, new_order, new_column


def one_hot_encode(ddf, categories, index_col, order=None, column=None):
    """
    Sparse one hot encoding of dask.DataFrame.

    Convert a dask.DataFrame into a series of SparseFrames by one-hot
    encoding specified columns.

    Parameters
    ----------
    ddf: dask.DataFrame
        e.g. the clickstream
    categories: dict
        Maps column name -> iterable of possible category values.
        See description of `order`.
    order: iterable
        Specify order in which one-hot encoded columns should be aligned.

        If `order = [col_name1, col_name2]`
        and `categories = {col_name1: ['A', 'B'], col_name2: ['C', 'D']}`,
        then the resulting SparseFrame will have columns
        `['A', 'B', 'C', 'D']`.

        If you don't specify order, then output columns' order depends on
        iteration over `categories` dictionary. You can pass `categories`
        as an OrderedDict instead of providing `order` explicitly.
    index_col: str, iterable
        which columns to use as index
    column: DEPRECATED
        Kept only for backward compatibility.

    Returns
    -------
        sparse_one_hot: sparsity.dask.SparseFrame
    """
    if not isinstance(categories, dict):
        log.warn(
            'Detected usage of deprecated interface of '
            'sparsity.dask.reshape.one_hot_encode function. Trying to '
            'fall back. The result is not guaranteed to be correct!'
        )
        categories, index_col, order, column = _parse_legacy_ohe_interface(
            categories, index_col, order, column)

    idx_meta = ddf._meta.reset_index().set_index(index_col).index[:0] \
        if index_col else ddf._meta.index

    if order is not None:
        categories = OrderedDict([(column, categories[column])
                                  for column in order])
    columns = []
    for column, column_cat in categories.items():
        if isinstance(column_cat, str):
            column_cat = _just_read_array(column_cat)
        columns.extend(column_cat)

    meta = sp.SparseFrame(np.array([]), columns=columns,
                          index=idx_meta)

    dsf = ddf.map_partitions(sparse_one_hot,
                             categories=categories,
                             index_col=index_col,
                             meta=object)

    return SparseFrame(dsf.dask, dsf._name, meta, dsf.divisions)
