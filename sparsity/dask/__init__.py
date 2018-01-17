import dask
from dask.dataframe.core import _get_return_type as \
    df_get_return_type

import sparsity as sp
from .core import SparseFrame
from .io import from_pandas, read_npz, from_ddf
from .reshape import one_hot_encode


def _get_return_type_sparsity(meta):
    # We need this to make dask dataframes _LocIndexer to work
    # on SparseFrames
    if isinstance(meta, SparseFrame):
        meta = meta._meta

    if isinstance(meta, sp.SparseFrame):
        return SparseFrame

    return df_get_return_type(meta)

dask.dataframe.core._get_return_type = _get_return_type_sparsity