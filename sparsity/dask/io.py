from glob import glob
from math import ceil

import numpy as np
import pandas as pd
from dask.base import tokenize
from dask.dataframe.io.io import sorted_division_locations

import sparsity as sp
from sparsity.dask.core import SparseFrame, _make_meta

_sorted = sorted


def from_ddf(ddf):
    """Convert a dask.dataframe.DataFrame to a sparsity.dask.SparseFrame.

    Parameters
    ----------
    ddf: dask.dataframe.DataFrame

    Returns
    -------
    dsf: sparsity.dask.SparseFrame
        a sparse dataframe collection
    """
    tmp = ddf.map_partitions(sp.SparseFrame, meta=object)
    dsf = SparseFrame(tmp.dask, tmp._name, ddf._meta,
                      divisions=tmp.divisions)
    return dsf


def from_pandas(df, npartitions=None, chunksize=None, name=None):
    """
    Parameters
    ----------
    df : pandas.DataFrame or pandas.Series
        The DataFrame/Series with which to construct a Dask DataFrame/Series
    npartitions : int, optional
        The number of partitions of the index to create. Note that depending on
        the size and index of the dataframe, the output may have fewer
        partitions than requested.
    chunksize : int, optional
        The size of the partitions of the index.
    name: string, optional
        An optional keyname for the dataframe. Define when dataframe large.
        Defaults to hashing the input. Hashing takes a lot of time on large df.
    """
    nrows = df.shape[0]

    if chunksize is None:
        chunksize = int(ceil(nrows / npartitions))
    else:
        npartitions = int(ceil(nrows / chunksize))

    if not df.index.is_monotonic_increasing:
        df = df.sort_index()

    divisions, locations = sorted_division_locations(df.index,
                                                     chunksize=chunksize)
    name = name or 'from_pandas-{}'.format(tokenize(df, npartitions))
    dsk = dict(((name, i), sp.SparseFrame(df.iloc[start: stop]))
               for i, (start, stop) in enumerate(zip(locations[:-1],
                                                     locations[1:])))
    meta = _make_meta(df)
    return SparseFrame(dsk, name, meta, divisions)


def read_npz(path, sorted=False):
    """
    Read SparseFrame from npz archives

    Parameters
    ----------
    path: str
        path to load files from can contain '*' to
        reference multiple files
    sorted: bool
        if the files are sorted read the index for each file
        to obtain divions

    Returns
    -------
        dsf: dask.SparseFrame
    """
    dsk = {}
    name = 'read_npz-{}'.format(tokenize(path))
    _paths = _sorted(list(glob(path)))
    archive = np.load(_paths[0])

    meta_idx, meta_cols = archive['frame_index'], archive['frame_columns']
    meta = sp.SparseFrame(np.empty(shape=(0, len(meta_cols))),
                          index=meta_idx[:0],
                          columns=meta_cols)
    for i, p in enumerate(_paths):
        dsk[name, i] = (sp.SparseFrame.read_npz, p)

    if sorted:
        level = 0 if isinstance(meta_idx, pd.MultiIndex) else None
        divisions = _npz_read_divisions(_paths, level=level)
    else:
        divisions = [None] * (len(_paths) + 1)

    return SparseFrame(dsk, name, meta, divisions=divisions)


def _npz_read_divisions(paths, level=None):
    """Load paths sequentially and generate divisions list."""
    divisions = []
    assert len(paths) > 1
    for p in paths:
        archive = np.load(p)
        idx = archive['frame_index']
        if level is not None:
            idx = idx.get_level_values(level)
        assert idx.is_monotonic_increasing
        istart = idx[0]
        istop = idx[-1]
        divisions.append(istart)
    divisions.append(istop)

    for i in range(len(divisions) - 1):
        if not divisions[i] < divisions[i+1]:
            raise ValueError("Divisions are not sorted"
                             "Problematic File:\n"
                             "{file}, !{div1} < {div2}".format(
                file=paths[i], div1=divisions[i], div2=divisions[i+1]
            ))

    return divisions