from glob import glob
from math import ceil

import numpy as np
import pandas as pd
from dask import delayed, base
from dask.base import tokenize
from dask.dataframe.io.io import sorted_division_locations
from dask.dataframe.utils import make_meta

import sparsity as sp
from sparsity.dask.core import SparseFrame
from sparsity.io import _write_dict_npz, _open_npz_archive

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
    if not all(np.issubdtype(dtype, np.number) for
               dtype in ddf.dtypes.tolist()):
        raise ValueError('Cannot create a sparse frame '
                         'of not numerical type')

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
    meta = make_meta(df)
    return SparseFrame(dsk, name, meta, divisions)


def read_npz(path, read_divisions=False, storage_options=None):
    """
    Read SparseFrame from npz archives

    Parameters
    ----------
    path: str
        path to load files from can contain '*' to
        reference multiple files
    read_divisions: bool
        if the files are sorted read the index for each file
        to obtain divions. If files are not sorted this will
        raise and error.

    Returns
    -------
        dsf: dask.SparseFrame
    """
    dsk = {}
    name = 'read_npz-{}'.format(tokenize(path))
    loader = None
    divisions = None
    try:
        loader = _open_npz_archive(path.split('*')[0] + 'metadata.npz',
                                   storage_options)
        divisions = loader['divisions']
        _paths = loader['partitions']
    except FileNotFoundError:
        _paths = _sorted(list(glob(path)))
    finally:
        if loader:
            loader.close()
    archive = _open_npz_archive(_paths[0], storage_options)

    meta_idx, meta_cols = archive['frame_index'], archive['frame_columns']
    meta = sp.SparseFrame(np.empty(shape=(0, len(meta_cols))),
                          index=meta_idx[:0],
                          columns=meta_cols)

    for i, p in enumerate(_paths):
        dsk[name, i] = (sp.SparseFrame.read_npz, p, storage_options)

    if divisions is None and read_divisions:
        level = 0 if isinstance(meta_idx, pd.MultiIndex) else None
        divisions = _npz_read_divisions(_paths, level=level)
    elif divisions is None:
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


def write_npz_metadata(writes, divisions, paths, fn,
                       block_size, storage_options):
    data = {}
    data['divisions'] = np.asarray(divisions)
    data['partitions'] = np.asarray(paths)

    _write_dict_npz(data, fn, block_size, storage_options)


def to_npz(sf: SparseFrame, path: str, block_size=None,
           storage_options=None, compute=True):
    if '*' not in path:
        raise ValueError('Path needs to contain "*" wildcard.')

    if '.npz' not in path:
        path += '.npz'

    tmpl_func = path.replace('*', '{0:06d}').format
    metadata_fn = path.split('*')[0] + 'metadata.npz'
    paths = list(map(tmpl_func, range(sf.npartitions)))

    write = delayed(sp.SparseFrame.to_npz, pure=False)
    writes = [write(part, fn, block_size, storage_options)
              for fn, part in zip(paths, sf.to_delayed())]

    write_metadata = delayed(write_npz_metadata, pure=False)
    out = write_metadata(writes, sf.divisions, paths, metadata_fn,
                         block_size, storage_options)

    if compute:
        out.compute()
        return None
    return out
