import numpy as np
import pandas as pd
from scipy import sparse

try:
    from traildb import TrailDB
    from sparsity._traildb import traildb_coo_repr_func
except (ImportError, OSError):
    TrailDB = False

def traildb_to_coo(db, fieldname):
    if not TrailDB:
        raise ImportError("Could not find traildb")
    db_handle = TrailDB(db)
    num_events = db_handle.num_events
    del db_handle
    r_idx = np.zeros(num_events, dtype=np.uint64)
    c_idx = np.zeros(num_events, dtype=np.uint64)
    uuids = np.zeros((num_events,16), dtype=np.uint8)
    timestamps = np.zeros(num_events, dtype=np.uint64)

    cols = traildb_coo_repr_func(db.encode(), fieldname.encode(), r_idx,
                                 c_idx, uuids, timestamps)
    return uuids, timestamps, cols,\
        sparse.coo_matrix((np.ones(num_events), (r_idx, c_idx)))

def to_npz(sf, filename):
    data = _csr_to_dict(sf.data)
    data['metadata'] = \
        {'multiindex': True if isinstance(sf.index, pd.MultiIndex) else False}
    data['frame_index'] = sf.index.values
    data['frame_columns'] = sf.columns.values
    np.savez(filename, **data)


def read_npz(filename):
    loader = np.load(filename)
    csr_mat = _load_csr(loader)
    idx = _load_idx_from_npz(loader)
    cols = loader['frame_columns']
    return (csr_mat, idx, cols)


def _csr_to_dict(array):
    return dict(data = array.data ,indices=array.indices,
                indptr =array.indptr, shape=array.shape)

def _load_csr(loader):
    return sparse.csr_matrix((loader['data'],
                              loader['indices'],
                              loader['indptr']),
                             shape=loader['shape'])


def _load_idx_from_npz(loader):
    idx = loader['frame_index']
    try:
        if loader['metadata'][()]['multiindex']:
            idx = pd.MultiIndex.from_tuples(idx)
    except KeyError:
        if all(map(lambda x: isinstance(x, tuple), idx)):
            idx = pd.MultiIndex.from_tuples(idx)
    return idx


def _just_read_array(path):
    if path.endswith('hdf') or path.endswith('hdf5'):
        return pd.read_hdf(path, '/df').values
    elif path.endswith('csv'):
        return pd.read_csv(path).values
    elif path.endswith('pickle'):
        return pd.read_pickle(path).values