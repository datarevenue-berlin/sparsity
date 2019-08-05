import os
import tempfile
from contextlib import contextmanager

import numpy as np
import pandas as pd
import pytest
import shutil

import sparsity
# 2017 starts with a sunday
from sparsity import SparseFrame


@pytest.fixture()
def sampledata():
    def gendata(n, categorical=False):
        sample_data = pd.DataFrame(
            dict(date=pd.date_range("2017-01-01", periods=n)))
        sample_data["weekday"] = sample_data.date.dt.weekday_name
        sample_data["weekday_abbr"] = sample_data.weekday.apply(
            lambda x: x[:3])

        if categorical:
            sample_data['weekday'] = sample_data['weekday'].astype('category')
            sample_data['weekday_abbr'] = sample_data['weekday_abbr'] \
                .astype('category')

        sample_data["id"] = np.tile(np.arange(7), len(sample_data) // 7 + 1)[
                            :len(sample_data)]
        return sample_data

    return gendata


@pytest.fixture()
def sample_frame_labels():
    return SparseFrame(np.identity(5),
                       columns=list('ABCDE'),
                       index=list('VWXYZ'))

@pytest.fixture()
def weekdays():
    return ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday',
            'Friday', 'Saturday']


@pytest.fixture()
def weekdays_abbr(weekdays):
    return list(map(lambda x: x[:3], weekdays))


@pytest.fixture()
def groupby_frame():
    shuffle_idx = np.random.permutation(np.arange(100))
    index = np.tile(np.arange(10), 10)
    data = np.vstack([np.identity(10) for _ in range(10)])
    t = SparseFrame(data[shuffle_idx, :], index=index[shuffle_idx])
    return t


@pytest.fixture()
def sf_midx():
    midx = pd.MultiIndex.from_arrays(
        [pd.date_range("2016-10-01", periods=5),
         np.arange(5)]
    )
    cols = list('ABCDE')
    sf = SparseFrame(np.identity(5), index=midx, columns=cols)
    return sf

@pytest.fixture()
def sf_midx_int():
    midx = pd.MultiIndex.from_arrays(
        [np.concatenate([np.ones(4), np.zeros(1)]),
         pd.date_range("2016-10-01", periods=5)]
    )
    cols = list('ABCDE')
    sf = SparseFrame(np.identity(5), index=midx, columns=cols)
    return sf

@pytest.fixture()
def testdb():
    return os.path.join(sparsity.__path__[0], 'test/tiny.tdb')


@pytest.fixture()
def clickstream():
    df = pd.DataFrame(
        dict(
            page_id=np.random.choice(list('ABCDE'), size=100),
            other_categorical=np.random.choice(list('FGHIJ'), size=100),
            id=np.random.choice([1,2,3,4,5,6,7,8,9], size=100)
        ),
        index=pd.date_range("2016-01-01", periods=100),
    )
    return df


@pytest.fixture()
def complex_example():
    first = np.identity(10)
    second = np.zeros((4, 10))
    third = np.zeros((4, 10))
    second[[0, 1, 2, 3], [2, 3, 4, 5]] = 10
    third[[0, 1, 2, 3], [6, 7, 8, 9]] = 20

    shuffle_idx = np.arange(10)
    np.random.shuffle(shuffle_idx)

    first = SparseFrame(first[shuffle_idx],
                        index=np.arange(10)[shuffle_idx])

    shuffle_idx = np.arange(4)
    np.random.shuffle(shuffle_idx)

    second = SparseFrame(second[shuffle_idx],
                         index=np.arange(2, 6)[shuffle_idx])

    shuffle_idx = np.arange(4)
    np.random.shuffle(shuffle_idx)

    third = SparseFrame(third[shuffle_idx],
                        index=np.arange(6, 10)[shuffle_idx])
    return first, second, third


@contextmanager
def tmpdir(dir=None):
    dirname = tempfile.mkdtemp(dir=dir)

    try:
        yield dirname
    finally:
        if os.path.exists(dirname):
            shutil.rmtree(dirname, ignore_errors=True)
