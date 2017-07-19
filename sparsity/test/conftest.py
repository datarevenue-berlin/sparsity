import os
import shutil
import tempfile
from contextlib import contextmanager

import numpy as np
import pandas as pd
import pytest
import sparsity


@pytest.fixture()
def testdb():
    return os.path.join(sparsity.__path__[0], 'test/tiny.tdb')


@pytest.fixture()
def clickstream():
    df = pd.DataFrame(dict(
        page_id=np.random.choice(list('ABCDE'), size=100),
        other_categorical=np.random.choice(list('FGHIJ'), size=100),
        id=np.random.choice([1,2,3,4,5,6,7,8,9], size=100)
        ),
    index=pd.date_range("2016-01-01", periods=100))
    return df


@contextmanager
def tmpdir(dir=None):
    dirname = tempfile.mkdtemp(dir=dir)

    try:
        yield dirname
    finally:
        if os.path.exists(dirname):
            shutil.rmtree(dirname, ignore_errors=True)
