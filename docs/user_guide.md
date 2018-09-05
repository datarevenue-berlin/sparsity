# Sparsity User Guide

## Quick start

### Creating a SparseFrame

Create a SparseFrame from numpy array:
```pydocstring
>>> import sparsity
>>> import numpy as np

>>> a = np.random.rand(10, 5)
>>> a[a < 0.9] = 0
>>> sf = sparsity.SparseFrame(a, index=np.arange(10, 20), columns=list('ABCDE'))
>>> sf
      A         B    C         D         E
10  0.0  0.000000  0.0  0.000000  0.000000
11  0.0  0.962851  0.0  0.000000  0.000000
12  0.0  0.858180  0.0  0.867824  0.930348
13  0.0  0.000000  0.0  0.000000  0.968163
14  0.0  0.000000  0.0  0.000000  0.985610
[10x5 SparseFrame of type '<class 'float64'>' 
 with 10 stored elements in Compressed Sparse Row format]
```

You can also create a SparseFrame from Pandas DataFrame:
```pydocstring
>>> import pandas as pd

>>> df = pd.DataFrame(a, index=np.arange(10, 20), columns=list('ABCDE'))
>>> sf = sparsity.SparseFrame(df)
>>> sf
      A         B    C         D         E
10  0.0  0.000000  0.0  0.000000  0.000000
11  0.0  0.962851  0.0  0.000000  0.000000
12  0.0  0.858180  0.0  0.867824  0.930348
13  0.0  0.000000  0.0  0.000000  0.968163
14  0.0  0.000000  0.0  0.000000  0.985610
[10x5 SparseFrame of type '<class 'float64'>' 
 with 10 stored elements in Compressed Sparse Row format]
```

### Indexing and subscripting

Indexing a SparseFrame with column name gives a new SparseFrame:
```pydocstring
>>> sf['A']
>>> sf['A']
      A
10  0.0
11  0.0
12  0.0
13  0.0
14  0.0
[10x1 SparseFrame of type '<class 'float64'>' 
 with 0 stored elements in Compressed Sparse Row format]
```

Similarly for a list of column names:
```pydocstring
>>> sf[['A', 'B']]
      A         B
10  0.0  0.000000
11  0.0  0.962851
12  0.0  0.858180
13  0.0  0.000000
14  0.0  0.000000
[10x2 SparseFrame of type '<class 'float64'>' 
 with 3 stored elements in Compressed Sparse Row format]
```

### Basic arithmetic operations

Add 2 SparseFrames:
```pydocstring
>>> sf.add(sf)
      A         B    C         D         E
10  0.0  0.000000  0.0  0.000000  0.000000
11  0.0  1.925701  0.0  0.000000  0.000000
12  0.0  1.716359  0.0  1.735649  1.860697
13  0.0  0.000000  0.0  0.000000  1.936325
14  0.0  0.000000  0.0  0.000000  1.971219
[10x5 SparseFrame of type '<class 'float64'>' 
 with 10 stored elements in Compressed Sparse Row format]
```

Multiply each row/column by a number:
```pydocstring
>>> sf.multiply(np.arange(10), axis='index')
      A         B    C         D         E
10  0.0  0.000000  0.0  0.000000  0.000000
11  0.0  0.962851  0.0  0.000000  0.000000
12  0.0  1.716359  0.0  1.735649  1.860697
13  0.0  0.000000  0.0  0.000000  2.904488
14  0.0  0.000000  0.0  0.000000  3.942438
[10x5 SparseFrame of type '<class 'float64'>' 
 with 10 stored elements in Compressed Sparse Row format]

>>> sf.multiply(np.arange(5), axis='columns')
      A         B    C         D         E
10  0.0  0.000000  0.0  0.000000  0.000000
11  0.0  0.962851  0.0  0.000000  0.000000
12  0.0  0.858180  0.0  2.603473  3.721393
13  0.0  0.000000  0.0  0.000000  3.872651
14  0.0  0.000000  0.0  0.000000  3.942438
[10x5 SparseFrame of type '<class 'float64'>' 
 with 10 stored elements in Compressed Sparse Row format]
```

### Join and groupby

Join 2 SparseFrames:
```pydocstring

```