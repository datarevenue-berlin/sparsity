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

Sum, mean, min and max methods are called on underlying Scipy CSR matrix
object. They can be computed over whole SparseFrame or along columns/rows:
```pydocstring
>>> sf.sum(axis=0)
matrix([[0.        , 2.79813655, 0.84659119, 2.8522892 , 2.88412053]])
>>> sf.mean(axis=1)
matrix([[0.        ],
        [0.19257014],
        [0.53127046],
        [0.19363253],
        [0.19712191],
        [0.        ],
        [0.19913979],
        [0.19542124],
        [0.        ],
        [0.36707143]])
>>> sf.min()
0.0
>>> sf.max()
0.9956989680903189
```

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

### Joining

By default SparseFrames are joined on their indexes:
```pydocstring
>>> sf2 = sparsity.SparseFrame(np.random.rand(3, 2), index=[9, 10, 11], columns=['X', 'Y'])
>>> sf2
           X         Y
9   0.182890  0.061269
10  0.039956  0.595605
11  0.407291  0.496680
[3x2 SparseFrame of type '<class 'float64'>' 
 with 6 stored elements in Compressed Sparse Row format]

>>> sf.join(sf2)
      A         B    C         D         E         X         Y
9   0.0  0.000000  0.0  0.000000  0.000000  0.182890  0.061269
10  0.0  0.000000  0.0  0.000000  0.000000  0.039956  0.595605
11  0.0  0.962851  0.0  0.000000  0.000000  0.407291  0.496680
12  0.0  0.858180  0.0  0.867824  0.930348  0.000000  0.000000
13  0.0  0.000000  0.0  0.000000  0.968163  0.000000  0.000000
[11x7 SparseFrame of type '<class 'float64'>' 
 with 16 stored elements in Compressed Sparse Row format]
```

You can also join on columns:
```pydocstring
>>> sf3 = sparsity.SparseFrame(np.random.rand(3, 2), index=[97, 98, 99], columns=['E', 'F'])
>>> sf3
           E         F
97  0.738614  0.958507
98  0.868556  0.230316
99  0.322914  0.587337
[3x2 SparseFrame of type '<class 'float64'>' 
 with 6 stored elements in Compressed Sparse Row format]

>>> sf.join(sf3, axis=0).iloc[-5:]
      A    B         C         D         E         F
18  0.0  0.0  0.000000  0.000000  0.000000  0.000000
19  0.0  0.0  0.846591  0.988766  0.000000  0.000000
97  0.0  0.0  0.000000  0.000000  0.738614  0.958507
98  0.0  0.0  0.000000  0.000000  0.868556  0.230316
99  0.0  0.0  0.000000  0.000000  0.322914  0.587337
[5x6 SparseFrame of type '<class 'float64'>' 
 with 8 stored elements in Compressed Sparse Row format]
```

### Groupby

Groupby-sum operation is optimized for sparse case:
```pydocstring
>>> df = pd.DataFrame({'X': [1, 1, 1, 0], 
...                    'Y': [0, 1, 0, 1],
...                    'gr': ['a', 'a', 'b', 'b'],
...                    'day': [10, 11, 11, 12]})
>>> df = df.set_index(['day', 'gr'])>>> sf4 = sparsity.SparseFrame(df)
>>> sf4
          X    Y
day gr          
10  a   1.0  0.0
11  a   1.0  1.0
    b   1.0  0.0
12  b   0.0  1.0
[4x2 SparseFrame of type '<class 'float64'>' 
 with 5 stored elements in Compressed Sparse Row format]

>>> sf4.groupby_sum(level=1)
     X    Y
a  2.0  1.0
b  1.0  1.0
[2x2 SparseFrame of type '<class 'float64'>' 
 with 4 stored elements in Compressed Sparse Row format]
```

Other operations can also be applied:
```pydocstring
>>> sf4.groupby_agg(level=1, agg_func=lambda x: x.mean(axis=0))
     X    Y
a  1.0  0.5
b  0.5  0.5
[2x2 SparseFrame of type '<class 'float64'>' 
 with 4 stored elements in Compressed Sparse Row format]
```
