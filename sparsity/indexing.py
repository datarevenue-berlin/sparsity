from pandas.core.indexing import _LocIndexer, _iLocIndexer

def get_indexers_list():

    return [
        ('iloc', _CsrILocationIndexer),
        ('loc', _CsrLocIndexer),
    ]


class _CsrLocIndexer(_LocIndexer):

    def __getitem__(self, item):
        return super().__getitem__(item)

    def _slice(self, slice, axis=0, kind=None):
        if axis != 0:
            raise NotImplementedError()
        return self.obj._slice(slice)


class _CsrILocationIndexer(_iLocIndexer):

    def __getitem__(self, item):
        return super().__getitem__(item)

    def _slice(self, slice, axis=0, kind=None):
        if axis != 0:
            raise NotImplementedError()
        return self.obj._slice(slice)
