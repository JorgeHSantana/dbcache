
def test_import():
    import dbcache
    assert hasattr(dbcache, 'DataBase')
    assert hasattr(dbcache, 'CacheHint')
