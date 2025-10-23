
from .db import DataBase
from .cache import CacheHint
from .exceptions import SchemaMismatchError

__all__ = ["DataBase", "CacheHint", "enable_db_hit_prints", "SchemaMismatchError"]

def enable_db_hit_prints(db):
    """Print when we actually hit the DB (execute_select)."""
    orig = db.adapter.execute_select
    def wrapper(sql, params=None):
        print("[dbcache] Hitting DB (execute_select). Fetching delta...", flush=True)
        return orig(sql, params or {})
    db.adapter.execute_select = wrapper
    return orig
