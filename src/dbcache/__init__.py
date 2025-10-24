
import re
from datetime import datetime, date
from decimal import Decimal

from .db import DataBase
from .cache import CacheHint
from .exceptions import SchemaMismatchError

__all__ = ["DataBase", "CacheHint", "enable_db_hit_prints", "enable_sql_echo", "enable_full_sql_echo", "SchemaMismatchError"]

def enable_db_hit_prints(db):
    orig = db.adapter.execute_select
    def wrapper(sql, params=None):
        print("[dbcache] Hitting DB (execute_select).", flush=True)
        return orig(sql, params or {})
    db.adapter.execute_select = wrapper
    return orig

def enable_sql_echo(db):
    orig = db.adapter.execute_select
    def wrapper(sql, params=None):
        print("\n[dbcache][SQL]", sql)
        print("[dbcache][PARAMS]", params or {})
        return orig(sql, params or {})
    db.adapter.execute_select = wrapper
    return orig

def _fmt_sql_literal(v):
    if v is None:
        return "NULL"
    if isinstance(v, (datetime, )):
        return "'" + str(v).replace("'", "''") + "'"
    if isinstance(v, (date, )):
        return "'" + v.isoformat() + "'"
    if isinstance(v, (int, float, Decimal)):
        return str(v)
    s = str(v)
    return "'" + s.replace("'", "''") + "'"

def enable_full_sql_echo(db):
    """Debug helper that prints a 'rendered' SQL replacing :name and %(name)s placeholders.
    For PRINT ONLY - never execute the rendered string.
    """
    orig = db.adapter.execute_select
    def wrapper(sql, params=None):
        p = params or {}

        # %(name)s
        def repl_percent(m):
            key = m.group(1)
            return _fmt_sql_literal(p.get(key))
        import re as _re
        sql_rendered = _re.sub(r"%\(([a-zA-Z_]\w*)\)s", repl_percent, sql)

        # :name
        def repl_colon(m):
            key = m.group(1)
            return _fmt_sql_literal(p.get(key))
        sql_rendered = _re.sub(r":([a-zA-Z_]\w*)\b", repl_colon, sql_rendered)

        print("\n[dbcache][FULL SQL]", sql_rendered)
        return orig(sql, params or {})
    db.adapter.execute_select = wrapper
    return orig
