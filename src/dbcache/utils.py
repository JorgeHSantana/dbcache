
import re, os
from typing import Any, Dict, Tuple
import pandas as pd

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def parse_lookback(s: str) -> pd.Timedelta:
    return pd.to_timedelta(s or "0min")

def normalize_ts(x: Any, tz: str) -> pd.Timestamp:
    ts = pd.to_datetime(x)
    if tz:
        ts = ts.tz_localize(tz) if ts.tzinfo is None else ts.tz_convert(tz)
        ts = ts.tz_convert("UTC")
    elif ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts

def month_key(ts: pd.Timestamp) -> str:
    return f"{ts.year:04d}-{ts.month:02d}"

def day_key(ts: pd.Timestamp) -> str:
    return f"{ts.year:04d}-{ts.month:02d}-{ts.day:02d}"

def is_select_sql(sql: str) -> bool:
    stripped = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE).strip()
    if not stripped: return False
    first = stripped.split(None, 1)[0].upper()
    if first in {"SELECT", "WITH"}:
        return ";" not in stripped.strip(";")
    return False

def expand_in_clause(sql: str, params: Dict) -> Tuple[str, Dict]:
    def repl(m: re.Match) -> str:
        name = m.group(1)
        val = params.get(name, None)
        if isinstance(val, (list, tuple)):
            if len(val) == 0:
                return "IN (NULL) /* empty list */"
            phs = []
            for i in range(len(val)):
                phs.append(f":{name}_{i}")
                params[f"{name}_{i}"] = val[i]
            params.pop(name, None)
            return "IN (" + ", ".join(phs) + ")"
        return f"IN :{name}"
    pattern = re.compile(r"IN\s*\(\s*:(\w+)\s*\)", re.IGNORECASE)
    new_sql = pattern.sub(repl, sql)
    return new_sql, params

def _escape_percent_literals_after_named(sql: str) -> str:
    """Escape % literals for DB-API 'pyformat' while keeping %(name)s placeholders intact."""
    placeholders = {}
    def hold(m: re.Match):
        key = f"__PH_{len(placeholders)}__"
        placeholders[key] = m.group(0)
        return key
    temp = re.sub(r"%\([a-zA-Z_]\w*\)s", hold, sql)
    temp = temp.replace('%', '%%')
    for k, v in placeholders.items():
        temp = temp.replace(k, v)
    return temp

def to_psycopg_named(sql: str) -> str:
    sql = re.sub(r":([a-zA-Z_]\w*)", lambda m: f"%({m.group(1)})s", sql.replace("::", "\x00")).replace("\x00", "::")
    return _escape_percent_literals_after_named(sql)

def neutralize_named_params(sql: str) -> str:
    """Replace :name parameters with NULL for peek-only queries."""
    sql = re.sub(r"IN\s*\(\s*:[a-zA-Z_]\w*\s*\)", "IN (NULL)", sql, flags=re.IGNORECASE)
    sql = re.sub(r":[a-zA-Z_]\w*", "NULL", sql)
    return sql
