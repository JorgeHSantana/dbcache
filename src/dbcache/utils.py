from __future__ import annotations
import re, os, typing as T, pandas as pd

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def parse_lookback(s: str | None) -> pd.Timedelta:
    if not s:
        return pd.Timedelta(0)
    try:
        return pd.to_timedelta(s)
    except Exception:
        return pd.to_timedelta("0min")

def normalize_ts(x: T.Any, tz: str | None) -> pd.Timestamp:
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
    first = stripped.split(None, 1)[0].upper() if stripped else ""
    if first in {"SELECT", "WITH"}:
        if ";" in stripped.strip(";"):
            return False
        return True
    return False

def expand_in_clause(sql: str, params: dict) -> tuple[str, dict]:
    def repl(m: re.Match):
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

def to_psycopg_named(sql: str) -> str:
    def repl(m: re.Match):
        name = m.group(1)
        return f"%({name})s"
    return re.sub(r":([a-zA-Z_]\w*)", repl, sql.replace("::", "\x00")).replace("\x00", "::")
