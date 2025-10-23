
# dbcache

**Incremental, on-disk caching for SQL `SELECT`s** with:
- **Gap filling:** fetch only the **missing time ranges** (“holes”) in **one query** (`fill_gaps=True`).
- **Partitioned storage** (monthly/daily/none) in **CSV(.gz)** or **Parquet** (Snappy/Zstd).
- **Schema guard:** prevents silent column drift—throws before downloading if result-set columns differ from cached schema.
- **De-duplication** on merge (by configurable index columns).
- **Driver niceties:** named params `:start`, `:end`, safe `%` literal handling (e.g., `LIKE '3.4.%'`), `IN (:ids)` auto-expansion.
- **Postgres + MySQL** adapters (psycopg/psycopg2, PyMySQL/mysqlclient).
- **Simple observability:** one-liner to print when the DB is actually hit.

## Why dbcache?

- **Download only once**: The first request seeds local partitions; subsequent requests reuse local data and fetch **only what’s missing**.
- **Holes-first strategy**: If you have a patchwork cache (e.g., 1–3, 7–10, 15–16), `fill_gaps=True` issues a single **segment-join** query to pull just the  **missing intervals**.
- **Safety by default**: If your query/tables change columns later, `dbcache` refuses to mutate the cache—**it fails fast** with a clear `SchemaMismatchError`.
- **Works with raw SQL _or_ a no-SQL convenience API** (`get_table`).
- **Predictable coverage**: A **coverage policy** (via `lookback`) decides whether the current cache is “good enough” to answer without hitting the DB.

---

## Installation

From GitHub (editable version pinning `main`):

```bash
pip install "dbcache @ git+https://github.com/JorgeHSantana/dbcache.git@main"
```

With extras (parquet + both DB drivers):

```bash
pip install "dbcache[all] @ git+https://github.com/JorgeHSantana/dbcache.git@main"
```

> **Notes**
> - **MySQL**: installs `pymysql` by default; `mysqlclient` is available via `dbcache[mysql]`.
> - **Parquet** requires `pyarrow` (included in `[all]` or `[parquet]`).

---

## Quick start

```python
from dbcache import DataBase, CacheHint, enable_db_hit_prints, SchemaMismatchError
from datetime import datetime, timezone

# 1) bootstrap a cache-enabled DB client
db = DataBase(
    url="host.com", port=5432, user="read_user", password="read_pass", name="my_db",
    path="database/",                   # local cache root
    db_type="postgres",                 # "postgres" | "mysql"
    cache_format="parquet",             # "csv" or "parquet"
    compress=True,                      # gzip for CSV, codec for Parquet
    parquet_codec="snappy",             # "snappy" | "zstd" (parquet only)
    compression_level=9,                # CSV only
    partitioning="monthly",             # "none" | "daily" | "monthly"
    index_columns=["id", "datetime"],   # used to dedupe when merging deltas
    datetime_tz="UTC",                  # normalize timestamps to UTC in cache
    lookback="30min"                    # GLOBAL lookback if per-query is not set
)

# 2) (optional) print when the DB is actually queried
enable_db_hit_prints(db)

# 3) time window for examples
start = datetime(2025, 10, 1, 0, 0, 0, tzinfo=timezone.utc)
end   = datetime(2025, 10, 22, 0, 0, 0, tzinfo=timezone.utc)
```

---

## Example A — Raw SQL with `CacheHint` (gap filling on)

```python
sql = \"\"\"
SELECT
  id,
  created,
  datetime
FROM my_table
WHERE
  datetime >= :start
  AND datetime <  :end
ORDER BY
  datetime
\"\"\"

try:
    df_sql = db.select(
        sql,
        params={"start": start, "end": end},
        cache_hint=CacheHint(
            cache_key="my_table_window",      # on-disk: database/my_db/_derived/my_table_window/...
            datetime_column="datetime",       # time column in the *result set*
            range_param_names=("start","end"),
            index_columns=["id","datetime"],  # dedupe keys on merge
            partitioning="monthly",
            lookback="30min",                 # PER-CALL lookback (overrides DataBase.lookback)
            fill_gaps=True,                   # NEW: fetch only missing intervals in ONE query
            max_segments=24                   # cap segments per roundtrip
        ),
        cache_mode="auto",                    # "auto" | "force" | "none"
    )
    print(df_sql.head())

except SchemaMismatchError as e:
    # thrown BEFORE download if columns differ from previously cached schema
    print(f"[schema error] {e}")
```

### What happens here?
- **First run:** populates monthly partitions for the requested window.
- **Next runs:** if cache **covers** the window (see “Coverage policy” below), it **doesn’t hit the DB**.
- **If there are holes** inside the window: with `fill_gaps=True`, the adapter assembles a **single segment-join query**:
  - **Postgres:** `WITH segs AS (SELECT unnest(...) s, unnest(...) e) JOIN ...`
  - **MySQL:** `JOIN (SELECT :s0,:e0 UNION ALL ...) segs ON ...`
- **Merging:** downloaded rows are merged + deduped by `index_columns`, then re-partitioned on disk.

---

## Example B — No raw SQL (`get_table`) with gap filling

```python
try:
    df_tbl = db.get_table(
        table="my_table",
        columns=["id", "datetime", "created"],  # defines the cached schema for this table
        start=start,
        end=end,
        datetime_column="datetime",
        where='created > datetime',             # optional (ANDed server-side)
        order_by="datetime",
        fill_gaps=True,                         # NEW: fill holes using one segmented query
        max_segments=24
    )
    print(df_tbl.head())

except SchemaMismatchError as e:
    print(f"[schema error] {e}")
```

> **Schema guard for tables**  
> The **first call** to `get_table` sets the **cached schema** (the `columns` you requested).  
> If later you request **different columns**, you’ll get `SchemaMismatchError` **before** any download.

---

## Coverage policy (when do we hit the DB?)

Let `dt` be your time column (e.g., `datetime`). The cache is considered **good enough** for `[start, end)` if:

```
min(cache.dt) <= start   AND   max(cache.dt) >= end - lookback
```

- If **true**, `dbcache` serves the request **entirely from local cache**.
- If **false**:
  - With `fill_gaps=True`, it computes **missing segments** inside `[start, end)` and fetches only those in one query.
  - Without `fill_gaps`, it fetches a **continuous trailing delta** from `max(start, max(cache.dt)-lookback)` to `end`.

You may set `lookback`:
- **Globally** at `DataBase` construction (default fallback).
- **Per-call** via `CacheHint.lookback`.

> Tip: If your window is “closed” (no new data expected), you can **increase the per-call lookback** (e.g., `30d`, `365d`) so coverage passes and the DB isn’t hit again.

---

## FAQ

**Q: How do I know if it hit the DB?**  
Call `enable_db_hit_prints(db)`. You’ll see:  
`[dbcache] Hitting DB (execute_select). Fetching delta...`

**Q: Can I disable the cache for one call?**  
Yes, `cache_mode="none"` in `select(...)` — it bypasses disk.

**Q: Force cache-only?**  
Use `cache_mode="force"`. If coverage fails or params are missing, it raises.

**Q: What about `%` in SQL?**  
`dbcache` preserves DB-API placeholders and **escapes literal `%`** in strings (e.g., `LIKE '3.4.%'`), so the driver doesn’t misinterpret them.

**Q: `IN (:ids)` with lists?**  
Works. `expand_in_clause` transforms it into `IN (:ids_0, :ids_1, ...)` and binds each element.

**Q: Parquet vs CSV(.gz)?**  
Parquet tends to be smaller and faster to read; CSV(.gz) is universal and human-friendly. Both support partitioning.

**Q: MySQL driver?**  
`pymysql` is installed by default. You can also `pip install "dbcache[mysql]"` to use `mysqlclient`.

**Q: Clear the cache?**  
Delete the on-disk folder under `path/name/...` (e.g., `database/my_db/...`).

---

## Minimal MySQL example

```python
db = DataBase(
    url="mysql-host", port=3306, user="u", password="p", name="my_db",
    path="cache/", db_type="mysql",
    cache_format="parquet", compress=True, parquet_codec="snappy",
    partitioning="monthly", index_columns=["id","dt"], datetime_tz="UTC", lookback="30min"
)

enable_db_hit_prints(db)

sql = \"\"\"
SELECT id, dt, status
FROM status_history
WHERE dt >= :start AND dt < :end
  AND code_version LIKE '3.4.%'
ORDER BY dt
\"\"\"

df = db.select(
    sql,
    params={"start": start, "end": end},
    cache_hint=CacheHint(
        cache_key="status_history_v34",
        datetime_column="dt",
        range_param_names=("start","end"),
        index_columns=["id","dt"],
        partitioning="monthly",
        lookback="30min",
        fill_gaps=True
    ),
    cache_mode="auto",
)
```

---

## Versioning highlights

- **0.2.12**: **Gap filling** (one segmented query), neutralized-param **schema peek** (no accidental formatting), percent-literal handling, MySQL & Postgres parity.
- Prior: incremental trailing-delta only (no official hole-filling), schema guard, parquet/CSV, partitioned cache, duplicates removal.

---

## License

MIT
