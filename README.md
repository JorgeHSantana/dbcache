# dbcache

### Instalation
```bash
pip install "dbcache @ git+https://github.com/JorgeHSantana/dbcache.git@main"
```
or
```bash
pip install "dbcache[all] @ git+https://github.com/JorgeHSantana/dbcache.git@main"
```

### Example
```python
# -*- coding: utf-8 -*-
from dbcache import DataBase, CacheHint, enable_db_hit_prints, SchemaMismatchError
from datetime import datetime, timezone

# --- bootstrap the cache-enabled DB client ---
db = DataBase(
    url="host.com", port=5432, user="read_user", password="read_pass", name="my_db",
    path="database/",                 # local cache root
    db_type="postgres",
    cache_format="parquet",           # "csv" or "parquet"
    compress=True,                    # gzip for CSV, codec for Parquet
    parquet_codec="snappy",           # "snappy" or "zstd" (for parquet)
    compression_level=9,              # only used if cache_format="csv"
    partitioning="monthly",           # "none" | "daily" | "monthly"
    index_columns=["id", "datetime"], # used for de-duplication when merging deltas
    datetime_tz="UTC",                # normalize all timestamps to UTC in cache
    lookback="30min"                  # GLOBAL lookback (fallback if per-query not set)
)

# (optional) print when the DB is actually queried
enable_db_hit_prints(db)

# --- time window for both examples ---
start = datetime(2025, 10, 1, 0, 0, 0, tzinfo=timezone.utc)
end   = datetime(2025, 10, 22, 0, 0, 0, tzinfo=timezone.utc)

# ---------------------------------------------------------------------------
# EXAMPLE A: WITH RAW SQL (select + CacheHint)
# ---------------------------------------------------------------------------
sql = """
SELECT
    *
FROM
    my_table
WHERE
    datetime >= :start
    AND datetime < :end
ORDER BY
    finished
"""

try:
    # Using a SQL query with a per-call CacheHint (has its own lookback)
    df_sql = db.select(
        sql,
        params={"start": start, "end": end},
        cache_hint=CacheHint(
            cache_key="my_table",            # cache directory under database/my_db/_derived/my_table
            datetime_column="datetime",      # time index column present in the *result set*
            range_param_names=("start","end"),
            index_columns=["id","datetime"], # used to drop duplicates on merge
            partitioning="monthly",
            lookback="30min",                # PER-CALL lookback (overrides DataBase.lookback for this call)
        ),
        cache_mode="auto",
    )
    print("=== SQL-based fetch (with CacheHint) ===")
    print(df_sql.head())

except SchemaMismatchException as e:
    # v0.2.4: if the query adds/drops columns vs cached schema, this throws BEFORE downloading data
    print(f"[schema error] {e}")


# ---------------------------------------------------------------------------
# EXAMPLE B: WITHOUT SQL (table access + incremental window)  <-- NEW
#            This uses db.get_table(...), i.e., no raw SQL string.
# ---------------------------------------------------------------------------
try:
    # Important: the `columns` you request define the *table cache schema*.
    # v0.2.4 enforces schema stability: if cache exists and you change columns later,
    # SchemaMismatchError will be raised BEFORE any download.
    df_tbl = db.get(
        table="my_table",
        columns=["id", "datetime", "created", "finished"],  # requested schema (must match cached schema once set)
        start=start,
        end=end,
        datetime_column="datetime",         # the time-column present in the table
        where='created > "datetime"',       # optional extra filter (ANDed on server side)
        order_by="datetime"                 # optional ORDER BY clause
    )

    print("=== Table-based fetch (no raw SQL) ===")
    print(df_tbl.head())

except SchemaMismatchException as e:
    print(f"[schema error] {e}")

```
