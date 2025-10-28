
# dbcache

Incremental, on-disk caching for SELECT queries (Postgres & MySQL) with partitioned CSV/Parquet files, schema guard, and graceful fallback to cached data on DB errors.

## Highlights

- **Incremental cache**: merges only what’s new (based on a datetime column) and reuses what’s already on disk.
- **Partitioned storage**: daily/monthly partitions that scale with time.
- **CSV or Parquet**: choose your cache format (`csv` or `parquet`). Optional gzip (CSV) or codec (Parquet).
- **Schema guard**: if cached schema differs from query schema, a `SchemaMismatchError` is raised _before_ downloading.
- **Single-source of truth**: all timestamps normalized to UTC in the cache.
- **DB fallback**: on connection/query failure (e.g., timeout), if the cache covers your window, it returns cached data and prints a warning.
- **Postgres & MySQL**: adapters with named parameters (`:start`, `:end`, etc.) and automatic `IN (:ids)` expansion.
- **Packaging fixed for `src/` layout**: ready for `pip install` from zip, path, or git.

---

## Installation

> Python 3.9+

### From zip (recommended for pinned build)
```bash
pip install /path/to/dbcache.zip
# or with extras:
pip install "dbcache[all] @ file:///ABSOLUTE/path/dbcache.zip"
```

### From a local folder (editable dev install)
```bash
pip install -e /path/to/dbcache
```

### From Git
```bash
pip install "dbcache @ git+https://github.com/JorgeHSantana/dbcache.git@main"
# or all extras:
pip install "dbcache[all] @ git+https://github.com/JorgeHSantana/dbcache.git@main"
```

### Extras

- `parquet` → `pyarrow`
- `postgres` → `psycopg2-binary`, `psycopg`
- `mysql` → `mysqlclient` (optional; default MySQL driver is `pymysql`)
- `all` → everything above

> MySQL: we default to `pymysql`. To switch to the C driver, install `mysqlclient` as well.

---

## Quick Start

```python
from dbcache import DataBase, CacheHint, SchemaMismatchError, enable_db_hit_prints
from datetime import datetime, timezone

# 1) Create a DB client with cache settings
db = DataBase(
    url="host.com", port=5432, user="read_user", password="read_pass", name="my_db",
    path="database/",                 # local cache root
    db_type="postgres",               # "postgres" | "mysql"
    cache_format="parquet",           # "csv" | "parquet"
    compress=True,                    # gzip for CSV, codec for Parquet
    parquet_codec="snappy",           # "snappy" | "zstd" (Parquet only)
    compression_level=9,              # only if cache_format="csv"
    partitioning="monthly",           # "none" | "daily" | "monthly"
    index_columns=["id", "datetime"], # de-duplication keys while merging
    datetime_tz="UTC",                # normalize timestamps to UTC in cache
    lookback="30min",                 # global lookback (window overlap to refresh)
    conn_timeout=10.0,                # DB connection timeout (seconds)
)

# (optional) print when the DB is actually queried
enable_db_hit_prints(db)

# 2) Define a time window
start = datetime(2025, 10, 1, 0, 0, 0, tzinfo=timezone.utc)
end   = datetime(2025, 10, 22, 0, 0, 0, tzinfo=timezone.utc)

# 3A) Use raw SQL + CacheHint
sql = """
SELECT
    id,
    created,
    datetime
FROM
    my_table
WHERE
    datetime >= :start
    AND datetime < :end
    AND created > datetime
ORDER BY
    datetime
"""

try:
    df_sql = db.select(
        sql,
        params={"start": start, "end": end},
        cache_hint=CacheHint(
            cache_key="my_table_window",     # cache dir: database/my_db/_derived/my_table_window
            datetime_column="datetime",      # datetime column present in the *result*
            range_param_names=("start","end"),
            index_columns=["id","datetime"], # de-duplication while merging
            partitioning="monthly",
            lookback="30min",                # per-call lookback (overrides DataBase.lookback)
        ),
        cache_mode="auto",                   # "auto" | "force" | "none"
    )
    print("=== SQL-based fetch ===")
    print(df_sql.head())

except SchemaMismatchError as e:
    # Raised BEFORE any download if query schema != cached schema
    print(f"[schema error] {e}")

# 3B) Without raw SQL: just table + columns + window
try:
    df_tbl = db.get_table(
        table="my_table",
        columns=["id", "datetime", "created"],   # requested schema (must match cache once set)
        start=start,
        end=end,
        datetime_column="datetime",              # the table’s datetime column
        where='created > "datetime"',            # optional extra filter
        order_by="datetime",                     # optional ORDER BY
        index_columns=["id", "datetime"]         # de-duplication keys while merging.
    )
    print("=== Table-based fetch ===")
    print(df_tbl.head())

except SchemaMismatchError as e:
    print(f"[schema error] {e}")
```

---

## Multi-DB example (Postgres + MySQL)

```python
from dbcache import DataBase, CacheHint, enable_db_hit_prints
from datetime import datetime, timezone

conns = {}
conns["prod"] = DataBase(
    url="postgres-host", port=5432, user="user", password="pass", name="pg_db",
    path="database/prod", db_type="postgres",
    cache_format="parquet", compress=True, parquet_codec="snappy",
    compression_level=9, partitioning="monthly",
    index_columns=["pic_id", "finished"], datetime_tz="UTC",
    lookback="120min", conn_timeout=8.0
)

conns["grafana"] = DataBase(
    url="mysql-host", port=3306, user="root", password="pass", name="status_db",
    path="database/grafana", db_type="mysql",
    cache_format="parquet", compress=True, parquet_codec="snappy",
    compression_level=9, partitioning="monthly",
    index_columns=["pic_id", "reference_date"], datetime_tz="UTC",
    lookback="120min", conn_timeout=8.0
)

# print when actually hitting DB
for c in conns.values():
    enable_db_hit_prints(c)

start = datetime(2025, 10, 1, 0, 0, 0, tzinfo=timezone.utc)
end   = datetime(2025, 10, 22, 0, 0, 0, tzinfo=timezone.utc)

# Postgres
sql1 = """
SELECT pic_id, created, finished
FROM pic_data_fragment
WHERE finished >= :start AND finished < :end AND created > finished
ORDER BY finished
"""
df1 = conns["prod"].select(
    sql1,
    params={"start": start, "end": end},
    cache_hint=CacheHint(
        cache_key="pic_data_fragment",
        datetime_column="finished",
        range_param_names=("start","end"),
        index_columns=["pic_id","finished"],
        partitioning="monthly",
        lookback="120min",
    ),
    cache_mode="auto",
)

# MySQL
sql2 = """
SELECT pic_id, reference_date, status, last_status
FROM status_history
WHERE reference_date >= :start AND reference_date < :end
  AND hardware_version > 8
  AND code_version LIKE '3.4.%'
ORDER BY pic_id DESC
"""
df2 = conns["grafana"].select(
    sql2,
    params={"start": start, "end": end},
    cache_hint=CacheHint(
        cache_key="status_history_v8_3.4",
        datetime_column="reference_date",
        range_param_names=("start","end"),
        index_columns=["pic_id","reference_date"],
        partitioning="monthly",
        lookback="120min",
    ),
    cache_mode="auto",
)
```

---

## How caching works

- **Partitions**: files are written under `<path>/<dbname>/<year>/<month>.{parquet|csv[.gz]}` (for `monthly`), or per-day for `daily`.  
- **Write policy**: when merging, duplicates are dropped using `index_columns` (if set) or the full row as fallback; rows are sorted by the datetime column before writing.
- **Lookback**: a small overlap from the last cached timestamp is re-fetched to avoid missing late-arriving data and to resolve duplicates deterministically.

---

## Configuration reference (most used)

| Parameter           | Where          | Type        | Notes |
|--------------------|----------------|-------------|-------|
| `path`             | `DataBase`     | str         | Local cache root folder. |
| `db_type`          | `DataBase`     | str         | `"postgres"` or `"mysql"`. |
| `cache_format`     | `DataBase`     | str         | `"csv"` or `"parquet"`. |
| `compress`         | `DataBase`     | bool        | CSV → gzip; Parquet → enable codec. |
| `parquet_codec`    | `DataBase`     | str         | `"snappy"` or `"zstd"` (when `cache_format="parquet"`). |
| `compression_level`| `DataBase`     | int (1–9)   | CSV gzip level. |
| `partitioning`     | `DataBase`     | str         | `"none"`, `"daily"`, `"monthly"`. |
| `index_columns`    | `DataBase`     | list[str]   | Keys to de-duplicate on merge. |
| `datetime_tz`      | `DataBase`     | str         | Input TZ; cache stored in UTC. |
| `lookback`         | both           | str (timedelta) | Global default or per-call in `CacheHint`. |
| `conn_timeout`     | `DataBase`     | float (s)   | DB connect timeout; triggers fallback if cache exists. |
| `cache_key`        | `CacheHint`    | str         | Cache directory for derived/SQL queries. |
| `datetime_column`  | `CacheHint`    | str         | Time column from result (for SQL) or table (for table mode). |
| `range_param_names`| `CacheHint`    | (str, str)  | Names for `:start`, `:end`. |
| `cache_mode`       | `select()`     | str         | `"auto"` (default), `"force"` (cache requires window), `"none"` (no cache). |

---

## License

MIT
