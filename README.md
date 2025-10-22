# dbcache (v0.2.5)

Instalation
```bash
pip install dbcache @ https://github.com/JorgeHSantana/dbcache.git
```

Example
```
from dbcache import DataBase, CacheHint, enable_db_hit_prints
from datetime import datetime, timezone

db = DataBase(
    url="host.com", port=5432, user="read_user", password="read_pass", name="my_db",
    path="database/", db_type="postgres",
    cache_format="parquet",
    compress=True, parquet_codec="snappy",
    compression_level=9,            # only CSV
    partitioning="monthly",
    index_columns=["id", "datetime"],
    datetime_tz="UTC",
    lookback="30min"
)

# (opcional) prints quando bater no banco
enable_db_hit_prints(db)

start = datetime(2025, 10, 1, 0, 0, 0, tzinfo=timezone.utc)
end   = datetime(2025, 10, 22, 0, 0, 0, tzinfo=timezone.utc)

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

df = db.select(
    sql,
    params={"start": start, "end": end},
    cache_hint=CacheHint(
        cache_key="my_table",
        datetime_column="datetime",
        range_param_names=("start","end"),
        index_columns=["id","datetime"],
        partitioning="monthly",
        lookback="30min",
    ),
    cache_mode="auto",
)

print(df.head())
```
