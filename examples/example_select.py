
from dbcache import DataBase, CacheHint, enable_db_hit_prints, enable_full_sql_echo
from datetime import datetime, timezone

db = DataBase(url="host", port=5432, user="u", password="p", name="db",
              path="database/", db_type="postgres",
              cache_format="parquet", compress=True, parquet_codec="snappy",
              partitioning="monthly", index_columns=["id","dt"],
              datetime_tz="UTC", lookback="30min")

enable_db_hit_prints(db); enable_full_sql_echo(db)

sql = "SELECT id, dt FROM t WHERE dt >= :start AND dt < :end"
start = datetime(2025,1,1, tzinfo=timezone.utc)
end   = datetime(2025,2,1, tzinfo=timezone.utc)

df = db.select(sql, params={"start": start, "end": end},
               cache_hint=CacheHint(cache_key="t", datetime_column="dt"))
print(df.head())
