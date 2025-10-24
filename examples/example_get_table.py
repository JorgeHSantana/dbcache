
from dbcache import DataBase, enable_full_sql_echo, enable_db_hit_prints
from datetime import datetime, timezone

db = DataBase(url="host", port=3306, user="u", password="p", name="db",
              path="database/", db_type="mysql",
              cache_format="parquet", compress=True, parquet_codec="snappy",
              partitioning="monthly", index_columns=["id","dt"],
              datetime_tz="UTC", lookback="30min")

enable_db_hit_prints(db); enable_full_sql_echo(db)

start = datetime(2025,1,1, tzinfo=timezone.utc)
end   = datetime(2025,2,1, tzinfo=timezone.utc)

df = db.get_table("t", ["id","dt","v"], start, end, datetime_column="dt",
                  where="v > 0", order_by="dt", fill_gaps=True)
print(df.head())
