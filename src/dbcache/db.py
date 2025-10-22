from __future__ import annotations
import typing as T
import os
import pandas as pd

from .utils import parse_lookback, normalize_ts, is_select_sql
from .lock import FileLock
from .cache import CacheManager, CacheHint
from .exceptions import SchemaMismatchError
from .adapters import PostgresAdapter

def _coverage_ok(df: pd.DataFrame, dt_col: str, start_ts: pd.Timestamp, end_ts: pd.Timestamp, lookback: pd.Timedelta) -> bool:
    if df.empty or dt_col not in df.columns:
        return False
    s = pd.to_datetime(df[dt_col], utc=True, errors="coerce")
    return (s.min() <= start_ts) and (s.max() >= (end_ts - lookback))

def _iter_days(start_ts: pd.Timestamp, end_ts: pd.Timestamp):
    cur = start_ts.normalize()
    while cur < end_ts:
        yield cur
        cur += pd.Timedelta(days=1)

def _iter_months(start_ts: pd.Timestamp, end_ts: pd.Timestamp):
    cur = pd.Timestamp(year=start_ts.year, month=start_ts.month, day=1, tz="UTC")
    while cur < end_ts:
        yield cur
        if cur.month == 12:
            cur = pd.Timestamp(year=cur.year + 1, month=1, day=1, tz="UTC")
        else:
            cur = pd.Timestamp(year=cur.year, month=cur.month + 1, day=1, tz="UTC")

def _group_consecutive(timestamps: list[pd.Timestamp], step: str) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    segs = []
    if not timestamps:
        return segs
    timestamps = sorted(timestamps)
    start = timestamps[0]
    prev = start
    for ts in timestamps[1:]:
        if step == "daily":
            expected = prev + pd.Timedelta(days=1)
        else:
            if prev.month == 12:
                expected = pd.Timestamp(year=prev.year + 1, month=1, day=1, tz="UTC")
            else:
                expected = pd.Timestamp(year=prev.year, month=prev.month + 1, day=1, tz="UTC")
        if ts != expected:
            segs.append((start, expected))
            start = ts
        prev = ts
    if step == "daily":
        segs.append((start, prev + pd.Timedelta(days=1)))
    else:
        if prev.month == 12:
            end = pd.Timestamp(year=prev.year + 1, month=1, day=1, tz="UTC")
        else:
            end = pd.Timestamp(year=prev.year, month=prev.month + 1, day=1, tz="UTC")
        segs.append((start, end))
    return segs

class DataBase:
    def __init__(
        self, url: str, port: int, user: str, password: str, name: str,
        path: str = "database/", db_type: str = "postgres", cache_format: str = "csv",
        compress: bool = False, datetime_tz: str | None = None, lookback: str = "15min",
        partitioning: str = "monthly", index_columns: list[str] | None = None,
        file_lock_timeout: float = 30.0, logger: T.Any | None = None,
        compression_level: int | None = None, parquet_codec: str | None = None,
    ):
        self.host = url
        self.port = int(port)
        self.user = user
        self.password = password
        self.dbname = name
        self.path = path
        self.db_type = db_type.lower()
        self.cache_format = cache_format
        self.compress = compress
        self.datetime_tz = datetime_tz
        self.lookback = parse_lookback(lookback)
        self.partitioning = partitioning
        self.index_columns = index_columns or []
        self.file_lock_timeout = file_lock_timeout
        self.logger = logger
        self.compression_level = compression_level
        self.parquet_codec = parquet_codec

        if self.db_type == "postgres":
            self.adapter = PostgresAdapter(self.host, self.port, self.user, self.password, self.dbname, self.datetime_tz)
        else:
            raise NotImplementedError(f"db_type '{self.db_type}' not supported.")

        if self.cache_format not in {"csv", "parquet"}:
            raise NotImplementedError("cache_format must be 'csv' or 'parquet'.")
        if self.cache_format == "parquet":
            try:
                import pyarrow  # noqa: F401
            except Exception as e:
                raise RuntimeError("To use cache_format='parquet', install 'pyarrow'.") from e
        self.cache = CacheManager(self.path, self.dbname, self.cache_format, self.compress, self.partitioning, self.compression_level, self.parquet_codec)

    def _get_cached_schema(self, base_root: str) -> list[str] | None:
        cat = self.cache.read_catalog(base_root)
        cols = cat.get("schema_columns")
        return list(cols) if cols else None

    def _set_cached_schema_if_absent(self, base_root: str, columns: list[str]) -> None:
        cat = self.cache.read_catalog(base_root)
        if "schema_columns" not in cat or not cat["schema_columns"]:
            cat["schema_columns"] = list(columns)
            self.cache.write_catalog(base_root, cat)

    def _ensure_schema_or_raise_select(self, base_root: str, sql: str, params: dict) -> list[str]:
        cached_cols = self._get_cached_schema(base_root)
        query_cols = self.adapter.peek_columns_sql(sql, params or {})
        if cached_cols is not None:
            if set(cached_cols) != set(query_cols):
                raise SchemaMismatchError(f"Cached schema {cached_cols} != query schema {query_cols}")
        else:
            self._set_cached_schema_if_absent(base_root, query_cols)
        return query_cols

    def _ensure_schema_or_raise_table(self, base_root: str, table: str, columns: list[str] | None) -> list[str]:
        cached_cols = self._get_cached_schema(base_root)
        intended_cols = columns if (columns is not None and len(columns) > 0) else None
        if intended_cols is None:
            if cached_cols is None:
                intended_cols = self.adapter.peek_table_columns(table, None)
            else:
                intended_cols = cached_cols
        if cached_cols is not None:
            if set(cached_cols) != set(intended_cols):
                raise SchemaMismatchError(f"Cached schema {cached_cols} != requested columns {intended_cols}")
        else:
            self._set_cached_schema_if_absent(base_root, list(intended_cols))
        return list(intended_cols)

    # ------------------------------- get_table -------------------------------
    def get_table(
        self, table: str, columns: list[str] | None, start: pd.Timestamp | str, end: pd.Timestamp | str,
        datetime_column: str, where: str | None = None, order_by: str | None = None
    ) -> pd.DataFrame:
        start_ts = normalize_ts(start, self.datetime_tz)
        end_ts = normalize_ts(end, self.datetime_tz)
        if start_ts >= end_ts:
            raise ValueError("start must be < end")

        base_root = self.cache.table_root(table)
        _ = self._ensure_schema_or_raise_table(base_root, table, columns)

        cached = self._read_partitions_window(base_root, start_ts, end_ts, usecols=columns)

        if _coverage_ok(cached, datetime_column, start_ts, end_ts, self.lookback):
            return self._filter_window(cached, datetime_column, start_ts, end_ts, order_by).reset_index(drop=True)

        missing_segments = []
        if self.partitioning == "daily":
            missing_days = []
            for day in _iter_days(start_ts, end_ts):
                part_path = self.cache.partition_path(base_root, day)
                if not os.path.exists(part_path):
                    missing_days.append(day)
            missing_segments = _group_consecutive(missing_days, "daily")
        elif self.partitioning == "monthly":
            missing_months = []
            for month in _iter_months(start_ts, end_ts):
                part_path = self.cache.partition_path(base_root, month)
                if not os.path.exists(part_path):
                    missing_months.append(month)
            missing_segments = _group_consecutive(missing_months, "monthly")

        if not missing_segments:
            if not cached.empty and datetime_column in cached.columns:
                cached_max = pd.to_datetime(cached[datetime_column], utc=True, errors="coerce").max()
                fetch_from = max(start_ts, cached_max - self.lookback)
            else:
                fetch_from = start_ts
            sql, params = self.adapter.build_incremental_query(
                table=table, columns=columns, start=fetch_from, end=end_ts,
                datetime_column=datetime_column, where=where, order_by=order_by,
            )
            self._ensure_only_select(sql)
            delta = self.adapter.execute_select(sql, params)
            merged = self._merge_and_dedupe(cached, delta, key_cols=self._dedup_keys(datetime_column))
            self._write_partitions_window(base_root, merged, datetime_column)
            return self._filter_window(merged, datetime_column, start_ts, end_ts, order_by).reset_index(drop=True)

        lb = self.lookback
        segs = []
        for s, e in missing_segments:
            seg_start = max(start_ts, s - lb)
            seg_end   = min(end_ts,   e + lb)
            if seg_start < seg_end:
                segs.append((seg_start, seg_end))
        sql, params = self.adapter.build_segmented_query(
            table=table, columns=columns, datetime_column=datetime_column,
            segments=segs, where=where, order_by=order_by,
        )
        self._ensure_only_select(sql)
        delta = self.adapter.execute_select(sql, params)
        merged = self._merge_and_dedupe(cached, delta, key_cols=self._dedup_keys(datetime_column))
        self._write_partitions_window(base_root, merged, datetime_column)
        return self._filter_window(merged, datetime_column, start_ts, end_ts, order_by).reset_index(drop=True)

    # -------------------------------- select ---------------------------------
    def select(
        self, sql: str, params: dict | list | tuple | None = None, *,
        cache_hint: CacheHint | None = None, cache_mode: str = "auto",
        order_by: str | None = None
    ) -> pd.DataFrame:
        sql = sql.strip()
        self._ensure_only_select(sql)
        if cache_mode not in {"auto","force","none"}:
            raise ValueError("cache_mode must be 'auto' | 'force' | 'none'")

        if cache_mode == "none" or cache_hint is None:
            return self.adapter.execute_select(sql, params or {})

        ch = cache_hint
        base_root = self.cache.derived_root(ch.cache_key)

        p = dict(params or {})
        if not isinstance(p, dict):
            raise ValueError("params must be dict when cache is enabled.")
        start_name, end_name = ch.range_param_names
        if start_name not in p or end_name not in p:
            if cache_mode == "force":
                raise ValueError(f"Cache in 'force' requires params '{start_name}' and '{end_name}'.")
            self._ensure_schema_or_raise_select(base_root, sql, p)
            return self.adapter.execute_select(sql, p)

        start_ts = normalize_ts(p[start_name], self.datetime_tz)
        end_ts = normalize_ts(p[end_name], self.datetime_tz)
        if start_ts >= end_ts:
            raise ValueError("start must be < end")

        _ = self._ensure_schema_or_raise_select(base_root, sql, p)

        cached = self._read_partitions_window(base_root, start_ts, end_ts, usecols=None)
        lb = parse_lookback(ch.lookback) if ch.lookback else self.lookback

        if _coverage_ok(cached, ch.datetime_column, start_ts, end_ts, lb):
            return self._filter_window(cached, ch.datetime_column, start_ts, end_ts, order_by).reset_index(drop=True)

        if ch.partitioning == "daily":
            missing_days = []
            for day in _iter_days(start_ts, end_ts):
                part_path = self.cache.partition_path(base_root, day)
                if not os.path.exists(part_path):
                    missing_days.append(day)
            missing_segments = _group_consecutive(missing_days, "daily")
        elif ch.partitioning == "monthly":
            missing_months = []
            for month in _iter_months(start_ts, end_ts):
                part_path = self.cache.partition_path(base_root, month)
                if not os.path.exists(part_path):
                    missing_months.append(month)
            missing_segments = _group_consecutive(missing_months, "monthly")
        else:
            missing_segments = []

        if not missing_segments:
            if not cached.empty and ch.datetime_column in cached.columns:
                cached_max = pd.to_datetime(cached[ch.datetime_column], utc=True, errors="coerce").max()
                fetch_from = max(start_ts, cached_max - lb)
            else:
                fetch_from = start_ts
            delta = self.adapter.execute_select(sql, p)
            if ch.datetime_column not in delta.columns:
                raise KeyError(f"Temporal column '{ch.datetime_column}' (CacheHint) not in result.")
            merged = self._merge_and_dedupe(cached, delta, key_cols=ch.index_columns or [])
            self._write_partitions_window(base_root, merged, ch.datetime_column)
            return self._filter_window(merged, ch.datetime_column, start_ts, end_ts, order_by).reset_index(drop=True)

        delta = self.adapter.execute_select(sql, p)
        if ch.datetime_column not in delta.columns:
            raise KeyError(f"Temporal column '{ch.datetime_column}' (CacheHint) not in result.")
        merged = self._merge_and_dedupe(cached, delta, key_cols=ch.index_columns or [])
        self._write_partitions_window(base_root, merged, ch.datetime_column)
        return self._filter_window(merged, ch.datetime_column, start_ts, end_ts, order_by).reset_index(drop=True)

    # ------------------------------- internals -------------------------------
    def _dedup_keys(self, datetime_column: str) -> list[str]:
        keys = list(self.index_columns) if self.index_columns else []
        if not keys:
            keys = [datetime_column]
        return keys

    def _ensure_only_select(self, sql: str) -> None:
        if not is_select_sql(sql):
            raise PermissionError("Only SELECT statements are allowed.")

    def _read_partitions_window(self, base_root: str, start_ts: pd.Timestamp, end_ts: pd.Timestamp, usecols: list[str] | None) -> pd.DataFrame:
        dfs = []
        if self.partitioning == "none":
            path = self.cache.partition_path(base_root, start_ts)
            with FileLock(path, timeout=self.file_lock_timeout):
                df = self.cache.read_df_if_exists(path, usecols=usecols)
            if not df.empty:
                dfs.append(df)
        elif self.partitioning == "daily":
            cur = start_ts.normalize()
            while cur < end_ts:
                path = self.cache.partition_path(base_root, cur)
                with FileLock(path, timeout=self.file_lock_timeout):
                    df = self.cache.read_df_if_exists(path, usecols=usecols)
                if not df.empty:
                    dfs.append(df)
                cur += pd.Timedelta(days=1)
        else:  # monthly
            cur = pd.Timestamp(year=start_ts.year, month=start_ts.month, day=1, tz="UTC")
            while cur < end_ts:
                path = self.cache.partition_path(base_root, cur)
                with FileLock(path, timeout=self.file_lock_timeout):
                    df = self.cache.read_df_if_exists(path, usecols=usecols)
                if not df.empty:
                    dfs.append(df)
                if cur.month == 12:
                    cur = pd.Timestamp(year=cur.year + 1, month=1, day=1, tz="UTC")
                else:
                    cur = pd.Timestamp(year=cur.year, month=cur.month + 1, day=1, tz="UTC")
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    def _write_partitions_window(self, base_root: str, df: pd.DataFrame, datetime_column: str) -> None:
        if df.empty:
            return
        if datetime_column not in df.columns:
            raise KeyError(f"Temporal column '{datetime_column}' not in DataFrame.")
        dft = df.copy()
        dft[datetime_column] = pd.to_datetime(dft[datetime_column], utc=True)

        cat = self.cache.read_catalog(base_root)
        if "schema_columns" not in cat or not cat["schema_columns"]:
            cat["schema_columns"] = list(dft.columns)
            self.cache.write_catalog(base_root, cat)

        if self.partitioning == "none":
            path = self.cache.partition_path(base_root, dft[datetime_column].min())
            with FileLock(path, timeout=self.file_lock_timeout):
                self.cache.write_df_atomic(path, dft.sort_values(by=datetime_column))
            return

        if self.partitioning == "daily":
            dft["_dkey"] = dft[datetime_column].dt.date
            for day, part in dft.groupby("_dkey"):
                ts = pd.Timestamp(day, tz="UTC")
                path = self.cache.partition_path(base_root, ts)
                with FileLock(path, timeout=self.file_lock_timeout):
                    old = self.cache.read_df_if_exists(path)
                    merged = self._merge_and_dedupe(old, part.drop(columns=["_dkey"]), key_cols=self.index_columns)
                    self.cache.write_df_atomic(path, merged.sort_values(by=datetime_column))
        else:
            s = pd.to_datetime(dft[datetime_column], utc=True, errors="coerce")
            dft["_mkey"] = s.dt.tz_localize(None).dt.to_period("M")
            for mkey, part in dft.groupby("_mkey"):
                ts = pd.Timestamp(mkey.start_time).tz_localize("UTC")
                path = self.cache.partition_path(base_root, ts)
                with FileLock(path, timeout=self.file_lock_timeout):
                    old = self.cache.read_df_if_exists(path)
                    merged = self._merge_and_dedupe(old, part.drop(columns=["_mkey"]), key_cols=self.index_columns)
                    self.cache.write_df_atomic(path, merged.sort_values(by=datetime_column))

    def _merge_and_dedupe(self, cached: pd.DataFrame, delta: pd.DataFrame, key_cols: list[str]) -> pd.DataFrame:
        if cached.empty:
            base = delta.copy()
        elif delta.empty:
            base = cached.copy()
        else:
            base = pd.concat([cached, delta], ignore_index=True)
        if not key_cols:
            return base
        return base.drop_duplicates(subset=key_cols, keep="last")

    def _filter_window(self, df: pd.DataFrame, datetime_column: str, start_ts: pd.Timestamp, end_ts: pd.Timestamp, order_by: str | None) -> pd.DataFrame:
        if df.empty:
            return df
        if datetime_column not in df.columns:
            return df
        dft = df.copy()
        dft[datetime_column] = pd.to_datetime(dft[datetime_column], utc=True)
        mask = (dft[datetime_column] >= start_ts.tz_convert("UTC")) & (dft[datetime_column] < end_ts.tz_convert("UTC"))
        dft = dft.loc[mask]
        if order_by:
            cols = [c.strip().split()[0] for c in order_by.split(",")]
            ascending = [("DESC" not in c.upper()) for c in order_by.split(",")]
            dft = dft.sort_values(by=cols, ascending=ascending)
        return dft
