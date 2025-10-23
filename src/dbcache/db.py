
from typing import Optional, List, Dict, Tuple, Any
import os
import pandas as pd

from .utils import parse_lookback, normalize_ts, is_select_sql
from .lock import FileLock
from .cache import CacheManager, CacheHint
from .exceptions import SchemaMismatchError
from .adapters import PostgresAdapter, MySQLAdapter

def _coverage_ok(df: pd.DataFrame, dt_col: str, start_ts: pd.Timestamp, end_ts: pd.Timestamp, lookback: pd.Timedelta) -> bool:
    if df.empty or dt_col not in df.columns: return False
    s = pd.to_datetime(df[dt_col], utc=True, errors='coerce')
    return (s.min() <= start_ts) and (s.max() >= (end_ts - lookback))

class DataBase:
    def __init__(self, url: str, port: int, user: str, password: str, name: str, path: str = 'database/', db_type: str = 'postgres', cache_format: str = 'csv', compress: bool = False, datetime_tz: Optional[str] = None, lookback: str = '15min', partitioning: str = 'monthly', index_columns: Optional[List[str]] = None, file_lock_timeout: float = 30.0, logger: Optional[Any] = None, compression_level: Optional[int] = None, parquet_codec: Optional[str] = None):
        self.host=url; self.port=int(port); self.user=user; self.password=password; self.dbname=name
        self.path=path; self.db_type=db_type.lower(); self.cache_format=cache_format; self.compress=compress
        self.datetime_tz=datetime_tz; self.lookback=parse_lookback(lookback); self.partitioning=partitioning
        self.index_columns=index_columns or []; self.file_lock_timeout=file_lock_timeout; self.logger=logger
        self.compression_level=compression_level; self.parquet_codec=parquet_codec

        if self.db_type=='postgres':
            self.adapter=PostgresAdapter(self.host,self.port,self.user,self.password,self.dbname,self.datetime_tz)
        elif self.db_type=='mysql':
            self.adapter=MySQLAdapter(self.host,self.port,self.user,self.password,self.dbname,self.datetime_tz)
        else:
            raise NotImplementedError(f"db_type '{self.db_type}' not supported.")

        if self.cache_format not in {'csv','parquet'}:
            raise NotImplementedError("cache_format must be 'csv' or 'parquet'.")
        if self.cache_format=='parquet':
            import pyarrow  # noqa: F401

        self.cache=CacheManager(self.path,self.dbname,self.cache_format,self.compress,self.partitioning,self.compression_level,self.parquet_codec)

    def _get_cached_schema(self, base_root: str):
        cat=self.cache.read_catalog(base_root); cols=cat.get('schema_columns'); return list(cols) if cols else None

    def _set_cached_schema_if_absent(self, base_root: str, columns: List[str]) -> None:
        cat=self.cache.read_catalog(base_root)
        if 'schema_columns' not in cat or not cat['schema_columns']:
            cat['schema_columns']=list(columns); self.cache.write_catalog(base_root, cat)

    def _ensure_schema_or_raise_select(self, base_root: str, sql: str, params: Dict) -> List[str]:
        cached=self._get_cached_schema(base_root)
        query_cols=self.adapter.peek_columns_sql(sql, params or {})
        if cached is not None:
            if set(cached)!=set(query_cols): raise SchemaMismatchError(f"Cached schema {cached} != query schema {query_cols}")
        else: self._set_cached_schema_if_absent(base_root, query_cols)
        return query_cols

    def _ensure_schema_or_raise_table(self, base_root: str, table: str, columns: Optional[List[str]]) -> List[str]:
        cached=self._get_cached_schema(base_root)
        intended=columns if (columns is not None and len(columns)>0) else None
        if intended is None:
            intended = self.adapter.peek_table_columns(table, None) if cached is None else cached
        if cached is not None:
            if set(cached)!=set(intended): raise SchemaMismatchError(f"Cached schema {cached} != requested columns {intended}")
        else: self._set_cached_schema_if_absent(base_root, list(intended))
        return list(intended)

    # -------- gap detection & filling --------
    def _segments_from_cache(self, df: pd.DataFrame, dt_col: str, start_ts: pd.Timestamp, end_ts: pd.Timestamp, max_segments: int) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
        if df.empty or dt_col not in df.columns:
            return [(start_ts, end_ts)]
        s = pd.to_datetime(df[dt_col], utc=True, errors='coerce')
        m = (s >= start_ts) & (s < end_ts)
        s = s[m].sort_values()
        holes = []
        cur = start_ts
        for ts in s.dropna().unique():
            ts = pd.Timestamp(ts).tz_convert('UTC')
            if ts > cur:
                holes.append((cur, ts))
                if len(holes) >= max_segments:
                    break
            cur = max(cur, ts)
        if cur < end_ts and len(holes) < max_segments:
            holes.append((cur, end_ts))
        holes = [(a,b) for (a,b) in holes if a < b]
        return holes

    def _fetch_fill_gaps(self, *, base_root: str, df_cached: pd.DataFrame, table: Optional[str], columns: Optional[List[str]], sql: Optional[str], params: Dict, dt_col: str, start_ts: pd.Timestamp, end_ts: pd.Timestamp, where: Optional[str], order_by: Optional[str], index_cols: List[str], max_segments: int) -> pd.DataFrame:
        segs = self._segments_from_cache(df_cached, dt_col, start_ts, end_ts, max_segments)
        if not segs:
            return df_cached

        if table is not None:
            seg_sql, seg_params = self.adapter.build_segmented_query(
                table=table, columns=columns, datetime_column=dt_col,
                segments=segs, where=where, order_by=order_by
            )
            delta = self.adapter.execute_select(seg_sql, seg_params)
        else:
            union_rows=[]; p=dict(params or {})
            for i,(a,b) in enumerate(segs):
                p[f"s{i}"]=a.to_pydatetime(); p[f"e{i}"]=b.to_pydatetime()
                union_rows.append(f"SELECT :s{i} AS s, :e{i} AS e")
            union_sql=" \nUNION ALL\n ".join(union_rows) if union_rows else "SELECT NULL AS s, NULL AS e LIMIT 0"
            if isinstance(self.adapter, MySQLAdapter):
                full_sql = "\n".join([
                    "SELECT sub.* FROM (", sql, ") sub",
                    "JOIN (", union_sql, ") AS segs",
                    f"ON sub.{dt_col} >= segs.s AND sub.{dt_col} < segs.e"
                ] + ([f"ORDER BY {order_by}"] if order_by else []))
            else:
                full_sql = "\n".join([
                    "WITH segs AS (", union_sql, ")",
                    "SELECT sub.* FROM (", sql, ") sub",
                    f"JOIN segs ON sub.{dt_col} >= segs.s AND sub.{dt_col} < segs.e"
                ] + ([f"ORDER BY {order_by}"] if order_by else []))
            delta = self.adapter.execute_select(full_sql, p)

        merged=self._merge_and_dedupe(df_cached, delta, key_cols=index_cols)
        self._write_partitions_window(base_root, merged, dt_col)
        return merged

    # -------------- public API --------------
    def get_table(self, table: str, columns: Optional[List[str]], start, end, datetime_column: str, where: Optional[str] = None, order_by: Optional[str] = None, *, fill_gaps: bool = False, max_segments: int = 24) -> pd.DataFrame:
        start_ts=normalize_ts(start, self.datetime_tz); end_ts=normalize_ts(end, self.datetime_tz)
        if start_ts>=end_ts: raise ValueError('start must be < end')
        base_root=self.cache.table_root(table)
        _=self._ensure_schema_or_raise_table(base_root, table, columns)

        cached=self._read_partitions_window(base_root, start_ts, end_ts, usecols=columns)
        if _coverage_ok(cached, datetime_column, start_ts, end_ts, self.lookback):
            return self._filter_window(cached, datetime_column, start_ts, end_ts, order_by).reset_index(drop=True)

        if fill_gaps:
            merged = self._fetch_fill_gaps(
                base_root=base_root, df_cached=cached, table=table, columns=columns, sql=None, params={},
                dt_col=datetime_column, start_ts=start_ts, end_ts=end_ts, where=where, order_by=order_by,
                index_cols=self._dedup_keys(datetime_column), max_segments=max_segments
            )
            return self._filter_window(merged, datetime_column, start_ts, end_ts, order_by).reset_index(drop=True)

        if not cached.empty and datetime_column in cached.columns:
            cached_max=pd.to_datetime(cached[datetime_column], utc=True, errors='coerce').max()
            fetch_from=max(start_ts, cached_max - self.lookback)
        else:
            fetch_from=start_ts

        sql, params = self.adapter.build_incremental_query(table, columns, fetch_from, end_ts, datetime_column, where, order_by)
        self._ensure_only_select(sql); delta=self.adapter.execute_select(sql, params)
        merged=self._merge_and_dedupe(cached, delta, key_cols=self._dedup_keys(datetime_column))
        self._write_partitions_window(base_root, merged, datetime_column)
        return self._filter_window(merged, datetime_column, start_ts, end_ts, order_by).reset_index(drop=True)

    def select(self, sql: str, params=None, *, cache_hint: Optional[CacheHint]=None, cache_mode: str='auto', order_by: Optional[str]=None) -> pd.DataFrame:
        sql=sql.strip(); self._ensure_only_select(sql)
        if cache_mode not in {'auto','force','none'}: raise ValueError("cache_mode must be 'auto','force','none'")
        if cache_mode=='none' or cache_hint is None: return self.adapter.execute_select(sql, params or {})

        ch=cache_hint; base_root=self.cache.derived_root(ch.cache_key)
        p=dict(params or {})
        if not isinstance(p, dict): raise ValueError('params must be dict when cache is enabled.')
        start_name, end_name = ch.range_param_names
        if start_name not in p or end_name not in p:
            if cache_mode=='force': raise ValueError("Cache in 'force' requires params '%s' and '%s'." % (start_name, end_name))
            self._ensure_schema_or_raise_select(base_root, sql, p); return self.adapter.execute_select(sql, p)

        start_ts=normalize_ts(p[start_name], self.datetime_tz); end_ts=normalize_ts(p[end_name], self.datetime_tz)
        if start_ts>=end_ts: raise ValueError('start must be < end')
        lb = parse_lookback(ch.lookback) if ch.lookback else self.lookback

        _=self._ensure_schema_or_raise_select(base_root, sql, p)
        cached=self._read_partitions_window(base_root, start_ts, end_ts, usecols=None)
        if _coverage_ok(cached, ch.datetime_column, start_ts, end_ts, lb):
            return self._filter_window(cached, ch.datetime_column, start_ts, end_ts, order_by).reset_index(drop=True)

        if ch.fill_gaps:
            merged = self._fetch_fill_gaps(
                base_root=base_root, df_cached=cached, table=None, columns=None, sql=sql, params=p,
                dt_col=ch.datetime_column, start_ts=start_ts, end_ts=end_ts, where=None, order_by=order_by,
                index_cols=ch.index_columns or [], max_segments=ch.max_segments
            )
            return self._filter_window(merged, ch.datetime_column, start_ts, end_ts, order_by).reset_index(drop=True)

        delta=self.adapter.execute_select(sql, p)
        if ch.datetime_column not in delta.columns: raise KeyError("Temporal column '%s' (CacheHint) not in result." % ch.datetime_column)
        merged=self._merge_and_dedupe(cached, delta, key_cols=ch.index_columns or [])
        self._write_partitions_window(base_root, merged, ch.datetime_column)
        return self._filter_window(merged, ch.datetime_column, start_ts, end_ts, order_by).reset_index(drop=True)

    # internals
    def _dedup_keys(self, datetime_column: str) -> List[str]:
        keys=list(self.index_columns) if self.index_columns else []
        if not keys: keys=[datetime_column]
        return keys

    def _ensure_only_select(self, sql: str) -> None:
        if not is_select_sql(sql): raise PermissionError('Only SELECT statements are allowed.')

    def _apply_order(self, df: pd.DataFrame, order_by: Optional[str], default_col: Optional[str] = None) -> pd.DataFrame:
        if not order_by:
            if default_col and default_col in df.columns:
                return df.sort_values(by=default_col, kind="mergesort")
            return df
        cols=[]; ascending=[]
        parts=[p.strip() for p in order_by.split(",") if p.strip()]
        for p in parts:
            tokens=p.split()
            col=tokens[0]
            asc=True
            if len(tokens)>1:
                dir=tokens[1].upper()
                if dir=="DESC": asc=False
            if col in df.columns:
                cols.append(col); ascending.append(asc)
        if cols:
            return df.sort_values(by=cols, ascending=ascending, kind="mergesort")
        return df

    def _filter_window(self, df: pd.DataFrame, dt_col: str, start_ts: pd.Timestamp, end_ts: pd.Timestamp, order_by: Optional[str]) -> pd.DataFrame:
        if df.empty: return df
        if dt_col not in df.columns: return df
        s = pd.to_datetime(df[dt_col], utc=True, errors='coerce')
        mask = (s >= start_ts) & (s < end_ts)
        out = df.loc[mask].copy()
        return self._apply_order(out, order_by, default_col=dt_col)

    def _merge_and_dedupe(self, cached: pd.DataFrame, delta: pd.DataFrame, key_cols: List[str]) -> pd.DataFrame:
        if cached is None or cached.empty:
            base = delta.copy()
        elif delta is None or delta.empty:
            base = cached.copy()
        else:
            all_cols = list({*cached.columns.tolist(), *delta.columns.tolist()})
            base = pd.concat([cached.reindex(columns=all_cols), delta.reindex(columns=all_cols)], ignore_index=True)
        if base.empty:
            return base
        if key_cols:
            key_cols=[c for c in key_cols if c in base.columns]
            if key_cols:
                return base.drop_duplicates(subset=key_cols, keep="last", ignore_index=True)
        return base.drop_duplicates(keep="last", ignore_index=True)

    def _read_partitions_window(self, base_root: str, start_ts: pd.Timestamp, end_ts: pd.Timestamp, usecols: Optional[List[str]]):
        dfs=[]
        if self.partitioning=='none':
            path=self.cache.partition_path(base_root, start_ts)
            with FileLock(path, timeout=self.file_lock_timeout): df=self.cache.read_df_if_exists(path, usecols=usecols)
            if not df.empty: dfs.append(df)
        elif self.partitioning=='daily':
            cur=start_ts.normalize()
            while cur<end_ts:
                path=self.cache.partition_path(base_root, cur)
                with FileLock(path, timeout=self.file_lock_timeout): df=self.cache.read_df_if_exists(path, usecols=usecols)
                if not df.empty: dfs.append(df)
                cur+=pd.Timedelta(days=1)
        else:
            cur=pd.Timestamp(year=start_ts.year, month=start_ts.month, day=1, tz='UTC')
            while cur<end_ts:
                path=self.cache.partition_path(base_root, cur)
                with FileLock(path, timeout=self.file_lock_timeout): df=self.cache.read_df_if_exists(path, usecols=usecols)
                if not df.empty: dfs.append(df)
                cur=pd.Timestamp(year=cur.year+1, month=1, day=1, tz='UTC') if cur.month==12 else pd.Timestamp(year=cur.year, month=cur.month+1, day=1, tz='UTC')
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    def _write_partitions_window(self, base_root: str, df: pd.DataFrame, datetime_column: str) -> None:
        if df.empty: return
        if datetime_column not in df.columns: raise KeyError("Temporal column '%s' not in DataFrame." % datetime_column)
        dft=df.copy(); dft[datetime_column]=pd.to_datetime(dft[datetime_column], utc=True)

        cat=self.cache.read_catalog(base_root)
        if 'schema_columns' not in cat or not cat['schema_columns']:
            cat['schema_columns']=list(dft.columns); self.cache.write_catalog(base_root, cat)

        if self.partitioning=='none':
            path=self.cache.partition_path(base_root, dft[datetime_column].min())
            with FileLock(path, timeout=self.file_lock_timeout): self.cache.write_df_atomic(path, dft.sort_values(by=datetime_column)); return

        if self.partitioning=='daily':
            dft['_dkey']=dft[datetime_column].dt.date
            for day, part in dft.groupby('_dkey'):
                ts=pd.Timestamp(day, tz='UTC'); path=self.cache.partition_path(base_root, ts)
                with FileLock(path, timeout=self.file_lock_timeout):
                    old=self.cache.read_df_if_exists(path)
                    merged=self._merge_and_dedupe(old, part.drop(columns=['_dkey']), key_cols=self.index_columns)
                    self.cache.write_df_atomic(path, merged.sort_values(by=datetime_column))
        else:
            s=pd.to_datetime(dft[datetime_column], utc=True, errors='coerce')
            s_naive = s.dt.tz_convert(None)
            dft['_mkey']=s_naive.dt.to_period('M')
            for mkey, part in dft.groupby('_mkey'):
                ts=pd.Timestamp(mkey.start_time).tz_localize('UTC'); path=self.cache.partition_path(base_root, ts)
                with FileLock(path, timeout=self.file_lock_timeout):
                    old=self.cache.read_df_if_exists(path)
                    merged=self._merge_and_dedupe(old, part.drop(columns=['_mkey']), key_cols=self.index_columns)
                    self.cache.write_df_atomic(path, merged.sort_values(by=datetime_column))
