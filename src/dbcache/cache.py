from __future__ import annotations
from dataclasses import dataclass
import os, json, pandas as pd
from .utils import ensure_dir, month_key, day_key

@dataclass
class CacheHint:
    cache_key: str
    datetime_column: str
    range_param_names: tuple[str, str] = ("start", "end")
    index_columns: list[str] | None = None
    partitioning: str = "monthly"
    lookback: str | None = None

class CacheManager:
    def __init__(self, root: str, dbname: str, cache_format: str, compress: bool, partitioning: str, compression_level: int | None = None, parquet_codec: str | None = None):
        self.root = os.path.abspath(root); self.dbname = dbname
        self.cache_format = cache_format; self.compress = compress; self.partitioning = partitioning
        self.compression_level = compression_level; self.parquet_codec = parquet_codec or ("snappy" if compress else None)
    def table_root(self, table: str) -> str:
        p = os.path.join(self.root, self.dbname, table); ensure_dir(p); return p
    def derived_root(self, cache_key: str) -> str:
        p = os.path.join(self.root, self.dbname, "_derived", cache_key); ensure_dir(p); return p
    def catalog_path(self, base_root: str) -> str: return os.path.join(base_root, "_catalog.json")
    def _ext(self) -> str: return ".parquet" if self.cache_format=="parquet" else (".csv.gz" if self.compress else ".csv")
    def partition_path(self, base_root: str, ts: pd.Timestamp) -> str:
        ext = self._ext()
        if self.partitioning=="none": return os.path.join(base_root, "full"+ext)
        elif self.partitioning=="daily":
            sub = os.path.join(base_root, f"{ts.year:04d}"); ensure_dir(sub); return os.path.join(sub, f"{ts.year:04d}-{ts.month:02d}-{ts.day:02d}{ext}")
        else:
            sub = os.path.join(base_root, f"{ts.year:04d}"); ensure_dir(sub); return os.path.join(sub, f"{ts.year:04d}-{ts.month:02d}{ext}")
    def read_catalog(self, base_root: str) -> dict:
        path = self.catalog_path(base_root)
        if not os.path.exists(path): return {}
        with open(path,"r",encoding="utf-8") as f: return json.load(f)
    def write_catalog(self, base_root: str, data: dict) -> None:
        path = self.catalog_path(base_root); tmp = path + ".tmp"
        with open(tmp,"w",encoding="utf-8") as f: json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        os.replace(tmp, path)
    def read_df_if_exists(self, path: str, usecols: list[str] | None = None) -> pd.DataFrame:
        if not os.path.exists(path): return pd.DataFrame()
        if self.cache_format=="parquet": return pd.read_parquet(path, columns=usecols)
        if path.endswith(".gz"): return pd.read_csv(path, usecols=usecols, compression="gzip")
        return pd.read_csv(path, usecols=usecols)
    def write_df_atomic(self, path: str, df: pd.DataFrame) -> None:
        tmp = path + ".tmp"
        if self.cache_format=="parquet":
            try: import pyarrow  # noqa: F401
            except Exception as e: raise RuntimeError("To use cache_format='parquet', install 'pyarrow'.") from e
            compression = self.parquet_codec; df.to_parquet(tmp, index=False, engine="pyarrow", compression=compression)
        else:
            if path.endswith(".gz"):
                comp={"method":"gzip"}; 
                # compression_level handled by writer of DataBase when constructing manager; here we don't have it, keep default
                df.to_csv(tmp, index=False, compression=comp)
            else: df.to_csv(tmp, index=False)
        os.replace(tmp, path)
