
from dataclasses import dataclass
from typing import Optional, List, Tuple
import os, json
import pandas as pd
from .utils import ensure_dir, month_key, day_key

@dataclass
class CacheHint:
    cache_key: str
    datetime_column: str
    range_param_names: Tuple[str, str] = ("start", "end")
    index_columns: Optional[List[str]] = None
    partitioning: str = "monthly"
    lookback: Optional[str] = None
    fill_gaps: bool = False
    max_segments: int = 24

class CacheManager:
    def __init__(self, root: str, dbname: str, cache_format: str, compress: bool, partitioning: str, compression_level: Optional[int] = None, parquet_codec: Optional[str] = None):
        self.root = os.path.abspath(root)
        self.dbname = dbname
        self.cache_format = cache_format
        self.compress = compress
        self.partitioning = partitioning
        self.compression_level = compression_level
        self.parquet_codec = parquet_codec or ("snappy" if compress else None)

    def table_root(self, table: str) -> str:
        p = os.path.join(self.root, self.dbname, table); ensure_dir(p); return p

    def derived_root(self, cache_key: str) -> str:
        p = os.path.join(self.root, self.dbname, "_derived", cache_key); ensure_dir(p); return p

    def catalog_path(self, base_root: str) -> str:
        return os.path.join(base_root, "_catalog.json")

    def _ext(self) -> str:
        if self.cache_format == "parquet": return ".parquet"
        return ".csv.gz" if self.compress else ".csv"

    def partition_path(self, base_root: str, ts: pd.Timestamp) -> str:
        ext = self._ext()
        if self.partitioning == "none":
            return os.path.join(base_root, "full" + ext)
        elif self.partitioning == "daily":
            sub = os.path.join(base_root, f"{ts.year:04d}"); ensure_dir(sub); return os.path.join(sub, f"{day_key(ts)}{ext}")
        else:
            sub = os.path.join(base_root, f"{ts.year:04d}"); ensure_dir(sub); return os.path.join(sub, f"{month_key(ts)}{ext}")

    def read_catalog(self, base_root: str) -> dict:
        path = self.catalog_path(base_root)
        if not os.path.exists(path): return {}
        with open(path, "r", encoding="utf-8") as f: return json.load(f)

    def write_catalog(self, base_root: str, data: dict) -> None:
        path = self.catalog_path(base_root); tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f: json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        os.replace(tmp, path)

    def read_df_if_exists(self, path: str, usecols: Optional[List[str]] = None) -> pd.DataFrame:
        if not os.path.exists(path): return pd.DataFrame()
        if self.cache_format == "parquet": return pd.read_parquet(path, columns=usecols)
        if path.endswith(".gz"): return pd.read_csv(path, usecols=usecols, compression="gzip")
        return pd.read_csv(path, usecols=usecols)

    def write_df_atomic(self, path: str, df: pd.DataFrame) -> None:
        tmp = path + ".tmp"
        if self.cache_format == "parquet":
            try: import pyarrow  # noqa: F401
            except Exception as e: raise RuntimeError("To use cache_format='parquet', install 'pyarrow'.") from e
            df.to_parquet(tmp, index=False, engine="pyarrow", compression=self.parquet_codec)
        else:
            if path.endswith(".gz"):
                comp = {"method": "gzip"}
                if self.compression_level is not None: comp["compresslevel"] = int(self.compression_level)
                df.to_csv(tmp, index=False, compression=comp)
            else:
                df.to_csv(tmp, index=False)
        os.replace(tmp, path)
