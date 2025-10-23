
import re
from typing import Optional, List, Dict
import pandas as pd
from .base import Adapter
from ..utils import expand_in_clause, _escape_percent_literals_after_named, neutralize_named_params

def _to_named(sql: str) -> str:
    sql = sql.replace("::", "\x00")
    sql = re.sub(r":([a-zA-Z_]\w*)", r"%(\1)s", sql)
    sql = sql.replace("\x00", "::")
    return _escape_percent_literals_after_named(sql)

class MySQLAdapter(Adapter):
    def __init__(self, host: str, port: int, user: str, password: str, dbname: str, tz: Optional[str]):
        super().__init__(host, port, user, password, dbname, tz)
        self._driver = None
        self._connect_fn = None
        self._prepare_driver()

    def _prepare_driver(self):
        try:
            import pymysql  # type: ignore
            self._driver = "pymysql"; self._connect_fn = pymysql.connect; return
        except Exception:
            pass
        try:
            import MySQLdb  # type: ignore
            self._driver = "mysqldb"; self._connect_fn = MySQLdb.connect; return
        except Exception as e:
            raise RuntimeError("MySQL support requires: pip install 'pymysql' (default) OR pip install 'mysqlclient'") from e

    def connect(self):
        return self._connect_fn(host=self.host, port=self.port, user=self.user, passwd=self.password, db=self.dbname, charset="utf8mb4", autocommit=True)

    def execute_select(self, sql: str, params: Optional[Dict] = None) -> pd.DataFrame:
        sql, params = expand_in_clause(sql, params or {})
        sql = _to_named(sql)
        conn = self.connect()
        try:
            cur = conn.cursor()
            try:
                cur.execute(sql, params)
                cols = [d[0] for d in (cur.description or [])]
                rows = cur.fetchall()
            finally:
                cur.close()
            df = pd.DataFrame(rows, columns=cols)
            if self.tz:
                for c in df.columns:
                    if pd.api.types.is_datetime64_any_dtype(df[c]):
                        df[c] = pd.to_datetime(df[c], utc=True)
            return df
        finally:
            self.close(conn)

    def peek_table_columns(self, table: str, columns: Optional[List[str]]) -> List[str]:
        if columns: return list(columns)
        sql = f"SELECT * FROM {self.quote_ident(table)} LIMIT 0"
        conn = self.connect()
        try:
            cur = conn.cursor(); cur.execute(sql)
            return [d[0] for d in (cur.description or [])]
        finally:
            self.close(conn)

    def peek_columns_sql(self, sql: str, params: Optional[Dict] = None) -> List[str]:
        wrapped = "SELECT * FROM (" + sql + ") AS src LIMIT 0"
        wrapped, _ = expand_in_clause(wrapped, dict(params or {}))
        wrapped = neutralize_named_params(wrapped)
        conn = self.connect()
        cur = conn.cursor()
        cur.execute(wrapped)
        cols = [d[0] for d in (cur.description or [])]
        cur.close(); conn.close()
        return cols

    def build_segmented_query(self, table: str, columns: Optional[List[str]], datetime_column: str, segments, where: Optional[str], order_by: Optional[str]):
        cols = "*" if not columns else ", ".join([self.quote_ident(c) for c in columns])
        union_rows=[]; params={}
        for i,(s,e) in enumerate(segments):
            params[f"s{i}"]=s.isoformat(); params[f"e{i}"]=e.isoformat()
            union_rows.append(f"SELECT CAST(%(s{i})s AS DATETIME) AS s, CAST(%(e{i})s AS DATETIME) AS e")
        seg_sql=" \nUNION ALL\n ".join(union_rows) if union_rows else "SELECT NULL AS s, NULL AS e LIMIT 0"
        sql=[
            f"SELECT {cols} FROM {self.quote_ident(table)} t",
            "JOIN (", seg_sql, f") AS segs ON t.{datetime_column} >= segs.s AND t.{datetime_column} < segs.e"
        ]
        if where: sql.append("WHERE ("+where+")")
        if order_by: sql.append("ORDER BY "+order_by)
        return "\n".join(sql), params

    def quote_ident(self, name: str) -> str: return f"`{name}`"
