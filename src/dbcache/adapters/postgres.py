
from typing import Optional, List, Dict
import pandas as pd
from ..utils import expand_in_clause, to_psycopg_named, neutralize_named_params
from .base import Adapter

class PostgresAdapter(Adapter):
    def __init__(self, host: str, port: int, user: str, password: str, dbname: str, tz: Optional[str]):
        super().__init__(host, port, user, password, dbname, tz)
        self._driver = None; self._connect_fn = None; self._prepare_driver()

    def _prepare_driver(self):
        try:
            import psycopg  # type: ignore
            self._driver = "psycopg"; self._connect_fn = psycopg.connect; return
        except Exception: pass
        try:
            import psycopg2  # type: ignore
            self._driver = "psycopg2"; self._connect_fn = psycopg2.connect; return
        except Exception as e:
            raise RuntimeError("Install 'psycopg' or 'psycopg2' for Postgres.") from e

    def connect(self):
        conn = self._connect_fn(host=self.host, port=self.port, user=self.user, password=self.password, dbname=self.dbname, application_name="dbcache")
        try:
            if self._driver == "psycopg":
                conn.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
            else:
                with conn.cursor() as cur: cur.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
        except Exception: pass
        return conn

    def execute_select(self, sql: str, params: Optional[Dict] = None) -> pd.DataFrame:
        sql, params = expand_in_clause(sql, params or {}); sql = to_psycopg_named(sql)
        conn = self.connect()
        try:
            if self._driver == "psycopg":
                with conn.cursor() as cur: cur.execute(sql, params); cols = [d[0] for d in cur.description]; rows = cur.fetchall()
            else:
                with conn.cursor() as cur: cur.execute(sql, params); cols = [d[0] for d in cur.description]; rows = cur.fetchall()
            df = pd.DataFrame(rows, columns=cols)
            if self.tz:
                for c in df.columns:
                    if pd.api.types.is_datetime64_any_dtype(df[c]):
                        df[c] = pd.to_datetime(df[c], utc=True).dt.tz_convert("UTC")
            return df
        finally:
            self.close(conn)

    def peek_table_columns(self, table: str, columns: Optional[List[str]]) -> List[str]:
        if columns: return list(columns)
        sql = f"SELECT * FROM {self.quote_ident(table)} LIMIT 0"
        conn = self.connect()
        try:
            if self._driver == "psycopg":
                with conn.cursor() as cur: cur.execute(sql); return [d[0] for d in cur.description]
            else:
                with conn.cursor() as cur: cur.execute(sql); return [d[0] for d in cur.description]
        finally:
            self.close(conn)

    def peek_columns_sql(self, sql: str, params: Optional[Dict] = None) -> List[str]:
        wrapped = "WITH src AS ( " + sql + " ) SELECT * FROM src LIMIT 0"
        wrapped, _ = expand_in_clause(wrapped, dict(params or {}))
        wrapped = neutralize_named_params(wrapped)
        conn = self.connect()
        try:
            if self._driver == "psycopg":
                with conn.cursor() as cur: cur.execute(wrapped); return [d[0] for d in cur.description]
            else:
                with conn.cursor() as cur: cur.execute(wrapped); return [d[0] for d in cur.description]
        finally:
            self.close(conn)

    def build_segmented_query(self, table: str, columns: Optional[List[str]], datetime_column: str, segments, where: Optional[str], order_by: Optional[str]):
        cols = "*" if not columns else ", ".join([self.quote_ident(c) for c in columns])
        sql = [
            "WITH segs AS (",
            "  SELECT",
            "    unnest(%(starts)s)::timestamptz AS s,",
            "    unnest(%(ends)s)::timestamptz   AS e",
            ")",
            f"SELECT {cols} FROM {self.quote_ident(table)} t",
            f"JOIN segs ON t.{datetime_column} >= segs.s AND t.{datetime_column} < segs.e",
        ]
        if where: sql.append("WHERE (" + where + ")")
        if order_by: sql.append("ORDER BY " + order_by)
        params = {"starts": [s.to_pydatetime() for s, _ in segments], "ends": [e.to_pydatetime() for _, e in segments]}
        return "\n".join(sql), params
