from __future__ import annotations
import pandas as pd

class Adapter:
    def __init__(self, host: str, port: int, user: str, password: str, dbname: str, tz: str | None):
        self.host=host; self.port=port; self.user=user; self.password=password; self.dbname=dbname; self.tz=tz
    def connect(self): raise NotImplementedError
    def close(self, conn): 
        try: conn.close()
        except Exception: pass
    def execute_select(self, sql: str, params: dict | None = None) -> pd.DataFrame: raise NotImplementedError
    def peek_table_columns(self, table: str, columns: list[str] | None) -> list[str]: raise NotImplementedError
    def peek_columns_sql(self, sql: str, params: dict | None = None) -> list[str]: raise NotImplementedError

    def build_incremental_query(self, table: str, columns: list[str] | None, start: pd.Timestamp, end: pd.Timestamp, datetime_column: str, where: str | None, order_by: str | None) -> tuple[str, dict]:
        cols = "*" if not columns else ", ".join([self.quote_ident(c) for c in columns])
        sql = ["SELECT", cols, "FROM", self.quote_ident(table), "WHERE", f"{self.quote_ident(datetime_column)} >= :start AND {self.quote_ident(datetime_column)} < :end"]
        if where: sql.extend(["AND", where])
        if order_by: sql.extend(["ORDER BY", order_by])
        return " ".join(sql), {"start": start.to_pydatetime(), "end": end.to_pydatetime()}

    def build_segmented_query(self, table: str, columns: list[str] | None, datetime_column: str, segments: list[tuple[pd.Timestamp, pd.Timestamp]], where: str | None, order_by: str | None) -> tuple[str, dict]:
        cols = "*" if not columns else ", ".join([self.quote_ident(c) for c in columns])
        dc = self.quote_ident(datetime_column)
        ors=[]; params={}
        for i,(s,e) in enumerate(segments):
            params[f"s{i}"]=s.to_pydatetime(); params[f"e{i}"]=e.to_pydatetime()
            ors.append(f"({dc} >= :s{i} AND {dc} < :e{i})")
        where_parts = []
        if ors: where_parts.append("(" + " OR ".join(ors) + ")")
        if where: where_parts.append(f"({where})")
        where_sql = " AND ".join(where_parts) if where_parts else "TRUE"
        sql = f"SELECT {cols} FROM {self.quote_ident(table)} WHERE {where_sql}"
        if order_by: sql += f" ORDER BY {order_by}"
        return sql, params

    def quote_ident(self, name: str) -> str: return f'"{name}"'
