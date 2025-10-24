
from typing import Optional, List, Dict
import pandas as pd

class Adapter:
    def __init__(self, host: str, port: int, user: str, password: str, dbname: str, tz: Optional[str], conn_timeout: float = 10.0):
        self.host = host; self.port = port; self.user = user; self.password = password; self.dbname = dbname; self.tz = tz
        self.conn_timeout = float(conn_timeout)
    def connect(self): raise NotImplementedError
    def close(self, conn):
        try: conn.close()
        except Exception: pass
    def execute_select(self, sql: str, params: Optional[Dict] = None) -> pd.DataFrame: raise NotImplementedError
    def peek_table_columns(self, table: str, columns: Optional[List[str]]) -> List[str]: raise NotImplementedError
    def peek_columns_sql(self, sql: str, params: Optional[Dict] = None) -> List[str]: raise NotImplementedError
    def quote_ident(self, name: str) -> str: return f'"{name}"'
