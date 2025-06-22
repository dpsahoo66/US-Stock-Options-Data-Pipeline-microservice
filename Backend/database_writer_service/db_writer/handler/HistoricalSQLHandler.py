# HistoricalSQLHandler.py
import pyodbc, os, logging

logger = logging.getLogger(__name__)

class HistoricalSQLHandler:
    def __init__(self):
        conn_str = os.getenv("AZURE_SQL_CONNECTION_STRING")
        self.conn = pyodbc.connect(conn_str)
        self.cursor = self.conn.cursor()

    def write_data(self, data):
        for r in data:
            try:
                self.cursor.execute("""
                     INSERT INTO StockData (Date, StockName, [Open], High, Low, [Close], Volume)
                     VALUES (?, ?, ?, ?, ?, ?, ?)
                """,  r["datetime"], r["symbol"], r["open"], r["high"], r["low"], r["close"], r["volume"])
            except Exception as e:
                logger.error(f"HistoricalSQL insert error: {e}")
        self.conn.commit()
        logger.info(f"SQL Historical write: {len(data)} rows")

    def __del__(self):
        self.cursor.close()
        self.conn.close()
