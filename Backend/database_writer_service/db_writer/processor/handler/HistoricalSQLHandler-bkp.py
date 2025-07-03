# HistoricalSQLHandler.py
import pyodbc, os, logging
from db_writer.utils.logConfig import LogConfig

logger = LogConfig()

class HistoricalSQLHandler:
    def __init__(self):
        conn_str = os.getenv("AZURE_SQL_CONN")
        self.conn = pyodbc.connect(conn_str)
        self.cursor = self.conn.cursor()

    def write_data(self, batch):
        for r in batch:
            try:
                self.cursor.execute("""
                     INSERT INTO StockData (StockName, Date, [Open], High, Low, [Close], Volume)
                     VALUES (?, ?, ?, ?, ?, ?, ?)
                """, r["StockName"], r["Date"], r["open"], r["high"], r["low"], r["close"], r["volume"])
            except Exception as e:
                logger.error(f"HistoricalSQL insert error: {e}")
        self.conn.commit()
        logger.info(f"SQL Historical write: {len(batch)} rows")

    def __del__(self):
        self.cursor.close()
        self.conn.close()
