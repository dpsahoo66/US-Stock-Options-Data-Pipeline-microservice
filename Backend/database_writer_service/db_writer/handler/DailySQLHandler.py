# db_writer/handler/DailySQLHandler.py

from django.conf import settings
import pyodbc
import logging

logger = logging.getLogger(__name__)

class DailySQLHandler:
    def __init__(self):
        conn_str = settings.AZURE_SQL_CONN
        self.conn = pyodbc.connect(conn_str)
        self.cursor = self.conn.cursor()

    def write_data(self, batch):
        for r in batch:
            try:
                self.cursor.execute("""
                    INSERT INTO StockData (Date, StockName, [Open], High, Low, [Close], Volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, r["datetime"], r["symbol"], r["open"], r["high"], r["low"], r["close"], r["volume"])
            except Exception as e:
                logger.error(f"DailySQL insert error: {e}")
        self.conn.commit()
        logger.info(f"SQL Daily write: {len(batch)} rows")

    def __del__(self):
        self.cursor.close()
        self.conn.close()
