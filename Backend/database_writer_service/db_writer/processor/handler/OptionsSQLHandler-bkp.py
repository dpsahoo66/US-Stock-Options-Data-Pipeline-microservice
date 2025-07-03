# db_writer/handler/OptionsSQLHandler.py
import pyodbc, os
from db_writer.utils.logConfig import LogConfig

logger = LogConfig()

class OptionsSQLHandler:
    def __init__(self):
        self.conn = pyodbc.connect(os.getenv("AZURE_SQL_CONN"))
        self.cur = self.conn.cursor()

    def write_data(self, batch):
        for r in batch:
            table = "call_options" if r["optionType"].lower()=="call" else "put_options"
            try:
                self.cur.execute(f"""
                    INSERT INTO {table} (
                        contractSymbol, lastTradeDate, expirationDate, strike,
                        lastPrice, bid, ask, change, percentChange, volume,
                        openInterest, impliedVolatility, inTheMoney,
                        contractSize, currency, StockName
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, r["contractSymbol"], r["lastTradeDate"], r["expirationDate"], r["strike"],
                     r["lastPrice"], r["bid"], r["ask"], r["change"],
                     r["percentChange"], r["volume"], r["openInterest"],
                     r["impliedVolatility"], bool(r["inTheMoney"]), r["contractSize"],
                     r["currency"], r["symbol"])
            except Exception as e:
                logger.error(f"Option insert error: {e}")
        self.conn.commit()
        logger.info(f"SQL Options: inserted {len(batch)} rows")
