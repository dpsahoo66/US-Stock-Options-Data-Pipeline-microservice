# db_writer/handler/OptionsSQLHandler.py
import pyodbc
import socket
import time
import logging
from django.conf import settings

logger = logging.getLogger(__name__)

class OptionsSQLHandler:
    def __init__(self):
        self.conn_strings = [
            settings.AZURE_SQL_CONNECTION_STRING,
            settings.AZURE_SQL_CONNECTION_STRING.replace("ODBC Driver 18", "ODBC Driver 17")
        ]
        self.max_retries = 5
        self.retry_delay = 20
        self.conn = None
        self.cursor = None
        self.connect()

    def connect(self):
        for conn_str in self.conn_strings:
            driver = conn_str.split(';')[0].split('=')[1]
            logger.info(f"Trying connection with {driver}")
            for attempt in range(self.max_retries):
                try:
                    logger.info(f"Connection attempt {attempt + 1}/{self.max_retries}")
                    server_ip = socket.gethostbyname("dash-gtd.database.windows.net")
                    logger.info(f"Server resolved to IP: {server_ip}")
                    self.conn = pyodbc.connect(conn_str)
                    self.cursor = self.conn.cursor()
                    self.cursor.execute("SELECT 1 FROM sys.tables WHERE name = 'StockData'")
                    if not self.cursor.fetchone():
                        raise Exception("StockData table does not exist")
                    logger.info("Connected successfully")
                    return
                except (pyodbc.Error, socket.gaierror) as e:
                    logger.error(f"Connection failed: {e}")
                    if attempt < self.max_retries - 1:
                        logger.info(f"Retrying in {self.retry_delay} seconds...")
                        time.sleep(self.retry_delay)
                logger.info(f"Failed with {driver}, trying next driver if available")
        raise Exception("All connection attempts failed")

    def write_data(self, data):
        i = 0
        for data_val in data:
            logger.info(data_val["symbol"])
            i+=1

            # table = 'put_options' if data_val['type'].lower() == 'puts' else 'call_options'
            # query = f"""
            # INSERT INTO {table} (
            #     contractSymbol, lastTradeDate, expirationDate, strike, lastPrice, bid, ask,
            #     change, percentChange, volume, openInterest, impliedVolatility, inTheMoney,
            #     contractSize, currency, StockName
            # )
            # VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            # """
            # values = (
            #     data_val.get('contractSymbol', 'UNKNOWN'),
            #     data_val['lastTradeDate'],
            #     data_val['expirationDate'],
            #     float(data_val['strike']),
            #     float(data_val.get('lastPrice')) if data_val.get('lastPrice') is not None else None,
            #     float(data_val.get('bid')) if data_val.get('bid') is not None else None,
            #     float(data_val.get('ask')) if data_val.get('ask') is not None else None,
            #     float(data_val.get('change')) if data_val.get('change') is not None else None,
            #     float(data_val.get('percentChange')) if data_val.get('percentChange') is not None else None,
            #     int(data_val.get('volume')) if data_val.get('volume') is not None else None,
            #     int(data_val.get('openInterest')) if data_val.get('openInterest') is not None else None,
            #     float(data_val.get('impliedVolatility')) if data_val.get('impliedVolatility') is not None else None,
            #     1 if data_val.get('inTheMoney', False) else 0,
            #     data_val.get('contractSize', 'REGULAR'),
            #     data_val.get('currency', 'USD'),
            #     data_val['symbol']
            # )
            # try:
            #     self.cursor.execute(query, values)
            #     self.conn.commit()
            # except pyodbc.Error as e:
            #     logger.error(f"Failed to write option data to {table}: {e}")
            #     self.connect()
            #     self.cursor.execute(query, values)
            #     self.conn.commit()

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Azure SQL connection closed")