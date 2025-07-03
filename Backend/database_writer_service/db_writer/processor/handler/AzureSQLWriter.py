import pyodbc
import socket
import time
from django.conf import settings
from db_writer.utils.logConfig import LogConfig

logger = LogConfig()
class AzureSQLWriter:
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

    def write_stock_data(self, data):
        query = """
        INSERT INTO StockData (StockName, Date, [Open], High, Low, [Close], Volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        values = (
            data['symbol'],
            data['datetime'].split(' ')[0],
            float(data['open']),
            float(data['high']),
            float(data['low']),
            float(data['close']),
            int(data['volume'])
        )
        try:
            self.cursor.execute(query, values)
            self.conn.commit()
        except pyodbc.Error as e:
            logger.error(f"Failed to write stock data: {e}")
            self.connect()  # Reconnect on failure
            self.cursor.execute(query, values)
            self.conn.commit()

    def write_option_data(self, data):
        table = 'put_options' if data['type'].lower() == 'put' else 'call_options'
        query = f"""
        INSERT INTO {table} (
            contractSymbol, lastTradeDate, expirationDate, strike, lastPrice, bid, ask,
            change, percentChange, volume, openInterest, impliedVolatility, inTheMoney,
            contractSize, currency, StockName
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        values = (
            data.get('contractSymbol', 'UNKNOWN'),
            data['lastTradeDate'],
            data['expirationDate'],
            float(data['strike']),
            float(data.get('lastPrice')) if data.get('lastPrice') is not None else None,
            float(data.get('bid')) if data.get('bid') is not None else None,
            float(data.get('ask')) if data.get('ask') is not None else None,
            float(data.get('change')) if data.get('change') is not None else None,
            float(data.get('percentChange')) if data.get('percentChange') is not None else None,
            int(data.get('volume')) if data.get('volume') is not None else None,
            int(data.get('openInterest')) if data.get('openInterest') is not None else None,
            float(data.get('impliedVolatility')) if data.get('impliedVolatility') is not None else None,
            1 if data.get('inTheMoney', False) else 0,
            data.get('contractSize', 'REGULAR'),
            data.get('currency', 'USD'),
            data['symbol']
        )
        try:
            self.cursor.execute(query, values)
            self.conn.commit()
        except pyodbc.Error as e:
            logger.error(f"Failed to write option data to {table}: {e}")
            self.connect()  # Reconnect on failure
            self.cursor.execute(query, values)
            self.conn.commit()

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Azure SQL connection closed")