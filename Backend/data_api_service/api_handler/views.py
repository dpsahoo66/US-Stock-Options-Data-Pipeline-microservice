import pyodbc
import socket
import time
import logging
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.conf import settings

logger = logging.getLogger(__name__)

class CombinedStockDataView(APIView):
    def get_connection_string(self, db_config):
        return (
            f"Driver={db_config['OPTIONS']['driver']};"
            f"Server={db_config['OPTIONS']['host']};"
            f"Database={db_config['NAME']};"
            f"Uid={db_config['OPTIONS']['user']};"
            f"Pwd={db_config['OPTIONS']['password']};"
            f"{db_config['OPTIONS']['extra_params']};"
        )

    def connect_with_retry(self, conn_str, max_retries=5, retry_delay=20):
        driver = conn_str.split(';')[0].split('=')[1]
        logger.info(f"Trying connection with {driver}")
        for attempt in range(max_retries):
            try:
                logger.info(f"Connection attempt {attempt + 1}/{max_retries}")
                conn = pyodbc.connect(conn_str)
                return conn
            except pyodbc.Error as e:
                sqlstate = e.args[0]
                if sqlstate in ('08S01', '40001', '40197', '40501', '40613', '23000'):
                    logger.warning(f"Transient error: {e}. Retrying...")
                else:
                    logger.error(f"Non-transient error: {e}")
                    break
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.warning(f"Max retries reached for {driver}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                break
        raise Exception(f"Failed to connect with {driver}")

    def get(self, request):
        try:
            # Resolve DNS
            logger.info("Resolving server name...")
            server_ip = socket.gethostbyname("dash-gtd.database.windows.net")
            logger.info(f"Server resolved to IP: {server_ip}")

            # Connect to first database
            conn_str1 = self.get_connection_string(settings.DATABASES['default'])
            conn1 = self.connect_with_retry(conn_str1)
            cursor1 = conn1.cursor()

            # Verify tables
            cursor1.execute("SELECT 1 FROM sys.tables WHERE name = 'StockData'")
            if not cursor1.fetchone():
                raise Exception("StockData table not found in default DB")

            select_query = "SELECT StockName, Date, [Open], High, Low, [Close], Volume FROM StockData"
            data1 = []

            # Fetch from first database
            cursor1.execute(select_query)
            for row in cursor1.fetchall():
                data1.append({
                    'symbol': row.StockName,
                    'date': row.Date.isoformat().split('T')[0] if row.Date else None,
                    'open': float(row.Open) if row.Open is not None else None,
                    'high': float(row.High) if row.High is not None else None,
                    'low': float(row.Low) if row.Low is not None else None,
                    'close': float(row.Close) if row.Close is not None else None,
                    'volume': int(row.Volume) if row.Volume is not None else None
                })
            
            # select_option_query = "SELECT contractSymbol, lastTradeDate, expirationDate, strike, lastPrice, bid, ask, change, percentChange, volume, openInterest, impliedVolatility, inTheMoney, contractSize, currency, StockName FROM call_options"
            # data1 = []

            # cursor1.execute(select_query)
            # for row in cursor1.fetchall():
            #     data1.append({
            #         'symbol': row.StockName,
            #         'date': row.Date.isoformat().split('T')[0] if row.Date else None,
            #         'open': float(row.Open) if row.Open is not None else None,
            #         'high': float(row.High) if row.High is not None else None,
            #         'low': float(row.Low) if row.Low is not None else None,
            #         'close': float(row.Close) if row.Close is not None else None,
            #         'volume': int(row.Volume) if row.Volume is not None else None
            #     })

            # Combine data
            response_data = {
                'status': 'success',
                'data': {
                    'stock_data': data1,
                    'total_db1_rows': len(data1)
                }
            }

            return Response(response_data, status=status.HTTP_200_OK)

        except pyodbc.Error as e:
            logger.error(f"Database error: {e}")
            return Response({
                'status': 'error',
                'message': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        except socket.gaierror as e:
            logger.error(f"DNS resolution error: {e}")
            return Response({
                'status': 'error',
                'message': 'DNS resolution failed'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        except Exception as e:
            logger.error(f"General error: {e}")
            return Response({
                'status': 'error',
                'message': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            for cursor, conn in [(cursor1, conn1)]:
                if cursor:
                    try:
                        cursor.close()
                    except pyodbc.Error as e:
                        logger.error(f"Error closing cursor: {e}")
                if conn:
                    try:
                        conn.close()
                    except pyodbc.Error as e:
                        logger.error(f"Error closing connection: {e}")