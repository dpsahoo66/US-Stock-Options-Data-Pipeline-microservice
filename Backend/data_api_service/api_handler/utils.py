import pyodbc
import socket
import time
import logging

logger = logging.getLogger(__name__)

def get_connection_string(db_config):
    return (
        f"Driver={db_config['OPTIONS']['driver']};"
        f"Server={db_config['OPTIONS']['host']};"
        f"Database={db_config['NAME']};"
        f"Uid={db_config['OPTIONS']['user']};"
        f"Pwd={db_config['OPTIONS']['password']};"
        f"{db_config['OPTIONS']['extra_params']};"
    )

def connect_with_retry(conn_str, max_retries=5, retry_delay=20):
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