from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from django.conf import settings
from dateutil import parser
from datetime import timezone
from db_writer.utils.logConfig import LogConfig

logger = LogConfig()

class InfluxDBWriter:
    def __init__(self):
        self.client = InfluxDBClient(
            url=settings.INFLUXDB_URL,
            token=settings.INFLUXDB_TOKEN,
            org=settings.INFLUXDB_ORG
        )
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    def write_15min_data(self, data):
        timestamp = parser.parse(data['datetime']).replace(tzinfo=timezone.utc)
        point = Point("stock_15min") \
            .tag("symbol", data['symbol']) \
            .field("open", float(data['open'])) \
            .field("high", float(data['high'])) \
            .field("low", float(data['low'])) \
            .field("close", float(data['close'])) \
            .field("volume", int(data['volume'])) \
            .time(timestamp, WritePrecision.NS)
        try:
            self.write_api.write(bucket=settings.INFLUXDB_BUCKET, record=point)
        except Exception as e:
            logger.error(f"Failed to write to InfluxDB: {e}")
            raise

    def close(self):
        self.client.close()
        logger.info("InfluxDB connection closed")