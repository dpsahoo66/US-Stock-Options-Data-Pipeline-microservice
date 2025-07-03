# db_writer/handler/InfluxHandler.py
from django.conf import settings
from influxdb_client import InfluxDBClient, Point, WritePrecision
import os, logging
from db_writer.utils.logConfig import LogConfig

logger = LogConfig()

class InfluxHandler:
    def __init__(self):
        self.client = InfluxDBClient(
            url=os.getenv("INFLUX_URL"),
            token=os.getenv("INFLUX_TOKEN"),
            org=os.getenv("INFLUX_ORG")
        )
        self.write_api = self.client.write_api(write_options=WritePrecision.NS)
        self.bucket = os.getenv("INFLUX_BUCKET")

    def write_data(self, batch):
        points = []
        for r in batch:
            try:
                p = Point("stock_15min")\
                    .tag("StockName", r["StockName"])\
                    .time(r["Date"], WritePrecision.NS)\
                    .field("open", float(r["open"]))\
                    .field("high", float(r["high"]))\
                    .field("low", float(r["low"]))\
                    .field("close", float(r["close"]))\
                    .field("volume", int(r["volume"]))\
                    
                points.append(p)
            except Exception as e:
                logger.warning(f"Skipping bad record: {e}")
        if points:
            self.write_api.write(bucket=self.bucket, record=points)
            logger.info(f"Influx: wrote {len(points)} points")
