# db_writer/management/commands/writer.py
from django.core.management.base import BaseCommand
from confluent_kafka import KafkaError
import json, time, logging

from db_writer.handler.InfluxHandler import InfluxHandler
from db_writer.handler.DailySQLHandler import DailySQLHandler
from db_writer.handler.HistoricalSQLHandler import HistoricalSQLHandler
from db_writer.handler.OptionsSQLHandler import OptionsSQLHandler
from db_writer.kafka import kafkaConfig
from django.conf import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = "Kafka to Influx + Azure SQL"

    def handle(self, *args, **kwargs):
        consumer = kafkaConfig.create_consumer()
        consumer.subscribe(list(settings.KAFKA_TOPICS.values()))

        influx = InfluxHandler()
        daily = DailySQLHandler()
        historical = HistoricalSQLHandler()
        options = OptionsSQLHandler()

        buffers = {t: [] for t in settings.KAFKA_TOPICS.values()}
        BATCH=8; FLUSH_SEC=10; last_flush=time.time()

        while True:
            msg = consumer.poll(1)
            now=time.time()
            if msg is None: pass
            elif msg.error(): logger.error(msg.error())
            else:
                t=msg.topic(); d=json.loads(msg.value().decode())
                buffers[t].append(d)

            for t, buf in buffers.items():
                if buf and (len(buf)>=BATCH or now-last_flush>FLUSH_SEC):
                    logger.info(f"FLUSH {t}, {len(buf)} items")
                    if t == settings.KAFKA_TOPICS['processed-15min']:
                        influx.write_data(buf)
                    elif t == settings.KAFKA_TOPICS['processed-daily']:
                        daily.write_data(buf)
                        historical.write_data(buf)
                    elif t == settings.KAFKA_TOPICS['processed-options']:
                        options.write_data(buf)
                    buffers[t] = []
            last_flush = now
