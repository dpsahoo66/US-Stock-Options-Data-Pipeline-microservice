# file_writer/management/commands/writer.py
from django.core.management.base import BaseCommand
from confluent_kafka import KafkaError
import json, time, logging

# from file_writer.handler.DailyDataFileHandler import DailyDataFileHandler
# from file_writer.handler.RealTimeDataFileHandler import RealTimeDataFileHandler
# from file_writer.handler.HistoricalDataFileHandler import HistoricalDataFileHandler
# from file_writer.handler.OptionsDataFileHandler import OptionsDataFileHandler

from file_writer.kafka import kafkaConfig
from django.conf import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = "Kafka to Influx + Azure SQL"

    def handle(self, *args, **kwargs):
        consumer = kafkaConfig.create_consumer()
        consumer.subscribe([
            settings.KAFKA_TOPICS['processed-file-daily'],
            settings.KAFKA_TOPICS['processed-file-15min'],
            settings.KAFKA_TOPICS['processed-file-options'],
            settings.KAFKA_TOPICS['processed-file-historical']
        ])

        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Kafka error: {msg.error()}")
                continue

            logger.info(f"Consumer Polling Message: {msg}")
            topic = msg.topic()
            raw_value = msg.value().decode('utf-8')

            try:
                data = json.loads(raw_value)
                logger.info(f"Consumed Data: {data}")

            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON: {e}")
                continue

            # try:
            #     if topic == settings.KAFKA_TOPICS['processed-file-daily']:
                                        
            #         DailyDataFileHandler(data)
                    
            #         logger.info(f"Daily data inserted succesfully ")

            #     elif topic == settings.KAFKA_TOPICS['processed-file-15min']:

            #         RealTimeDataFileHandler(data)

            #         logger.info(f"15 min data inserted succesfully ")

            #     elif topic == settings.KAFKA_TOPICS['processed-file-options']:

            #         OptionsDataFileHandler(data)

            #         logger.info(f"Option data inserted succesfully ")
            
            #     elif topic == settings.KAFKA_TOPICS['processed-file-historical']:

            #         HistoricalDataFileHandler(data)

            #         logger.info(f"historical data inserted succesfully ")

            # except Exception as e:
            #     logger.error(f"Processing error: {e}")