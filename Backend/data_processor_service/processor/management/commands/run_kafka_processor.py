from django.core.management.base import BaseCommand
from confluent_kafka import KafkaError
import json
import logging
from processor.kafka import kafkaConfig
from django.conf import settings

from processor.handler.DailyDataProcessor import DailyDataProcessor
from processor.handler.RealTimeDataProcessor import RealTimeDataProcessor
from processor.handler.OptionDataProcessor import OptionDataProcessor
from processor.handler.HistoricalDataProcessor import HistoricalDataProcessor

import logging
# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Kafka consumer that processes and republishes messages'

    def handle(self, *args, **options):
        consumer = kafkaConfig.create_consumer()
        producer = kafkaConfig.create_producer()

        consumer.subscribe([
            settings.KAFKA_TOPICS['daily'],
            settings.KAFKA_TOPICS['15min'],
            settings.KAFKA_TOPICS['options'],
            settings.KAFKA_TOPICS['historical']
        ])

        logger.info("Kafka processor started. Listening for messages...")
        logger.info(f"INSIDE DATA PROCESSOR")

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

            try:
                if topic == settings.KAFKA_TOPICS['daily']:
                                        
                    processed = DailyDataProcessor(data)
                    
                    data_value = json.dumps(processed).encode('utf-8')

                    logger.info(f"Passing processed data to processed-daily Producer: {data_value}")

                    producer.produce(settings.KAFKA_TOPICS['processed-daily'], value=data_value)

                    logger.info(f"Passing processed data to processed-file-daily Producer: {data_value}")

                    producer.produce(settings.KAFKA_TOPICS['processed-file-daily'], value=data_value)


                elif topic == settings.KAFKA_TOPICS['15min']:

                    processed = RealTimeDataProcessor(data)

                    data_value = json.dumps(processed).encode('utf-8')

                    logger.info(f"Passing processed data to processed-15min Producer: {data_value}")

                    producer.produce(settings.KAFKA_TOPICS['processed-15min'], value=data_value)

                    logger.info(f"Passing processed data to processed-file-15min Producer: {data_value}")

                    producer.produce(settings.KAFKA_TOPICS['processed-file-15min'], value=data_value)


                elif topic == settings.KAFKA_TOPICS['options']:

                    processed = OptionDataProcessor(data)

                    data_value = json.dumps(processed).encode('utf-8')

                    logger.info(f"Passing processed data to processed-options Producer: {data_value}")

                    producer.produce(settings.KAFKA_TOPICS['processed-options'], value=data_value)

                    logger.info(f"Passing processed data to processed-file-options Producer: {data_value}")

                    producer.produce(settings.KAFKA_TOPICS['processed-file-options'], value=data_value)

                
                elif topic == settings.KAFKA_TOPICS['historical']:

                    processed = HistoricalDataProcessor(data)

                    data_value = json.dumps(processed).encode('utf-8')

                    logger.info(f"Passing processed data to processed-historical Producer: {data_value}")

                    producer.produce(settings.KAFKA_TOPICS['processed-historical'], value=data_value)

                    logger.info(f"Passing processed data to processed-file-historical Producer: {data_value}")

                    producer.produce(settings.KAFKA_TOPICS['processed-file-historical'], value=data_value)


                producer.flush()

            except Exception as e:
                logger.error(f"Processing error: {e}")
