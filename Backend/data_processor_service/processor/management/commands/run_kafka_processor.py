from django.core.management.base import BaseCommand
from confluent_kafka import KafkaError
import json
import logging
from processor.kafka import kafkaConfig
from django.conf import settings

from processor.handler import DailyDataProcessor, RealTimeDataProcessor, OptionDataProcessor

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
            settings.KAFKA_TOPICS['options']
        ])

        logger.info("Kafka processor started. Listening for messages...")

        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Kafka error: {msg.error()}")
                continue

            topic = msg.topic()
            raw_value = msg.value().decode('utf-8')

            try:
                data = json.loads(raw_value)
                print(data)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON: {e}")
                continue

            try:
                if topic == settings.KAFKA_TOPICS['daily']:
                    processed = DailyDataProcessor(data)
                    producer.produce(settings.KAFKA_TOPICS['processed-daily'], value=json.dumps(processed).encode('utf-8'))

                elif topic == settings.KAFKA_TOPICS['15min']:
                    processed = RealTimeDataProcessor(data)
                    producer.produce(settings.KAFKA_TOPICS['processed-15min'], value=json.dumps(processed).encode('utf-8'))

                elif topic == settings.KAFKA_TOPICS['options']:
                    for record in data:
                        processed = OptionDataProcessor(record)
                        producer.produce(settings.KAFKA_TOPICS['processed-options'], value=json.dumps(processed).encode('utf-8'))

                producer.flush()
            except Exception as e:
                logger.error(f"Processing error: {e}")
