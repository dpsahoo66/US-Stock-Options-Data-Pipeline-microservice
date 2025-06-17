from confluent_kafka import Consumer, Producer, KafkaError
import json
from django.http import JsonResponse
import logging
from datetime import datetime, timezone

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'  # Update with your Kafka broker
CONSUMER_CONFIG = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'data_processor_group',
    'auto.offset.reset': 'earliest'
}
PRODUCER_CONFIG = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS
}

def create_consumer():
    """Create and return a Kafka consumer."""
    return Consumer(CONSUMER_CONFIG)

def create_producer():
    """Create and return a Kafka producer."""
    return Producer(PRODUCER_CONFIG)

def RealTimeDataProcessor(request):
    """
    Subscribes to '5min-data' topic, processes real-time 5-minute data,
    and pushes processed data to 'processed-5min-data' topic.
    """
    consumer = create_consumer()
    producer = create_producer()
    consumer.subscribe(['5min-data'])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                    return JsonResponse({
                        "status": "error",
                        "message": f"Kafka error: {msg.error()}"
                    }, status=500)

            try:
                data = json.loads(msg.value().decode('utf-8'))
                # Process real-time data
                processed_data = {
                    "symbol": data.get("meta", {}).get("symbol", "UNKNOWN"),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "latest_price": float(data.get("values", [{}])[0].get("close", 0)),
                    "average_volume": float(data.get("values", [{}])[0].get("volume", 0)),
                    "status": "processed"
                }
                
                # Push to new topic
                producer.produce(
                    'processed-5min-data',
                    value=json.dumps(processed_data).encode('utf-8')
                )
                producer.flush()
                logger.info(f"Processed real-time data for {processed_data['symbol']}")

            except Exception as e:
                logger.error(f"Error processing real-time data: {e}")
                continue

        return JsonResponse({
            "status": "success",
            "message": "Real-time data processing completed"
        }, status=200)

    except KeyboardInterrupt:
        logger.info("Real-time data processing interrupted")
        consumer.close()
        return JsonResponse({
            "status": "stopped",
            "message": "Real-time data processing stopped"
        }, status=200)

def DailyDataProcessor(request):
    """
    Subscribes to 'daily-data' topic, processes daily data,
    and pushes processed data to 'processed-daily-data' topic.
    """
    consumer = create_consumer()
    producer = create_producer()
    consumer.subscribe(['daily-data'])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                    return JsonResponse({
                        "status": "error",
                        "message": f"Kafka error: {msg.error()}"
                    }, status=500)

            try:
                data = json.loads(msg.value().decode('utf-8'))
                # Process daily data
                processed_data = {
                    "symbol": data.get("meta", {}).get("symbol", "UNKNOWN"),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "daily_close": float(data.get("values", [{}])[0].get("close", 0)),
                    "daily_volume": float(data.get("values", [{}])[0].get("volume", 0)),
                    "status": "processed"
                }
                
                # Push to new topic
                producer.produce(
                    'processed-daily-data',
                    value=json.dumps(processed_data).encode('utf-8')
                )
                producer.flush()
                logger.info(f"Processed daily data for {processed_data['symbol']}")

            except Exception as e:
                logger.error(f"Error processing daily data: {e}")
                continue

        return JsonResponse({
            "status": "success",
            "message": "Daily data processing completed"
        }, status=200)

    except KeyboardInterrupt:
        logger.info("Daily data processing interrupted")
        consumer.close()
        return JsonResponse({
            "status": "stopped",
            "message": "Daily data processing stopped"
        }, status=200)

def OptionDataProcessor(request):
    """
    Subscribes to 'options-data' topic, processes options data,
    and pushes processed data to 'processed-options-data' topic.
    """
    consumer = create_consumer()
    producer = create_producer()
    consumer.subscribe(['options-data'])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                    return JsonResponse({
                        "status": "error",
                        "message": f"Kafka error: {msg.error()}"
                    }, status=500)

            try:
                data = json.loads(msg.value().decode('utf-8'))
                # Process options data
                processed_data = {
                    "symbol": data.get("contractSymbol", "UNKNOWN"),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "strike": float(data.get("strike", 0)),
                    "last_price": float(data.get("lastPrice", 0)),
                    "expiration_date": data.get("expirationDate", ""),
                    "status": "processed"
                }
                
                # Push to new topic
                producer.produce(
                    'processed-options-data',
                    value=json.dumps(processed_data).encode('utf-8')
                )
                producer.flush()
                logger.info(f"Processed options data for {processed_data['symbol']}")

            except Exception as e:
                logger.error(f"Error processing options data: {e}")
                continue

        return JsonResponse({
            "status": "success",
            "message": "Options data processing completed"
        }, status=200)

    except KeyboardInterrupt:
        logger.info("Options data processing interrupted")
        consumer.close()
        return JsonResponse({
            "status": "stopped",
            "message": "Options data processing stopped"
        }, status=200)