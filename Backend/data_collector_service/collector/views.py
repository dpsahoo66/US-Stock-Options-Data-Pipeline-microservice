import os
import json
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta

from django.http import JsonResponse
from django.conf import settings
from collector.utils.logConfig import LogConfig
from collector.kafka.kafkaConfig import create_producer
from collector.utils.symbols import SYMBOL_SETS

# Configure logging
logger = LogConfig()
NUM_SET = len(SYMBOL_SETS)

def fetch_each_day_data(request):
    logger.info("API triggered for fetching daily data")
    try:
        producer = create_producer()
        try:
            trigger_time = datetime.now(ZoneInfo("UTC")).strftime('%Y-%m-%d %H:%M:%S')
            # Publish a trigger message for each set_id(partitions)
            for set_id in range(NUM_SET):
                trigger_data = json.dumps({"set_id": set_id, "timestamp": trigger_time}).encode('utf-8')
                producer.produce(settings.KAFKA_TOPICS['trigger-daily'], value=trigger_data, partition=set_id)
                logger.info(f"Published fetch-trigger-daily for set_id {set_id} at {trigger_time} to partition {set_id}")
            producer.flush(timeout=10)
            
            return JsonResponse({
                "status": "success",
                "message": f"Triggered daily processing for all sets at {trigger_time}",
                "data": {}
            }, status=202)
        finally:
            producer.flush() 
    except Exception as e:
        logger.error(f"Failed to publish to fetch-trigger-daily: {str(e)}")
        return JsonResponse({
            "status": "error",
            "message": "Failed to trigger daily processing"
        }, status=500)

def fetch_last_15min_data(request):
    logger.info("API triggered for fetching 15-minute interval data")
    try:
        producer = create_producer()
        try:
            trigger_time = datetime.now(ZoneInfo("UTC")).strftime('%Y-%m-%d %H:%M:%S')
            # Publish a trigger message for each set_id
            for set_id in range(NUM_SET):
                trigger_data = json.dumps({"set_id": set_id, "timestamp": trigger_time}).encode('utf-8')
                # IMPORTANT: Specify partition to match set_id
                producer.produce(settings.KAFKA_TOPICS['trigger-15min'], value=trigger_data, partition=set_id)
                logger.info(f"Published fetch-trigger-15min for set_id {set_id} at {trigger_time} to partition {set_id}")
            producer.flush(timeout=10) # Flush after sending all triggers
            
            return JsonResponse({
                "status": "success",
                "message": f"Triggered 15-minute processing for all sets at {trigger_time}",
                "data": {}
            }, status=202)
        finally:
            producer.flush() # Ensure all messages are flushed on exit
    except Exception as e:
        logger.error(f"Failed to publish to fetch-trigger-15min: {str(e)}")
        return JsonResponse({
            "status": "error",
            "message": "Failed to trigger 15-minute processing"
        }, status=500)

def fetch_historical_data(request):
    logger.info("API triggered for fetching historical data")
    try:
        producer = create_producer()
        try:
            trigger_time = datetime.now(ZoneInfo("UTC")).strftime('%Y-%m-%d %H:%M:%S')
            # Publish a trigger message for each set_id
            for set_id in range(NUM_SET):
                trigger_data = json.dumps({"set_id": set_id, "timestamp": trigger_time}).encode('utf-8')
                # IMPORTANT: Specify partition to match set_id
                producer.produce(settings.KAFKA_TOPICS['trigger-historical'], value=trigger_data, partition=set_id)
                logger.info(f"Published fetch-trigger-historical for set_id {set_id} at {trigger_time} to partition {set_id}")
            producer.flush(timeout=10) # Flush after sending all triggers
            
            return JsonResponse({
                "status": "success",
                "message": f"Triggered historical processing for all sets at {trigger_time}",
                "data": {}
            }, status=202)
        finally:
            producer.flush() # Ensure all messages are flushed on exit
    except Exception as e:
        logger.error(f"Failed to publish to fetch-trigger-historical: {str(e)}")
        return JsonResponse({
            "status": "error",
            "message": "Failed to trigger historical processing"
        }, status=500)

from collector.handler.OptionDataCollector import OptionDataCollector
def fetch_option_data(request):
    """
    Fetches options data for multiple symbols (in batches of 8) from Yahoo Finance.
    """
    today = datetime.now().date()
    start_date = datetime.strptime("2025-06-16", "%Y-%m-%d").date()
    # cutoff = datetime.strptime("2025-08-16", "%Y-%m-%d").date()
    cutoff = today + timedelta(days=65)
    full_result = []

    logger.info(f"today: {today}")
    logger.info(f"cutoff: {cutoff}")
    producer = create_producer()
    full_result = OptionDataCollector(producer, start_date, cutoff, full_result)

    return JsonResponse({
        "status": "success",
        "message": "Options data fetched for all symbols in batches.",
        "data": full_result
    }, status=200)