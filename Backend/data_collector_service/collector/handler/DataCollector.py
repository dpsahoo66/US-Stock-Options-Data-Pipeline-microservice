import os
import json
import time
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from confluent_kafka import KafkaError, Producer 

from django.conf import settings
from collector.utils.create_batch import create_batches
from collector.kafka import kafkaConfig 
from collector.utils.logConfig import LogConfig

# Configure logging
logger = LogConfig()

class DataCollector:
    def __init__(self, set_id, symbols, api_key, partition, trigger_type):
        self.set_id = set_id
        self.symbols = symbols
        self.api_key = api_key
        self.partition = partition
        self.trigger_type = trigger_type
        self.URL = "https://api.twelvedata.com/time_series"
        
        self.process() # Start data collection immediately upon instantiation

    def process(self):
        
        pid = os.getpid()
        logger.info(f"Worker {pid} processing set {self.set_id}, partition {self.partition}, trigger_type {self.trigger_type} for {len(self.symbols)} symbols.")
        
        producer = None
        try:
            producer = kafkaConfig.create_producer()
            logger.info(f"Set {self.set_id}, Trigger {self.trigger_type}: Kafka producer created successfully.")
        except KafkaError as e:
            logger.error(f"Set {self.set_id}, Trigger {self.trigger_type}: Failed to create Kafka producer: {str(e)}")
            return 
        
        batch_results = []
        
        params_base = {}
        kafka_topic = ""
        sleep_time = 70 # Adjusted based on API rate limits

        if self.trigger_type == 'daily':
            params_base = {
                "interval": "1day", 
                "start_date": (datetime.now(ZoneInfo("UTC")).date() - timedelta(days=1)).strftime('%Y-%m-%d'),
                "end_date":  datetime.now(ZoneInfo("UTC")).date().strftime('%Y-%m-%d'),
                "outputsize": 1
            }
            kafka_topic = settings.KAFKA_TOPICS['daily']
            
        elif self.trigger_type == '15min':
            params_base = {
                "interval": "1min", 
                 "start_date": (datetime.now(ZoneInfo("UTC")).date() - timedelta(days=1)).strftime('%Y-%m-%d'),
                "end_date":  datetime.now(ZoneInfo("UTC")).date().strftime('%Y-%m-%d'),
                "outputsize": 15 
            }
            kafka_topic = settings.KAFKA_TOPICS['15min']
            
        elif self.trigger_type == 'historical':
            params_base = {
                "interval": "1day",
                "start_date": (datetime.now(ZoneInfo("UTC")).date() - timedelta(days=7)).strftime('%Y-%m-%d'),
                "end_date":  datetime.now(ZoneInfo("UTC")).date().strftime('%Y-%m-%d'),
                # "start_date": datetime.now(ZoneInfo("UTC")) - timedelta(days=365), # for 1 year
                # "end_date":  datetime.now(ZoneInfo("UTC")),
                "outputsize": 5000 
            }
            kafka_topic = settings.KAFKA_TOPICS['historical']
        else:
            logger.error(f"Set {self.set_id}, Trigger {self.trigger_type}: Unknown trigger_type. Exiting process.")
            return

        try:
            logger.info(f"Set {self.set_id}, Trigger {self.trigger_type}: Starting data fetch for {len(self.symbols)} symbols.")
            
            # processing symbol in batched of 8 as per API per minute limit
            for batch in create_batches(self.symbols, batch_size=8): 
                logger.info(f"Set {self.set_id}, Trigger {self.trigger_type}: Processing batch: {batch}")
                batch_result = {}
                for symbol in batch:
                    
                    params = params_base.copy()
                    params.update({"symbol": symbol, "apikey": self.api_key})
                    success = self._fetch_data(params, symbol, batch_result, producer, kafka_topic) 
                    
                    if not success:
                        logger.warning(f"Set {self.set_id}, Symbol {symbol}, Trigger {self.trigger_type}: Failed to fetch")
                                
                batch_results.append(batch_result)
                logger.info(f"Set {self.set_id}, Trigger {self.trigger_type}: Completed batch, waiting {sleep_time} seconds (for API rate limits).")
                time.sleep(sleep_time) 

            # Flushing the producer ONLY ONCE after all data for this DataCollector run is produced.
            producer.flush(timeout=30) # A longer timeout for the final flush
            logger.info(f"Set {self.set_id}, Trigger {self.trigger_type}: All buffered data flushed to Kafka topic {kafka_topic}.")
            
        except Exception as e:
            logger.error(f"Set {self.set_id}, Trigger {self.trigger_type}: Processing failed: {str(e)}", exc_info=True)
        finally:
            
            # Final small flush to catch any stragglers if an error occurred earlier.
            if producer:
                producer.flush(timeout=5) 
            logger.info(f"Worker {pid} finished processing set {self.set_id} for trigger {self.trigger_type}.")
        
        return batch_results

    def _fetch_data(self, params, symbol, batch_result, producer: Producer, kafka_topic):
        """
        Fetches data for a single symbol and produces it to Kafka.
        """
        for attempt in range(3): 
            try:
                response = requests.get(self.URL, params=params, timeout=10) 
                
                logger.debug(f"Set {self.set_id}, Symbol {symbol}, Trigger {self.trigger_type}: Attempt {attempt + 1}, Response Status: {response.status_code}, Content: {response.text[:500]}...") # Log partial content

                if response.status_code == 200:
                    data = response.json()
                    if data.get('code') == 400 or "values" not in data or not data.get("values"):
                        message = data.get("message", "No data available or invalid symbol/parameters.")
                        logger.info(f"Set {self.set_id}, Symbol {symbol}, Trigger {self.trigger_type}: API returned error or no data: {message}")
                        batch_result[symbol] = {"error": message}
                        return False 
                    else:
                        batch_result[symbol] = data 

                        data_value = json.dumps(data).encode('utf-8')
                        logger.info(f"before pushing: {data_value}")
                        producer.produce(kafka_topic, value=data_value, partition=self.partition)
                        logger.debug(f"Set {self.set_id}, Symbol {symbol}, Trigger {self.trigger_type}: Produced data to partition {self.partition}, topic {kafka_topic}.")
                        
                        return True # Successfully produced messages
                else:
                    # HTTP status code indicates an error
                    logger.error(f"Set {self.set_id}, Symbol {symbol}, Trigger {self.trigger_type}: HTTP Request failed with status {response.status_code}, Content: {response.text}")
                    batch_result[symbol] = {
                        "error": f"HTTP Failed: {response.status_code}",
                        "details": response.text
                    }
                    if response.status_code == 429: # Too many requests
                        logger.warning(f"Set {self.set_id}, Symbol {symbol}, Trigger {self.trigger_type}: Rate limit hit. Retrying after delay.")
                        time.sleep(15 * (attempt + 1)) 
                    else:
                        time.sleep(5) # Shorter sleep for other errors before retrying
            except requests.RequestException as e:
                logger.error(f"Set {self.set_id}, Symbol {symbol}, Trigger {self.trigger_type}: Request exception on attempt {attempt + 1}: {str(e)}", exc_info=True)
                batch_result[symbol] = {"error": str(e)}
                time.sleep(5 * (attempt + 1)) 
            except Exception as e:
                logger.error(f"Set {self.set_id}, Symbol {symbol}, Trigger {self.trigger_type}: Unexpected error in _fetch_data on attempt {attempt + 1}: {str(e)}", exc_info=True)
                batch_result[symbol] = {"error": f"Unexpected error: {str(e)}"}
                time.sleep(5)

        logger.error(f"Set {self.set_id}, Symbol {symbol}, Trigger {self.trigger_type}: All {attempt + 1} attempts failed to fetch data.")
        return False # All attempts failed