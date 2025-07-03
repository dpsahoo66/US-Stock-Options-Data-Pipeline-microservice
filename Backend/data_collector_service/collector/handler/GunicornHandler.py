import os
import time
import json
import signal
import atexit
from threading import Thread
from confluent_kafka import Consumer, KafkaError

from collector.utils.symbols import SYMBOL_SETS, API_KEYS
from collector.kafka import kafkaConfig
from collector.utils.logConfig import LogConfig
from collector.handler.DataCollector import DataCollector

logger = LogConfig()
NUM_SETS = len(SYMBOL_SETS)

# Global dictionary to map set_id to its corresponding symbols and API key.
# We assume set_id corresponds directly to the index in SYMBOL_SETS and API_KEYS.
SET_CONFIGS = {
    i: {
        "symbols": SYMBOL_SETS[i],
        "api_key": API_KEYS[i]
    } for i in range(NUM_SETS)
}

# The main loop for a Kafka consumer thread within a Gunicorn worker.
def worker_loop(consumer_instance):

    pid = os.getpid()
    logger.info(f"Worker {pid} (Thread {threading.current_thread().name}) waiting for Kafka triggers.")

    trigger_topics = [
                    os.getenv("KAFKA_TRIGGER_DAILY"), 
                    os.getenv("KAFKA_TRIGGER_15MIN"), 
                    os.getenv("KAFKA_TRIGGER_HISTORICAL")
                    ]
    consumer_instance.subscribe(trigger_topics)
    
    # Dictionary to keep track of the last processed timestamp for each (set_id, trigger_type) tuple
    # This prevents reprocessing the same trigger message if Kafka delivers it again.
    last_triggers = {}
    for sid in range(NUM_SETS):
        last_triggers[(sid, 'daily')] = None
        last_triggers[(sid, '15min')] = None
        last_triggers[(sid, 'historical')] = None
    
    try:
        while True:
            msg = consumer_instance.poll(1.0) 
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(f"Worker {pid}: Reached end of partition {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                    continue
                else:
                    logger.error(f"Worker {pid}: Consumer error: {msg.error()}")
                    continue
            
            trigger_type = None
            topic = msg.topic()
            if topic == os.getenv("KAFKA_TRIGGER_DAILY"):
                trigger_type = 'daily'
            elif topic == os.getenv("KAFKA_TRIGGER_15MIN"):
                trigger_type = '15min'
            elif topic ==  os.getenv("KAFKA_TRIGGER_HISTORICAL"):
                trigger_type = 'historical'
            
            if trigger_type:
                try:
                    trigger_data = json.loads(msg.value().decode('utf-8'))
                    trigger_set_id = trigger_data.get('set_id')
                    trigger_timestamp = trigger_data.get('timestamp')
                    
                    # Validating trigger_set_id
                    if trigger_set_id is None or trigger_set_id not in SET_CONFIGS:
                        logger.warning(f"Worker {pid}: Received trigger for invalid or unconfigured set_id {trigger_set_id}. Skipping message from topic {topic}.")
                        continue

                    # Checking if this trigger has already been processed (deduplication)
                    if trigger_timestamp != last_triggers.get((trigger_set_id, trigger_type)):
                        config = SET_CONFIGS[trigger_set_id]
                        symbols = config["symbols"]
                        api_key = config["api_key"]
                        
                        # The partition is the set_id
                        partition = trigger_set_id 

                        logger.info(f"Worker {pid} (set {trigger_set_id}, partition {partition}) detected new {trigger_type} trigger at {trigger_timestamp}")
                        
                        # Instantiating and run the DataCollector for this specific set_id
                        DataCollector(trigger_set_id, symbols, api_key, partition, trigger_type)
                        
                        # Update the last processed timestamp
                        last_triggers[(trigger_set_id, trigger_type)] = trigger_timestamp
                        logger.info(f"Worker {pid} (set {trigger_set_id}, partition {partition}) completed {trigger_type} processing.")

                    else:
                        logger.debug(f"Worker {pid} (set {trigger_set_id}, partition {msg.partition()}): Duplicate or already processed {trigger_type} trigger for timestamp {trigger_timestamp}. Skipping.")

                except json.JSONDecodeError as e:
                    logger.error(f"Worker {pid}: Failed to decode trigger message from topic {topic}: {str(e)}")
                except Exception as e:
                    logger.error(f"Worker {pid}: Unhandled error while processing trigger from topic {topic}: {str(e)}", exc_info=True)
    except Exception as e:
        logger.error(f"Worker {pid} error in main worker_loop: {str(e)}", exc_info=True)
        # In case of a critical error, the thread might exit. Gunicorn will eventually
        # restart the worker process if it completely fails. A short sleep here
        # might help prevent a tight error loop if there's a transient issue.
        time.sleep(5)
    finally:
        logger.info(f"Worker {pid} consumer closing.")
        consumer_instance.close()

# Gunicorn runs post fork hook immediately after a worker process is forked from the master process.
# post_fork is executed once in each worker process, right after the worker is forked but before it starts handling requests. 
def post_fork(server, worker):
    
    # process(worker) ID
    pid = os.getpid()
    logger.info(f"Gunicorn worker {pid} forked. Initializing Kafka consumer thread.")
    try:
        
        # Using the same 'data_processor_group' for all worker for parallelism 
        consumer = kafkaConfig.create_consumer() 
        
        # Start the worker_loop in a new thread, It's marked as daemon so it won't block the main thread.
        t = Thread(target=worker_loop, args=(consumer,), name=f"KafkaConsumerThread-{pid}")
        t.daemon = True 
        t.start()
        
        logger.info(f"Worker {pid}: Kafka consumer thread started successfully.")
    except Exception as e:
        logger.error(f"Worker {pid} failed to initialize Kafka consumer thread: {str(e)}", exc_info=True)
       

def handle_sigterm(signum, frame):
    """Graceful shutdown handler for SIGTERM."""
    logger.info(f"Worker {os.getpid()} received SIGTERM, exiting gracefully.")
    exit(0)

def cleanup():
    """Cleanup function registered with atexit."""
    logger.info("Main process (or exiting worker) performing cleanup.")
    # Add any global resource cleanup here if necessary.
    # Kafka consumer/producer are closed within their respective threads/functions.

# Register signal handlers for graceful shutdown
signal.signal(signal.SIGTERM, handle_sigterm)
atexit.register(cleanup)

# Import necessary for threading.current_thread() in logger
import threading
