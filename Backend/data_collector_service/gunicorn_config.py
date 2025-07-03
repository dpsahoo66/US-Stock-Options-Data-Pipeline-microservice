
import os
from django.conf import settings
from collector.utils.symbols import SYMBOL_SETS
from collector.handler.GunicornHandler import post_fork as real_post_fork

NUM_SETS = len(SYMBOL_SETS)

bind = "0.0.0.0:8000"
workers = NUM_SETS # This is critical: ensures you have enough workers for each set_id/partition
threads = 1       # Each Gunicorn worker will have 1 main thread + 1 Kafka consumer thread
timeout = 300     # Increase timeout if your API calls or Kafka operations take longer
worker_class = 'sync' # Using sync workers. You could explore gevent/eventlet for more async behavior.


# Gunicorn hook: called in the context of the new worker process right after it's forked.
def post_fork(server, worker):
    real_post_fork(server, worker)

# Logging configuration for Gunicorn itself
accesslog = "-" 
errorlog = "-"  
loglevel = "info" 
capture_output = True 