import logging
from file_writer.utils.logConfig import LogConfig

logger = LogConfig()

def HistoricalDataFileHandler(data):
    logger.info("inside HistoricalDataFileHandler ")

    return data