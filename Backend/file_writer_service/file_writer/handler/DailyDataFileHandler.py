from file_writer.utils.logConfig import LogConfig

logger = LogConfig()

def DailyDataFileHandler(data):
    logger.info("inside DailyDataFileHandler ")
    return data