import pandas as pd
import logging
from .DataPreprocessor import DataPreprocessor

logger = logging.getLogger(__name__)

def DailyDataProcessor(data):
    
    logger.info(f"PROCESSOR CALLED DAILYDATAPROCESSOR ")

    try:
        symbol = data['meta']['symbol']
        data_value = data['values'][0]

        data_value['symbol'] = symbol

        # Converting values to DataFrame for processing passing as list as it is single dictionary
        data_df = pd.DataFrame([data_value])

        df = data_df[['datetime','symbol', 'open', 'high', 'low', 'close', 'volume']]

        logger.info(f"Processing daily data for symbol: {symbol}")
        
        # Initializing preprocessor for daily stock data
        preprocessor = DataPreprocessor(data_type='daily_stock')
        
        # Applying comprehensive preprocessing
        processed_data = preprocessor.preprocess_stock_data(df)
        
        logger.info(f"Daily data processing completed for {symbol}")

        logger.info(f"Processed data sending back to Processor: {processed_data}")

        return processed_data
        
    except Exception as e:
        logger.error(f"Daily data processing error: {str(e)}")
        # Return original data if processing fails
        return data