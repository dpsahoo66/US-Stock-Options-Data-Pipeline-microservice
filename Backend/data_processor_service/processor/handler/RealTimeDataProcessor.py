import pandas as pd
import logging
from processor.handler.DataPreprocessor import DataPreprocessor
from processor.utils.logConfig import LogConfig

logger = LogConfig()

def RealTimeDataProcessor(data):
    
    logger.info(f"PROCESSOR CALLED REALTIMEDATAPROCESSOR")

    try:

        symbol = data['meta']['symbol']
        data_value = data['values']
    
        for value in data_value:
            value['symbol'] = symbol

         # Convert values to DataFrame for processing
        data_df = pd.DataFrame(data_value)

        df = data_df[['datetime','symbol', 'open', 'high', 'low', 'close', 'volume']]

        logger.info(f"Processing real-time data for symbol: {symbol}")
        
        # Initializing preprocessor for real-time stock data
        preprocessor = DataPreprocessor(data_type='realtime_stock')
        
        # Applying comprehensive preprocessing
        processed_data = preprocessor.preprocess_stock_data(df)
        
       
        # Sort by datetime for real-time data (important for time series)
        df = pd.DataFrame(processed_data)
        if 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime'], utc=True)  
            df = df.sort_values('datetime').reset_index(drop=True)

            # converting into Influx-friendly timestamp ISO 8601 UTC
            df['datetime'] = df['datetime'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')  
            processed_data = df.to_dict(orient='records')
        
        logger.info(f"Real-time data processing completed for {symbol}")

        logger.info(f"Processed data sending back to Processor: {processed_data}")

        return processed_data
        
    except Exception as e:
        logger.error(f"Real-time data processing error: {str(e)}")
        # Return original data if processing fails
        return data