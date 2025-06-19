import pandas as pd
import logging
from .DataPreprocessor import DataPreprocessor

logger = logging.getLogger(__name__)

def RealTimeDataProcessor(data):
    """
    Processes real-time (15-minute) stock data with comprehensive preprocessing
    Applies all 5 preprocessing steps:
    1. Handling missing values
    2. Removing duplicates  
    3. Check invalid rows
    4. Fixing data types
    5. Correcting inconsistent formatting
    
    Args:
        data: Dictionary containing real-time stock data
    Returns:
        Processed data dictionary
    """
    try:
        logger.info(f"Processing real-time data for symbol: {data.get('symbol', 'unknown')}")
        
        # Initialize preprocessor for real-time stock data
        preprocessor = DataPreprocessor(data_type='realtime_stock')
        
        # Apply comprehensive preprocessing
        processed_data = preprocessor.preprocess_stock_data(data)
        
        # Add processing timestamp and data type
        processed_data['processing_timestamp'] = pd.Timestamp.now().isoformat()
        processed_data['data_type'] = 'realtime'
        processed_data['processor_version'] = '1.0'
        
        # Additional real-time specific processing
        if 'values' in processed_data and processed_data['values']:
            # Sort by datetime for real-time data (important for time series)
            df = pd.DataFrame(processed_data['values'])
            if 'datetime' in df.columns:
                df['datetime'] = pd.to_datetime(df['datetime'])
                df = df.sort_values('datetime').reset_index(drop=True)
                df['datetime'] = df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
                processed_data['values'] = df.to_dict(orient='records')
        
        logger.info(f"Real-time data processing completed for {data.get('symbol', 'unknown')}")
        return processed_data
        
    except Exception as e:
        logger.error(f"Real-time data processing error: {str(e)}")
        # Return original data if processing fails
        return data