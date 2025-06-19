import pandas as pd
import logging
from .DataPreprocessor import DataPreprocessor

logger = logging.getLogger(__name__)

def DailyDataProcessor(data):
    """
    Processes daily stock data with comprehensive preprocessing
    Applies all 5 preprocessing steps:
    1. Handling missing values
    2. Removing duplicates  
    3. Check invalid rows
    4. Fixing data types
    5. Correcting inconsistent formatting
    
    Args:
        data: Dictionary containing daily stock data
    Returns:
        Processed data dictionary
    """
    try:
        logger.info(f"Processing daily data for symbol: {data.get('symbol', 'unknown')}")
        
        # Initialize preprocessor for daily stock data
        preprocessor = DataPreprocessor(data_type='daily_stock')
        
        # Apply comprehensive preprocessing
        processed_data = preprocessor.preprocess_stock_data(data)
        
        # Add processing timestamp and data type
        processed_data['processing_timestamp'] = pd.Timestamp.now().isoformat()
        processed_data['data_type'] = 'daily'
        processed_data['processor_version'] = '1.0'
        
        logger.info(f"Daily data processing completed for {data.get('symbol', 'unknown')}")
        return processed_data
        
    except Exception as e:
        logger.error(f"Daily data processing error: {str(e)}")
        # Return original data if processing fails
        return data