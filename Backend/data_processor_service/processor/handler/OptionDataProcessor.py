import pandas as pd
import logging
from .DataPreprocessor import DataPreprocessor

logger = logging.getLogger(__name__)

def OptionDataProcessor(data):
    """
    Processes options data with comprehensive preprocessing
    Applies all 5 preprocessing steps:
    1. Handling missing values
    2. Removing duplicates  
    3. Check invalid rows
    4. Fixing data types
    5. Correcting inconsistent formatting
    
    Args:
        data: Dictionary containing options data
    Returns:
        Processed data dictionary
    """
    try:
        logger.info(f"Processing options data for symbol: {data.get('symbol', 'unknown')}")
        
        # Initialize preprocessor for options data
        preprocessor = DataPreprocessor(data_type='options')
        
        # Apply comprehensive preprocessing
        processed_data = preprocessor.preprocess_stock_data(data)
        
        # Add processing timestamp and data type
        processed_data['processing_timestamp'] = pd.Timestamp.now().isoformat()
        processed_data['data_type'] = 'options'
        processed_data['processor_version'] = '1.0'
        
        # Additional options-specific validation
        if 'values' in processed_data and processed_data['values']:
            processed_data = _validate_options_specific_data(processed_data)
        
        logger.info(f"Options data processing completed for {data.get('symbol', 'unknown')}")
        return processed_data
        
    except Exception as e:
        logger.error(f"Options data processing error: {str(e)}")
        # Return original data if processing fails
        return data

def _validate_options_specific_data(data):
    """
    Additional validation specific to options data
    """
    try:
        df = pd.DataFrame(data['values'])
        original_count = len(df)
        
        # Options-specific validation rules can be added here
        # For now, we'll use the same stock validation
        # but this can be extended for options-specific fields like:
        # - strike price validation
        # - expiration date validation
        # - option type validation (call/put)
        # - implied volatility ranges
        
        # Example: If options data has strike prices
        if 'strike' in df.columns:
            # Remove invalid strike prices (negative or zero)
            df = df[df['strike'] > 0]
        
        # Example: If options data has expiration dates
        if 'expiration' in df.columns:
            df['expiration'] = pd.to_datetime(df['expiration'], errors='coerce')
            df = df.dropna(subset=['expiration'])
            # Remove expired options (optional based on business logic)
            # current_date = pd.Timestamp.now()
            # df = df[df['expiration'] >= current_date]
        
        data['values'] = df.to_dict(orient='records')
        
        if len(df) != original_count:
            logger.info(f"Options-specific validation removed {original_count - len(df)} invalid records")
        
        return data
        
    except Exception as e:
        logger.error(f"Options-specific validation error: {str(e)}")
        return data