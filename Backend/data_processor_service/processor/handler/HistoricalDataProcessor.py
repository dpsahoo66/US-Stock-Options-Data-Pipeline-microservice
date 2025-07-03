import pandas as pd
import logging
from processor.handler.DataPreprocessor import DataPreprocessor
from processor.utils.logConfig import LogConfig

logger = LogConfig()

def HistoricalDataProcessor(data):

    logger.info("PROCESSOR CALLED HISTORICALDATAPROCESSOR")

    try:
        symbol = data['meta']['symbol']
        data_values = data['values']          

        for row in data_values:
            row['symbol'] = symbol

        data_df = pd.DataFrame(data_values)

        df = data_df[['datetime', 'symbol', 'open', 'high', 'low', 'close', 'volume']]

        preprocessor = DataPreprocessor(data_type='historical_stock')
        processed = preprocessor.preprocess_stock_data(df)

        processed_df = pd.DataFrame(processed)
        processed_df['datetime'] = (
            pd.to_datetime(processed_df['datetime'], utc=True, errors='coerce')
              .sort_values()
        )
        processed_df['datetime'] = processed_df['datetime'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        logger.info(f"Historical data processing completed for {symbol}")
        return processed_df.to_dict(orient='records')

    except Exception as e:
        logger.error(f"Historical data processing error: {e}", exc_info=True)
        return data