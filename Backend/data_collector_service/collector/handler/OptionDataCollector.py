import json
import time
import pandas as pd
import yfinance as yf
from datetime import datetime

from django.conf import settings
from collector.utils.create_batch import create_batches
from collector.utils.logConfig import LogConfig
from collector.utils.symbols import SYMBOLS


logger = LogConfig()

def OptionDataCollector(producer, start_date, cutoff, full_result):
    for batch in create_batches(SYMBOLS, batch_size=8):
        batch_result = {}
        for symbol in batch:
            try:

                ticker = yf.Ticker(symbol)
                logger.info(f"[{symbol}] - Ticker object created.")
                
                try:
                    expirations = ticker.options
                    logger.info(f"[{symbol}] - expiration date fetched")
                except Exception as e:
                    logger.error(f"[{symbol}] - Error creating expiration update to new yfinance version: {e}", exc_info=True)
                    continue
                
                if not expirations:
                    batch_result[symbol] = {
                        "status": "error",
                        "message": "No expiration dates found."
                    }
                    continue

                logger.info(f"expirations: {expirations}")

                valid_expirations = []
                for exp in expirations:
                    try:
                        exp_date = datetime.strptime(exp, "%Y-%m-%d").date()
                        if start_date <= exp_date <= cutoff:
                            valid_expirations.append(exp)
                    except Exception as e:
                        print(f"Invalid expiration format for {symbol}: {exp} — {e}")

                if not valid_expirations:
                    batch_result[symbol] = {
                        "status": "no_data",
                        "message": "No valid expirations in next 60 days."
                    }
                    continue
                
                logger.info(f"valid_expirations: {valid_expirations}")

                all_calls = []
                all_puts = []
                for expiry in valid_expirations:
                    try:
                        opt_chain = ticker.option_chain(expiry)
                        calls = opt_chain.calls.copy()
                        calls["expirationDate"] = expiry
                        all_calls.append(calls)
                        puts = opt_chain.puts.copy()
                        puts["expirationDate"] = expiry
                        all_puts.append(puts)
                        print(f"{symbol}: Fetched {len(calls)} calls for {expiry}")
                    except Exception as e:
                        print(f"{symbol}: Error fetching data for {expiry}: {e}")

                logger.info(f"all_calls: {all_calls}")

                if all_calls:
                    calls_df = pd.concat(all_calls, ignore_index=True)
                    calls_df["type"] = "calls"
                    if not calls_df.empty:
                        # Convert timestamp columns to string first
                        for col in ['lastTradeDate', 'expirationDate']:
                            if col in calls_df.columns:
                                calls_df[col] = calls_df[col].astype(str)

                        # Now convert DataFrame to list of dicts
                        data_batch = calls_df.to_dict(orient='records')

                        # Serialize data_batch with json.dumps and encode to bytes for Kafka
                        data_value = json.dumps(data_batch).encode('utf-8')
                        logger.info(f"Passing fetched data to option-data Producer: {data_value}")
                        producer.produce(settings.KAFKA_TOPICS['options'], value=data_value)
                        producer.flush()
                        
                        batch_result[symbol] = {
                            "status": "success",
                            "message": f"{len(data_batch)} option records",
                            "data": data_batch
                        }
                    else:
                        batch_result[symbol] = {
                            "status": "warning",
                            "message": "No option data found"
                        }
                else:
                    batch_result[symbol] = {
                        "status": "warning",
                        "message": "No valid expirations with data"
                    }
                
                if all_puts:
                    puts_df = pd.concat(all_puts, ignore_index=True)
                    puts_df["type"] = "puts"
                    if not puts_df.empty:
                        # Convert timestamp columns to string first
                        for col in ['lastTradeDate', 'expirationDate']:
                            if col in puts_df.columns:
                                puts_df[col] = puts_df[col].astype(str)

                        # Now convert DataFrame to list of dicts
                        data_batch = puts_df.to_dict(orient='records')

                        # Serialize data_batch with json.dumps and encode to bytes for Kafka
                        data_value = json.dumps(data_batch).encode('utf-8')
                        logger.info(f"Passing fetched data to option-data Producer: {data_value}")
                        producer.produce(settings.KAFKA_TOPICS['options'], value=data_value)
                        producer.flush()
                        
                        batch_result[symbol] = {
                            "status": "success",
                            "message": f"{len(data_batch)} option records",
                            "data": data_batch
                        }
                    else:
                        batch_result[symbol] = {
                            "status": "warning",
                            "message": "No option data found"
                        }
                else:
                    batch_result[symbol] = {
                        "status": "warning",
                        "message": "No valid expirations with data"
                    }

            except Exception as e:
                batch_result[symbol] = {
                    "status": "error",
                    "message": f"Unexpected error: {e}"
                }
            
        full_result.append(batch_result)
        time.sleep(5) 