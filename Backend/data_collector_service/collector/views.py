from django.shortcuts import render
import requests
import os
import json
from confluent_kafka import Producer
from datetime import datetime
from datetime import datetime, timedelta, timezone
import time
from django.http import JsonResponse
from collector.utils.symbols import symbols
import yfinance as yf
import pandas as pd

# Kafka Producer setup
conf = {'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')}
producer = Producer(conf)


from collector.util.create_batch import create_batches

def fetch_each_day_data(request):
    """
    Fetches daily data for all symbols in batches and pushes each to Kafka.
    One batch per second (batch size = 8).
    """
    URL = "https://api.twelvedata.com/time_series"
    api_key = os.getenv('TWELVE_DATA_API_KEY')

    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).strftime('%Y-%m-%d')

    all_results = []

    for batch in create_batches(symbols, batch_size=8):
        batch_result = {}

        for symbol in batch:
            params = {
                "symbol": symbol,
                "interval": "1day",
                "start_date": today,
                "end_date": tomorrow,
                "apikey": api_key,
                "outputsize": 1
            }

            response = requests.get(URL, params=params)

            if response.status_code == 200:
                data = response.json()
                if data.get('code') == 400:
                    batch_result[symbol] = {"error": "No data available"}
                    continue
                else:
                    batch_result[symbol] = data

                    # Push to Kafka
                    # producer.produce('daily-data', value=json.dumps(data).encode('utf-8'))
                    # producer.flush()
            else:
                batch_result[symbol] = {
                    "error": f"Failed: {response.status_code}",
                    "details": response.text
                }

        all_results.append(batch_result)
        time.sleep(60)  # Wait 1 second before next batch

    return JsonResponse({
        "status": "success",
        "message": "Fetched and pushed all batches",
        "data": all_results
    }, status=200)


def fetch_last_15min_data(request):
    """
    Fetches last 15 minutes data for multiple symbols (in batches of 8) from Twelve Data API.
    """

    URL = "https://api.twelvedata.com/time_series"
    api_key = os.getenv('TWELVE_DATA_API_KEY')
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).strftime('%Y-%m-%d')

    all_data = []

    for batch in create_batches(symbols, batch_size=8):
        batch_data = {}
        for symbol in batch:
            params = {
                "symbol": symbol,
                "interval": "1min",
                "start_date": today,
                "end_date": tomorrow,
                "apikey": api_key,
                "outputsize": 15
            }

            response = requests.get(URL, params=params)

            if response.status_code == 200:
                data = response.json()
                if data.get('code') == 400:
                    batch_data[symbol] = {"error": "No data available"}
                    continue
                else:
                    batch_data[symbol] = data
                    # Optionally send to Kafka
                    # producer.produce('15min-data', value=json.dumps(data).encode('utf-8'))
                    # producer.flush()
            else:
                batch_data[symbol] = {
                    "error": f"Failed: {response.status_code}",
                    "details": response.text
                }

        all_data.append(batch_data)
        # time.sleep(60)  # Optional delay: wait 1 min before next batch

    return JsonResponse({
        "status": "success",
        "message": "Fetched 5-minute data for all symbols in batches",
        "data": all_data
    }, status=200)

def fetch_option_data(request):
    """
    Fetches options data for multiple symbols (in batches of 8) from Yahoo Finance.
    """
    today = datetime.now().date()
    cutoff = today + timedelta(days=65)
    full_result = []

    for batch in create_batches(symbols, batch_size=8):
        batch_result = {}
        for symbol in batch:
            try:
                ticker = yf.Ticker(symbol)
                expirations = ticker.options
                if not expirations:
                    batch_result[symbol] = {
                        "status": "error",
                        "message": "No expiration dates found."
                    }
                    continue

                valid_expirations = []
                for exp in expirations:
                    try:
                        exp_date = datetime.strptime(exp, "%Y-%m-%d").date()
                        if today <= exp_date <= cutoff:
                            valid_expirations.append(exp)
                    except Exception as e:
                        print(f"Invalid expiration format for {symbol}: {exp} — {e}")

                if not valid_expirations:
                    batch_result[symbol] = {
                        "status": "no_data",
                        "message": "No valid expirations in next 60 days."
                    }
                    continue

                all_calls = []
                for expiry in valid_expirations:
                    try:
                        opt_chain = ticker.option_chain(expiry)
                        calls = opt_chain.calls.copy()
                        calls["expirationDate"] = expiry
                        all_calls.append(calls)
                        print(f"{symbol}: Fetched {len(calls)} calls for {expiry}")
                    except Exception as e:
                        print(f"{symbol}: Error fetching data for {expiry}: {e}")

                if all_calls:
                    calls_df = pd.concat(all_calls, ignore_index=True)
                    if not calls_df.empty:
                        data_batch = calls_df.to_dict(orient='records')
                        # producer.produce('options-data', value=json.dumps(data_batch).encode('utf-8'))
                        # producer.flush()
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
        
    return JsonResponse({
        "status": "success",
        "message": "Options data fetched for all symbols in batches.",
        "data": full_result
    }, status=200)