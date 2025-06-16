from django.shortcuts import render
import requests
import os
import json
from confluent_kafka import Producer
from datetime import datetime
from datetime import datetime, timedelta, timezone
from django.http import JsonResponse
from collector.util.symbols import symbols

# Kafka Producer setup
conf = {'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')}
producer = Producer(conf)

def fetch_each_day_data(request):
    """
    Fetches daily data for the given symbol and date from Twelve Data API.
    """
    symbol = "AAPL"
    URL = "https://api.twelvedata.com/time_series"
    api_key = os.getenv('TWELVE_DATA_API_KEY')

    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).strftime('%Y-%m-%d')

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
            return JsonResponse({
            "status": "error", 
             "message": "No data is available on the specified dates", 
             "data": data
             }, status=400)

        # producer.produce('daily-data', value=json.dumps(data).encode('utf-8'))
        # producer.flush()

        return JsonResponse({
            "status": "success", 
             "message": "Daily data sent to Kafka", 
             "data": data
             }, status=200)
    else:
        return JsonResponse({
            "status": "error",
            "message": f"Failed to fetch daily data: {response.status_code}",
            "details": response.text
        }, status=response.status_code)


def fetch_last_5min_data(request):
    """
    Fetches last 5 minutes data for the given symbol from Twelve Data API.
    """
    symbol = "AAPL"
    URL = "https://api.twelvedata.com/time_series"
    api_key = os.getenv('TWELVE_DATA_API_KEY')

    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).strftime('%Y-%m-%d')

    params = {
        "symbol": symbol,
        "interval": "1min",
        "start_date": today,
        "end_date": tomorrow,
        "apikey": api_key,
        "outputsize": 5 
    }

    response = requests.get(URL, params=params)
    
    if response.status_code == 200:
        data = response.json()
        if data.get('code') == 400:
            return JsonResponse({
            "status": "error", 
             "message": "No data is available on the specified dates", 
             "data": data
             }, status=400)
        # producer.produce('5min-data', value=json.dumps(data).encode('utf-8'))
        # producer.flush()

        return JsonResponse({"status": "success", "message": "5-min data sent to Kafka", "data": data}, status=200)
    else:
        return JsonResponse({
            "status": "error",
            "message": f"Failed to fetch 5min data: {response.status_code}",
            "details": response.text
        }, status=response.status_code)
    

import yfinance as yf
import pandas as pd
def fetch_option_data(request):
    """
    Fetches options data for the given symbol from Yahoo Finance.
    """
    symbol = "AAPL"
    ticker = yf.Ticker(symbol)
    today = datetime.now().date()
    cutoff = today + timedelta(days=65)

    try:
        expirations = ticker.options
        if not expirations:
            return JsonResponse({
                    "status": "error",
                    "message": "No expiration dates found for the symbol."
                }, status=204)
        print(f"Total expirations found: {len(expirations)}")

    except Exception as e:
        return JsonResponse({
                "status": "error",
                "message": f"Failed to fetch expiration dates: {e}"
            }, status=204)

    valid_expirations = []
    for exp in expirations:
        try:
            exp_date = datetime.strptime(exp, "%Y-%m-%d").date()
            if today <= exp_date <= cutoff:
                valid_expirations.append(exp)
        except Exception as e:
            print(f"Invalid expiration format: {exp} — {e}")

    if not valid_expirations:
        return JsonResponse({
            "status": "no_data", 
            "message": "No valid expirations in the next 60 days."
            }, status=204)

    all_calls = []

    for expiry in valid_expirations:
        try:
            opt_chain = ticker.option_chain(expiry)
            calls = opt_chain.calls.copy()
            calls["expirationDate"] = expiry
            all_calls.append(calls)
            print(f"Fetched calls for expiry: {expiry} — {len(calls)} rows")
        except Exception as e:
            print(f"Error fetching data for {expiry}: {e}")

    if all_calls:
        calls_df = pd.concat(all_calls, ignore_index=True)
        if not calls_df.empty:
            try:
                data_batch = calls_df.to_dict(orient='records')
                print(data_batch)
                # producer.produce('options-data', value=json.dumps(data).encode('utf-8'))
                # producer.flush()
               
                return JsonResponse({
                    "status": "success",
                    "message": f"Sent {len(data_batch)} options records to Kafka."
                }, status=200)
            except Exception as e:
                return JsonResponse({
                    "status": "error", 
                    "message": f"Kafka error: {e}"
                    }, status=500)
        else:
            return JsonResponse({
                "status": "warning", 
                "message": "No option data found to send."
                }, status=204)
    else:
        return JsonResponse({
            "status": "warning", 
            "message": "No valid expirations within 60 days."
            }, status=204)