#ALPHA_VANTAGE_API_KEY = "ZKN7NH8WBGOG51VS"
# import requests

# url = "https://www.alphavantage.co/query?function=OPTION_CHAIN&symbol=AAPL&apikey=ZKN7NH8WBGOG51VS"
# # url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey=ZKN7NH8WBGOG51VS'
# r = requests.get(url)
# data = r.json()

# print(data)

# import yfinance as yf

# symbol = 'IBM'
# data = yf.download(symbol, period='max', interval='1d')  # Can also try '1wk' or '1mo' for longer range
# print(data.isnull().any())

# print(data.tail())

# from polygon import RESTClient

# client = RESTClient("ItiXVFIDOr4Il3hWal0uOpCfJ5Aql5sp")

# contracts = []
# for c in client.list_options_contracts(
# 	underlying_ticker="APPL",
# 	contract_type="puts",
# 	order="asc",
# 	limit=10,
# 	sort="ticker",
# 	):
#     contracts.append(c)

# print(contracts)

# import requests

# # Replace with your Polygon.io API key
# api_key = "ItiXVFIDOr4Il3hWal0uOpCfJ5Aql5sp"
# symbol = "AAPL"
# url = f"https://api.polygon.io/v3/reference/options/contracts?contract_type=call&order=asc&limit=10&sort=ticker&apiKey=ItiXVFIDOr4Il3hWal0uOpCfJ5Aql5sp"

# response = requests.get(url)

# # Check for successful response
# if response.status_code == 200:
#     data = response.json()
#     results = data.get('results', [])
    
#     if not results:
#         print("No option contracts found.")
#     else:
#         for idx, contract in enumerate(results):
#             print(f"{idx+1}. Ticker: {contract.get('ticker')}")
#             print(f"   Expiration Date: {contract.get('expiration_date')}")
#             print(f"   Strike Price: {contract.get('strike_price')}")
#             print(f"   Contract Type: {contract.get('contract_type')}")
#             print(f"   Exercise Style: {contract.get('exercise_style')}")
#             print("-" * 50)
# else:
#     print(f"Failed to fetch data: {response.status_code}")
#     print(response.json())

import requests
import pandas as pd

# Replace with your own API key
api_key = "4954a2a422a84f228c11681b41980848"
symbol = "AAPL"  # Change to any U.S. stock symbol like 'MSFT', 'GOOGL', etc.

url = "https://api.twelvedata.com/time_series"

params = {
    "symbol": symbol,
    "interval": "1h",
    "start_date": "2000-01-01",
    "end_date": "2025-06-08",
    "apikey": api_key,
    "outputsize": 5000
}

response = requests.get(url, params=params)
data = response.json()

# Check for errors
if "values" not in data:
    print("Error:", data.get("message", "Unknown error"))
else:
    df = pd.DataFrame(data["values"])
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime")
    print(df)