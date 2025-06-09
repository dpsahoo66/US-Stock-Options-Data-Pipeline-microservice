# tweleve data api to fetch real time stock data 

import requests
import pandas as pd

# Replace with your own API key
api_key = "4954a2a422a84f228c11681b41980848"
symbol = "AAPL"

url = "https://api.twelvedata.com/time_series"

params = {
    "symbol": symbol,
    "interval": "1min",
    "start_date": "2000-01-01",
    "end_date": "2025-06-10",
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
    df = df.sort_values("datetime", ascending=False)  # Most recent first
    # Save to Excel
    file_name = f"{symbol}_realTime_data.xlsx"
    df.to_excel(file_name, index=False)
    print(f"Data saved successfully to '{file_name}'")
