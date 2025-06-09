import os
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def setup_session():
    """Initialize a requests Session with retry strategy for robust API calls."""
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    return session

def fetch_stock_data(session, ticker, start_date, end_date, api_key):
    """Fetch daily stock data from Polygon.io and return as DataFrame."""
    try:
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}"
        params = {
            "adjusted": "true",
            "sort": "asc",
            "limit": 50000,
            "apiKey": api_key
        }
        response = session.get(url, params=params)
        response.raise_for_status()  # Raise exception for 4xx/5xx errors
        
        data = response.json()
        if data.get("status") != "OK" or not data.get("results"):
            print(f"No data returned for {ticker} between {start_date} and {end_date}. Status: {data.get('status')}")
            return None
        
        # Parse results into DataFrame
        aggs = [{
            "Date": pd.to_datetime(result["t"], unit="ms"),
            "Open": result["o"],
            "High": result["h"],
            "Low": result["l"],
            "Close": result["c"],
            "Volume": result["v"]
        } for result in data["results"]]
        
        df = pd.DataFrame(aggs)
        df.set_index("Date", inplace=True)
        df.sort_index(inplace=True)
        return df
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

def plot_stock_data(df, ticker):
    """Generate a candlestick chart with volume subplot for the stock data."""
    if df is None or df.empty:
        print("No data to plot.")
        return
    
    # Define plot style
    mc = mpf.make_marketcolors(up="g", down="r", volume="b")
    s = mpf.make_mpf_style(marketcolors=mc, gridstyle="--")
    
    # Create figure with candlestick and volume subplot
    fig, axlist = mpf.plot(
        df,
        type="candle",
        title=f"{ticker} Daily Stock Price",
        ylabel="Price (USD)",
        volume=True,
        ylabel_lower="Volume",
        style=s,
        figsize=(12, 8),
        show_nontrading=False,
        returnfig=True
    )
    
    # Adjust layout and display
    plt.tight_layout()
    plt.show()

def main():
    """Main function to fetch and plot stock data."""
    # Configuration
    ticker = "AAPL"  # Example ticker (Apple)
    end_date = datetime(2025, 6, 4).date()  # Current date (June 4, 2025)
    start_date = end_date - timedelta(days=30)  # 6 months of data
    
    # Get API key
    api_key = "sNHyKFFhnh4wNvmvnpR1nqz51rhO2dVM"
    if not api_key:
        print("POLYGON_API_KEY environment variable not set. Please set it with your Polygon.io API key.")
        return
    
    # Setup session
    session = setup_session()
    
    # Fetch data
    print(f"Fetching data for {ticker} from {start_date} to {end_date}...")
    df = fetch_stock_data(session, ticker, start_date, end_date, api_key)
    
    # Plot data
    if df is not None:
        print("Data fetched successfully. Generating plot...")
        print("\nSample Data:")
        print(df.head())
        plot_stock_data(df, ticker)
    else:
        print("Failed to fetch data.")

if __name__ == "__main__":
    main()