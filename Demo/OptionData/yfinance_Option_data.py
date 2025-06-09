# yfinance is used to retrieve the options chain (calls and puts) for stock, including strike prices, bid/ask prices, implied volatility, etc., for a specific expiration date.
import yfinance as yf

ticker = yf.Ticker("AAPL")
expirations = ticker.options
opt_chain = ticker.option_chain(expirations[0])
print(opt_chain)