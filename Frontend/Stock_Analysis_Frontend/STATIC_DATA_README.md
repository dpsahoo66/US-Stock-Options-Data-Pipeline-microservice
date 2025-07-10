# Static Data Setup for Frontend Testing

## Overview
This frontend application has been configured to use static data instead of connecting to the Azure database. This allows you to test all frontend features without requiring the backend services or Azure database connection.

## Available Data

### Stock Data
The following stocks are available with realistic historical data:
- **AAPL** (Apple Inc.) - 3 days of data
- **GOOGL** (Alphabet Inc.) - 3 days of data  
- **MSFT** (Microsoft Corporation) - 3 days of data
- **TSLA** (Tesla Inc.) - 3 days of data
- **AMZN** (Amazon.com Inc.) - 3 days of data

### Options Data
Options data is available for:
- **AAPL** - Call and Put options with various strike prices
- **GOOGL** - Call and Put options
- **MSFT** - Call and Put options

## Features Available for Testing

### Dashboard Features
- ✅ Stock summary cards with price, change, and volume
- ✅ Interactive price charts with 1D and 5D filters
- ✅ Options data tables with expiry date filtering
- ✅ List and Straddle view modes for options
- ✅ Search functionality for stocks
- ✅ Responsive design and modern UI

### Data Features
- ✅ Realistic stock prices and volume data
- ✅ Options data with strike prices, bid/ask, and implied volatility
- ✅ Search and filtering capabilities
- ✅ Error handling and loading states

## How to Test

1. **Start the application:**
   ```bash
   cd Frontend/Stock_Analysis_Frontend
   npm install
   ng serve
   ```

2. **Navigate to the dashboard:**
   - Open `http://localhost:4200` in your browser
   - The dashboard will load with AAPL data by default

3. **Test different stocks:**
   - Use the search bar to find different stocks (AAPL, GOOGL, MSFT, TSLA, AMZN)
   - Click on different stocks to see their data

4. **Test options features:**
   - Switch between List and Straddle views
   - Filter by different expiry dates
   - View call and put options data

5. **Test chart features:**
   - Use the 1D and 5D filters on the price chart
   - The chart will show realistic price movements

## Switching Back to Real API

When your Azure database is available again, you can switch back to the real API by:

1. Reverting the `stock-data.service.ts` file to use HTTP requests
2. Adding back `HttpClientModule` to `app-module.ts`
3. Ensuring your backend services are running

## Data Structure

The static data follows the same structure as the real API:
- Stock data includes: symbol, date, open, high, low, close, volume
- Options data includes: contract symbol, strike, bid/ask, volume, implied volatility, etc.
- All data is properly typed with TypeScript interfaces

## Performance

- Data loads instantly (with simulated network delays)
- No external dependencies required
- Perfect for development and testing
- All Angular Material components work as expected 