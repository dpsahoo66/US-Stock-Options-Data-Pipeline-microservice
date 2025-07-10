import { Injectable } from '@angular/core';
import { Observable, of, delay } from 'rxjs';
import { 
  ApiResponse, 
  StockDataResponse, 
  OptionsDataResponse, 
  SearchResponse, 
  StockNamesResponse 
} from '../models/stock-data.interface';

@Injectable({
  providedIn: 'root'
})
export class StockDataService {
  // Static data for testing without Azure database
  private staticStockData = [
    {
      symbol: 'AAPL',
      date: '2025-01-10',
      open: 185.50,
      high: 187.20,
      low: 184.80,
      close: 186.75,
      volume: 45678900
    },
    {
      symbol: 'AAPL',
      date: '2025-01-09',
      open: 184.20,
      high: 186.10,
      low: 183.50,
      close: 185.50,
      volume: 42345600
    },
    {
      symbol: 'AAPL',
      date: '2025-01-08',
      open: 183.00,
      high: 185.30,
      low: 182.40,
      close: 184.20,
      volume: 39876500
    },
    {
      symbol: 'GOOGL',
      date: '2025-01-10',
      open: 142.30,
      high: 144.50,
      low: 141.80,
      close: 143.90,
      volume: 23456700
    },
    {
      symbol: 'GOOGL',
      date: '2025-01-09',
      open: 141.50,
      high: 143.20,
      low: 140.90,
      close: 142.30,
      volume: 21876500
    },
    {
      symbol: 'GOOGL',
      date: '2025-01-08',
      open: 140.20,
      high: 142.10,
      low: 139.60,
      close: 141.50,
      volume: 20123400
    },
    {
      symbol: 'MSFT',
      date: '2025-01-10',
      open: 378.50,
      high: 381.20,
      low: 377.80,
      close: 380.15,
      volume: 34567800
    },
    {
      symbol: 'MSFT',
      date: '2025-01-09',
      open: 376.80,
      high: 379.50,
      low: 375.90,
      close: 378.50,
      volume: 32109800
    },
    {
      symbol: 'MSFT',
      date: '2025-01-08',
      open: 375.20,
      high: 377.80,
      low: 374.10,
      close: 376.80,
      volume: 29876500
    },
    {
      symbol: 'TSLA',
      date: '2025-01-10',
      open: 248.50,
      high: 252.30,
      low: 247.20,
      close: 251.80,
      volume: 56789000
    },
    {
      symbol: 'TSLA',
      date: '2025-01-09',
      open: 246.80,
      high: 250.10,
      low: 245.50,
      close: 248.50,
      volume: 54321000
    },
    {
      symbol: 'TSLA',
      date: '2025-01-08',
      open: 245.20,
      high: 248.90,
      low: 244.10,
      close: 246.80,
      volume: 51234500
    },
    {
      symbol: 'AMZN',
      date: '2025-01-10',
      open: 155.30,
      high: 157.80,
      low: 154.60,
      close: 156.90,
      volume: 45678900
    },
    {
      symbol: 'AMZN',
      date: '2025-01-09',
      open: 154.10,
      high: 156.50,
      low: 153.40,
      close: 155.30,
      volume: 43210900
    },
    {
      symbol: 'AMZN',
      date: '2025-01-08',
      open: 152.80,
      high: 155.20,
      low: 152.10,
      close: 154.10,
      volume: 40987600
    }
  ];

  private staticOptionsData = {
    call_options: [
      {
        contractSymbol: 'AAPL250110C00185000',
        lastTradeDate: '2025-01-10',
        expirationDate: '2025-01-17',
        strike: 185.00,
        lastPrice: 2.45,
        bid: 2.40,
        ask: 2.50,
        change: 0.15,
        percentChange: 6.52,
        volume: 1234,
        openInterest: 5678,
        impliedVolatility: 0.25,
        inTheMoney: true,
        contractSize: '100',
        currency: 'USD',
        StockName: 'AAPL'
      },
      {
        contractSymbol: 'AAPL250110C00190000',
        lastTradeDate: '2025-01-10',
        expirationDate: '2025-01-17',
        strike: 190.00,
        lastPrice: 0.85,
        bid: 0.80,
        ask: 0.90,
        change: -0.05,
        percentChange: -5.56,
        volume: 2345,
        openInterest: 3456,
        impliedVolatility: 0.22,
        inTheMoney: false,
        contractSize: '100',
        currency: 'USD',
        StockName: 'AAPL'
      },
      {
        contractSymbol: 'AAPL250110C00195000',
        lastTradeDate: '2025-01-10',
        expirationDate: '2025-01-17',
        strike: 195.00,
        lastPrice: 0.25,
        bid: 0.20,
        ask: 0.30,
        change: 0.05,
        percentChange: 25.00,
        volume: 3456,
        openInterest: 2345,
        impliedVolatility: 0.20,
        inTheMoney: false,
        contractSize: '100',
        currency: 'USD',
        StockName: 'AAPL'
      },
      {
        contractSymbol: 'GOOGL250110C00145000',
        lastTradeDate: '2025-01-10',
        expirationDate: '2025-01-17',
        strike: 145.00,
        lastPrice: 1.20,
        bid: 1.15,
        ask: 1.25,
        change: 0.10,
        percentChange: 9.09,
        volume: 1234,
        openInterest: 2345,
        impliedVolatility: 0.23,
        inTheMoney: false,
        contractSize: '100',
        currency: 'USD',
        StockName: 'GOOGL'
      },
      {
        contractSymbol: 'MSFT250110C00380000',
        lastTradeDate: '2025-01-10',
        expirationDate: '2025-01-17',
        strike: 380.00,
        lastPrice: 2.80,
        bid: 2.75,
        ask: 2.85,
        change: 0.20,
        percentChange: 7.69,
        volume: 2345,
        openInterest: 3456,
        impliedVolatility: 0.24,
        inTheMoney: true,
        contractSize: '100',
        currency: 'USD',
        StockName: 'MSFT'
      }
    ],
    put_options: [
      {
        contractSymbol: 'AAPL250110P00185000',
        lastTradeDate: '2025-01-10',
        expirationDate: '2025-01-17',
        strike: 185.00,
        lastPrice: 1.80,
        bid: 1.75,
        ask: 1.85,
        change: -0.10,
        percentChange: -5.26,
        volume: 1234,
        openInterest: 3456,
        impliedVolatility: 0.26,
        inTheMoney: false,
        contractSize: '100',
        currency: 'USD',
        StockName: 'AAPL'
      },
      {
        contractSymbol: 'AAPL250110P00180000',
        lastTradeDate: '2025-01-10',
        expirationDate: '2025-01-17',
        strike: 180.00,
        lastPrice: 0.95,
        bid: 0.90,
        ask: 1.00,
        change: 0.05,
        percentChange: 5.56,
        volume: 2345,
        openInterest: 2345,
        impliedVolatility: 0.24,
        inTheMoney: false,
        contractSize: '100',
        currency: 'USD',
        StockName: 'AAPL'
      },
      {
        contractSymbol: 'AAPL250110P00175000',
        lastTradeDate: '2025-01-10',
        expirationDate: '2025-01-17',
        strike: 175.00,
        lastPrice: 0.45,
        bid: 0.40,
        ask: 0.50,
        change: 0.10,
        percentChange: 28.57,
        volume: 3456,
        openInterest: 1234,
        impliedVolatility: 0.22,
        inTheMoney: false,
        contractSize: '100',
        currency: 'USD',
        StockName: 'AAPL'
      },
      {
        contractSymbol: 'GOOGL250110P00145000',
        lastTradeDate: '2025-01-10',
        expirationDate: '2025-01-17',
        strike: 145.00,
        lastPrice: 1.05,
        bid: 1.00,
        ask: 1.10,
        change: -0.05,
        percentChange: -4.55,
        volume: 1234,
        openInterest: 2345,
        impliedVolatility: 0.25,
        inTheMoney: false,
        contractSize: '100',
        currency: 'USD',
        StockName: 'GOOGL'
      },
      {
        contractSymbol: 'MSFT250110P00380000',
        lastTradeDate: '2025-01-10',
        expirationDate: '2025-01-17',
        strike: 380.00,
        lastPrice: 2.15,
        bid: 2.10,
        ask: 2.20,
        change: 0.15,
        percentChange: 7.50,
        volume: 2345,
        openInterest: 3456,
        impliedVolatility: 0.27,
        inTheMoney: false,
        contractSize: '100',
        currency: 'USD',
        StockName: 'MSFT'
      }
    ]
  };

  constructor() {}

  /**
   * Get all stock data
   */
  getAllStockData(): Observable<ApiResponse<StockDataResponse>> {
    const response: ApiResponse<StockDataResponse> = {
      status: 'success',
      data: {
        stock_data: this.staticStockData,
        total_db1_rows: this.staticStockData.length
      }
    };
    return of(response).pipe(delay(500)); // Simulate network delay
  }

  /**
   * Get all options data (both put and call options)
   */
  getAllOptionsData(): Observable<ApiResponse<OptionsDataResponse>> {
    const response: ApiResponse<OptionsDataResponse> = {
      status: 'success',
      data: this.staticOptionsData
    };
    return of(response).pipe(delay(300)); // Simulate network delay
  }

  /**
   * Search for stocks and their options by stock name
   */
  searchStock(stockName: string): Observable<ApiResponse<SearchResponse>> {
    const filteredStockData = this.staticStockData.filter(stock => 
      stock.symbol.toLowerCase().includes(stockName.toLowerCase())
    );
    
    const filteredCallOptions = this.staticOptionsData.call_options.filter(option => 
      option.StockName.toLowerCase().includes(stockName.toLowerCase())
    );
    
    const filteredPutOptions = this.staticOptionsData.put_options.filter(option => 
      option.StockName.toLowerCase().includes(stockName.toLowerCase())
    );

    const response: ApiResponse<SearchResponse> = {
      status: 'success',
      data: {
        stock_data: filteredStockData,
        put_options: filteredPutOptions,
        call_options: filteredCallOptions,
        total_stock_rows: filteredStockData.length,
        total_put_rows: filteredPutOptions.length,
        total_call_rows: filteredCallOptions.length
      }
    };
    return of(response).pipe(delay(400)); // Simulate network delay
  }

  /**
   * Get stock names matching a pattern
   */
  getStockNames(stockName: string): Observable<ApiResponse<StockNamesResponse>> {
    const uniqueSymbols = [...new Set(this.staticStockData.map(stock => stock.symbol))];
    const filteredSymbols = uniqueSymbols.filter(symbol => 
      symbol.toLowerCase().includes(stockName.toLowerCase())
    );
    
    const response: ApiResponse<StockNamesResponse> = {
      status: 'success',
      data: {
        stock_data: filteredSymbols.map(symbol => ({ stocknames: symbol })),
        total_stock_rows: filteredSymbols.length
      }
    };
    return of(response).pipe(delay(200)); // Simulate network delay
  }

  /**
   * Get stock data for a specific symbol
   */
  getStockDataBySymbol(symbol: string): Observable<ApiResponse<StockDataResponse>> {
    const filteredStockData = this.staticStockData.filter(stock => 
      stock.symbol.toUpperCase() === symbol.toUpperCase()
    );
    
    const response: ApiResponse<StockDataResponse> = {
      status: 'success',
      data: {
        stock_data: filteredStockData,
        total_db1_rows: filteredStockData.length
      }
    };
    return of(response).pipe(delay(300)); // Simulate network delay
  }
}