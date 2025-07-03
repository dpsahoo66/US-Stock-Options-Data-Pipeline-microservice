import { Injectable } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';
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
  private readonly API_BASE_URL = 'http://localhost:8006';

  constructor(private http: HttpClient) {}

  /**
   * Get all stock data
   */
  getAllStockData(): Observable<ApiResponse<StockDataResponse>> {
    return this.http.get<ApiResponse<StockDataResponse>>(`${this.API_BASE_URL}/api/stock-data/`);
  }

  /**
   * Get all options data (both put and call options)
   */
  getAllOptionsData(): Observable<ApiResponse<OptionsDataResponse>> {
    return this.http.get<ApiResponse<OptionsDataResponse>>(`${this.API_BASE_URL}/api/options-data/`);
  }

  /**
   * Search for stocks and their options by stock name
   */
  searchStock(stockName: string): Observable<ApiResponse<SearchResponse>> {
    const params = new HttpParams().set('stock_name', stockName);
    return this.http.get<ApiResponse<SearchResponse>>(`${this.API_BASE_URL}/api/search/`, { params });
  }

  /**
   * Get stock names matching a pattern
   */
  getStockNames(stockName: string): Observable<ApiResponse<StockNamesResponse>> {
    const params = new HttpParams().set('stock_name', stockName);
    return this.http.get<ApiResponse<StockNamesResponse>>(`${this.API_BASE_URL}/api/stocknames/`, { params });
  }
}