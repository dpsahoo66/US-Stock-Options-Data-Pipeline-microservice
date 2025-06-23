import { Injectable } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';

export interface StockDataPoint {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface StockDataResponse {
  stock_name: string;
  data: StockDataPoint[];
  total_records: number;
}

export interface StockListResponse {
  stocks: string[];
  total: number;
}

export interface StockSummary {
  stock_name: string;
  total_records: number;
  earliest_date: string;
  latest_date: string;
  average_close: number;
  min_low: number;
  max_high: number;
  total_volume: number;
}

@Injectable({
  providedIn: 'root'
})
export class StockDataService {
  private apiUrl = 'http://localhost:8000/api';

  constructor(private http: HttpClient) { }

  getStockList(): Observable<StockListResponse> {
    return this.http.get<StockListResponse>(`${this.apiUrl}/stocks/list`);
  }

  getHistoricalData(
    stockSymbol: string, 
    startDate?: string, 
    endDate?: string, 
    limit?: number
  ): Observable<StockDataResponse> {
    let params = new HttpParams();
    
    if (startDate) params = params.set('start_date', startDate);
    if (endDate) params = params.set('end_date', endDate);
    if (limit) params = params.set('limit', limit.toString());

    return this.http.get<StockDataResponse>(`${this.apiUrl}/stocks/${stockSymbol}/historical`, { params });
  }

  getStockSummary(stockSymbol: string): Observable<StockSummary> {
    return this.http.get<StockSummary>(`${this.apiUrl}/stocks/${stockSymbol}/summary`);
  }

  testConnection(): Observable<any> {
    return this.http.get(`${this.apiUrl}/test-connection`);
  }
}