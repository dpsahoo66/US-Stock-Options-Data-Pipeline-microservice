import { Component, OnInit, ViewChild } from '@angular/core';
import { MatTableDataSource } from '@angular/material/table';
import { MatPaginator } from '@angular/material/paginator';
import { MatSort } from '@angular/material/sort';
import { StockDataService } from '../services/stock-data.service';
import { StockData } from '../models/stock-data.interface';

@Component({
  selector: 'app-historical-data',
  standalone: false,
  templateUrl: './historical-data.html',
  styleUrl: './historical-data.scss'
})
export class HistoricalData implements OnInit {
  @ViewChild(MatPaginator) paginator!: MatPaginator;
  @ViewChild(MatSort) sort!: MatSort;

  displayedColumns: string[] = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume'];
  dataSource = new MatTableDataSource<StockData>();
  isLoading = false;
  error: string | null = null;
  searchTerm = '';
  availableStocks: string[] = [];
  selectedStock = '';

  constructor(private stockDataService: StockDataService) {}

  ngOnInit(): void {
    this.loadHistoricalData();
  }

  ngAfterViewInit(): void {
    this.dataSource.paginator = this.paginator;
    this.dataSource.sort = this.sort;
  }

  loadHistoricalData(): void {
    this.isLoading = true;
    this.error = null;

    this.stockDataService.getAllStockData().subscribe({
      next: (response) => {
        if (response.status === 'success') {
          const stockData = response.data.stock_data;
          this.dataSource.data = stockData;
          this.extractAvailableStocks(stockData);
        } else {
          this.error = response.message || 'Failed to load historical data';
        }
        this.isLoading = false;
      },
      error: (err) => {
        console.error('Error loading historical data:', err);
        this.error = 'Failed to connect to the API. Please check if the backend is running.';
        this.isLoading = false;
      }
    });
  }

  extractAvailableStocks(stockData: StockData[]): void {
    const uniqueStocks = [...new Set(stockData.map(item => item.symbol))];
    this.availableStocks = ['', ...uniqueStocks.sort()]; // Empty string for "All Stocks"
  }

  onStockFilterChange(): void {
    this.applyFilter();
  }

  onSearchChange(): void {
    this.applyFilter();
  }

  applyFilter(): void {
    const filterValue = this.searchTerm.toLowerCase();
    
    this.dataSource.filterPredicate = (data: StockData, filter: string) => {
      const matchesSearch = !filter || 
        data.symbol.toLowerCase().includes(filter) ||
        data.date.toLowerCase().includes(filter);
      
      const matchesStock = !this.selectedStock || data.symbol === this.selectedStock;
      
      return matchesSearch && matchesStock;
    };
    
    this.dataSource.filter = filterValue;
  }

  searchSpecificStock(): void {
    if (!this.searchTerm.trim()) {
      this.loadHistoricalData();
      return;
    }

    this.isLoading = true;
    this.error = null;

    this.stockDataService.searchStock(this.searchTerm).subscribe({
      next: (response) => {
        if (response.status === 'success') {
          this.dataSource.data = response.data.stock_data;
        } else {
          this.error = response.message || 'No data found for the search term';
          this.dataSource.data = [];
        }
        this.isLoading = false;
      },
      error: (err) => {
        console.error('Error searching stock:', err);
        this.error = 'Failed to search for stock data';
        this.isLoading = false;
      }
    });
  }

  formatCurrency(value: number | null): string {
    if (value === null || value === undefined) return 'N/A';
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD'
    }).format(value);
  }

  formatNumber(value: number | null): string {
    if (value === null || value === undefined) return 'N/A';
    return new Intl.NumberFormat('en-US').format(value);
  }

  exportToCSV(): void {
    const data = this.dataSource.filteredData;
    if (data.length === 0) {
      return;
    }

    const headers = ['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume'];
    const csvContent = [
      headers.join(','),
      ...data.map(row => [
        row.symbol,
        row.date,
        row.open || '',
        row.high || '',
        row.low || '',
        row.close || '',
        row.volume || ''
      ].join(','))
    ].join('\n');

    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `historical-stock-data-${new Date().toISOString().split('T')[0]}.csv`;
    link.click();
    window.URL.revokeObjectURL(url);
  }
}