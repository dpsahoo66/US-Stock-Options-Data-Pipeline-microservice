import { Component, OnInit, ViewChild } from '@angular/core';
import { MatTableDataSource } from '@angular/material/table';
import { MatPaginator } from '@angular/material/paginator';
import { MatSort } from '@angular/material/sort';
import { StockDataService } from '../services/stock-data.service';
import { OptionData } from '../models/stock-data.interface';

@Component({
  selector: 'app-options-data',
  standalone: false,
  templateUrl: './options-data.html',
  styleUrl: './options-data.scss'
})
export class OptionsData implements OnInit {
  @ViewChild('putPaginator') putPaginator!: MatPaginator;
  @ViewChild('callPaginator') callPaginator!: MatPaginator;
  @ViewChild('putSort') putSort!: MatSort;
  @ViewChild('callSort') callSort!: MatSort;

  displayedColumns: string[] = [
    'contractSymbol', 'StockName', 'expirationDate', 'strike', 'lastPrice', 
    'bid', 'ask', 'change', 'percentChange', 'volume', 'openInterest', 'impliedVolatility', 'inTheMoney'
  ];
  
  putOptionsDataSource = new MatTableDataSource<OptionData>();
  callOptionsDataSource = new MatTableDataSource<OptionData>();
  
  isLoading = false;
  error: string | null = null;
  searchTerm = '';
  selectedTab = 0;
  
  putOptionsCount = 0;
  callOptionsCount = 0;

  constructor(private stockDataService: StockDataService) {}

  ngOnInit(): void {
    this.loadOptionsData();
  }

  ngAfterViewInit(): void {
    this.putOptionsDataSource.paginator = this.putPaginator;
    this.putOptionsDataSource.sort = this.putSort;
    this.callOptionsDataSource.paginator = this.callPaginator;
    this.callOptionsDataSource.sort = this.callSort;
  }

  loadOptionsData(): void {
    this.isLoading = true;
    this.error = null;

    this.stockDataService.getAllOptionsData().subscribe({
      next: (response) => {
        if (response.status === 'success') {
          this.putOptionsDataSource.data = response.data.put_options;
          this.callOptionsDataSource.data = response.data.call_options;
          this.putOptionsCount = response.data.put_options.length;
          this.callOptionsCount = response.data.call_options.length;
        } else {
          this.error = response.message || 'Failed to load options data';
        }
        this.isLoading = false;
      },
      error: (err) => {
        console.error('Error loading options data:', err);
        this.error = 'Failed to connect to the API. Please check if the backend is running.';
        this.isLoading = false;
      }
    });
  }

  onSearchChange(): void {
    this.applyFilter();
  }

  applyFilter(): void {
    const filterValue = this.searchTerm.toLowerCase();
    
    const filterPredicate = (data: OptionData, filter: string) => {
      return !filter || 
        data.contractSymbol.toLowerCase().includes(filter) ||
        data.StockName.toLowerCase().includes(filter) ||
        data.expirationDate.toLowerCase().includes(filter);
    };
    
    this.putOptionsDataSource.filterPredicate = filterPredicate;
    this.callOptionsDataSource.filterPredicate = filterPredicate;
    
    this.putOptionsDataSource.filter = filterValue;
    this.callOptionsDataSource.filter = filterValue;
  }

  searchSpecificOptions(): void {
    if (!this.searchTerm.trim()) {
      this.loadOptionsData();
      return;
    }

    this.isLoading = true;
    this.error = null;

    this.stockDataService.searchStock(this.searchTerm).subscribe({
      next: (response) => {
        if (response.status === 'success') {
          this.putOptionsDataSource.data = response.data.put_options;
          this.callOptionsDataSource.data = response.data.call_options;
          this.putOptionsCount = response.data.put_options.length;
          this.callOptionsCount = response.data.call_options.length;
        } else {
          this.error = response.message || 'No options data found for the search term';
          this.putOptionsDataSource.data = [];
          this.callOptionsDataSource.data = [];
          this.putOptionsCount = 0;
          this.callOptionsCount = 0;
        }
        this.isLoading = false;
      },
      error: (err) => {
        console.error('Error searching options:', err);
        this.error = 'Failed to search for options data';
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

  formatPercentage(value: number | null): string {
    if (value === null || value === undefined) return 'N/A';
    return new Intl.NumberFormat('en-US', {
      style: 'percent',
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    }).format(value / 100);
  }

  getChangeClass(change: number | null): string {
    if (change === null || change === undefined) return '';
    return change >= 0 ? 'positive-change' : 'negative-change';
  }

  exportToCSV(optionType: 'put' | 'call'): void {
    const dataSource = optionType === 'put' ? this.putOptionsDataSource : this.callOptionsDataSource;
    const data = dataSource.filteredData;
    
    if (data.length === 0) {
      return;
    }

    const headers = [
      'Contract Symbol', 'Stock Name', 'Expiration Date', 'Strike', 'Last Price',
      'Bid', 'Ask', 'Change', 'Percent Change', 'Volume', 'Open Interest',
      'Implied Volatility', 'In The Money'
    ];
    
    const csvContent = [
      headers.join(','),
      ...data.map(row => [
        row.contractSymbol,
        row.StockName,
        row.expirationDate,
        row.strike || '',
        row.lastPrice || '',
        row.bid || '',
        row.ask || '',
        row.change || '',
        row.percentChange || '',
        row.volume || '',
        row.openInterest || '',
        row.impliedVolatility || '',
        row.inTheMoney ? 'Yes' : 'No'
      ].join(','))
    ].join('\n');

    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `${optionType}-options-data-${new Date().toISOString().split('T')[0]}.csv`;
    link.click();
    window.URL.revokeObjectURL(url);
  }

  onTabChange(index: number): void {
    this.selectedTab = index;
  }
}