import {
  Component,
  OnInit,
  AfterViewInit,
  ElementRef,
  ViewChild,
} from '@angular/core';
import * as Highcharts from 'highcharts/highstock';

import { StockDataService } from '../services/stock-data.service';
import { StockData } from '../models/stock-data.interface';

@Component({
  selector: 'app-historical-data',
  standalone: false,
  templateUrl: './historical-data.html',
  styleUrl: './historical-data.scss'
})
export class HistoricalData implements OnInit, AfterViewInit {
  /* ---------------- view state ---------------- */
  stockData: StockData[] = [];
  chartData: [number, number][] = [];

  isLoading = false;
  error: string | null = null;
  searchTerm = '';
  availableStocks: string[] = [];
  selectedStock = '';

  /* -------------- Highcharts refs -------------- */
  @ViewChild('chartContainer', { static: false })
  chartEl!: ElementRef<HTMLDivElement>;

  private chart?: Highcharts.Chart;

  constructor(private stockDataService: StockDataService) {}

  /* ---------- lifecycle hooks ---------- */
  ngOnInit(): void {
    this.loadHistoricalData();
  }

  /** Called once Angular has rendered the template */
  ngAfterViewInit(): void {
    if (this.chartData.length) {
      this.renderStockChart();
    }
  }

  /* ---------- data loading ---------- */
  loadHistoricalData(): void {
    this.isLoading = true;
    this.error = null;

    this.stockDataService.getAllStockData().subscribe({
      next: (response) => {
        if (response.status === 'success') {
          this.stockData = response.data.stock_data;
          this.extractAvailableStocks();
          this.updateChartData();
          /* element now exists if user stayed on historical data tab */
          if (this.chartEl) {
            this.renderStockChart();
          }
        } else {
          this.error = response.message || 'Failed to load historical data';
        }
        this.isLoading = false;
      },
      error: (err) => {
        console.error('Error loading historical data:', err);
        this.error =
          'Failed to connect to the API. Please check if the backend is running.';
        this.isLoading = false;
      },
    });
  }

  extractAvailableStocks(): void {
    const uniqueStocks = [...new Set(this.stockData.map((i) => i.symbol))];
    this.availableStocks = uniqueStocks.sort();
  }

  /* ---------- UI handlers ---------- */
  onStockChange(selectedStock: string): void {
    this.selectedStock = selectedStock;
    this.updateChartData();
    this.renderStockChart();
  }

  onSearchChange(): void {
    // Filter available stocks based on search term
    if (this.searchTerm.trim()) {
      this.updateChartData();
      this.renderStockChart();
    }
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
          this.stockData = response.data.stock_data;
          this.extractAvailableStocks();
          this.selectedStock = this.searchTerm.toUpperCase();
          this.updateChartData();
          if (this.chartEl) {
            this.renderStockChart();
          }
        } else {
          this.error = response.message || 'No data found for the search term';
          this.stockData = [];
          this.chartData = [];
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

  /* ---------- helpers ---------- */
  updateChartData(): void {
    let filteredData = this.stockData;

    // Filter by selected stock
    if (this.selectedStock) {
      filteredData = filteredData.filter((i) => i.symbol === this.selectedStock);
    }

    // Filter by search term if no specific stock is selected
    if (this.searchTerm.trim() && !this.selectedStock) {
      const searchLower = this.searchTerm.toLowerCase();
      filteredData = filteredData.filter((i) => 
        i.symbol.toLowerCase().includes(searchLower)
      );
    }

    const sorted = filteredData.sort(
      (a, b) => new Date(a.date).getTime() - new Date(b.date).getTime()
    );

    this.chartData = sorted.map((i) => [
      new Date(i.date).getTime(),
      i.close ?? 0,
    ]);
  }

  renderStockChart(): void {
    /* guard against empty data or missing element */
    if (!this.chartData.length || !this.chartEl) {
      return;
    }

    /* destroy old chart when switching symbols */
    if (this.chart) {
      this.chart.destroy();
    }

    const chartTitle = this.selectedStock 
      ? `${this.selectedStock} Historical Stock Analysis`
      : 'Historical Stock Data Analysis';

    this.chart = Highcharts.stockChart(this.chartEl.nativeElement, {
      chart: {
        type: 'areaspline',
        backgroundColor: 'transparent',
      },
      title: {
        text: chartTitle,
        style: { color: '#fff', fontSize: '18px' },
      },
      subtitle: {
        text: 'View historical stock price movements over time',
        style: { color: 'rgba(255, 255, 255, 0.8)' },
      },
      rangeSelector: {
        selected: 3,
        buttons: [
          { type: 'month',  count: 1, text: '1m', title: 'View 1 month' },
          { type: 'month',  count: 3, text: '3m', title: 'View 3 months' },
          { type: 'month',  count: 6, text: '6m', title: 'View 6 months' },
          { type: 'ytd',                text: 'YTD', title: 'View year to date' },
          { type: 'year',   count: 1, text: '1y', title: 'View 1 year' },
          { type: 'all',                text: 'All', title: 'View all' },
        ],
        buttonTheme: {
          fill: 'rgba(255, 255, 255, 0.1)',
          stroke: 'rgba(255, 255, 255, 0.3)',
          style: { color: '#fff' },
          states: {
            hover: { fill: 'rgba(255, 191, 0, 0.2)' },
            select: { fill: '#ffbf00', style: { color: '#000' } }
          }
        }
      },
      navigator: { 
        enabled: true,
        handles: {
          backgroundColor: '#ffbf00',
          borderColor: '#fff'
        },
        series: {
          color: '#ffbf00',
          fillOpacity: 0.1
        }
      },
      scrollbar: { 
        enabled: true,
        barBackgroundColor: 'rgba(255, 255, 255, 0.1)',
        barBorderColor: 'rgba(255, 255, 255, 0.3)',
        buttonBackgroundColor: 'rgba(255, 255, 255, 0.1)',
        buttonBorderColor: 'rgba(255, 255, 255, 0.3)',
        rifleColor: '#fff',
        trackBackgroundColor: 'rgba(0, 0, 0, 0.1)'
      },
      credits: { enabled: false },
      yAxis: {
        opposite: false,
        title: { text: 'Price ($)', style: { color: '#fff' } },
        labels: { style: { color: '#fff' } },
        gridLineColor: 'rgba(255, 255, 255, 0.1)'
      },
      xAxis: { 
        labels: { style: { color: '#fff' } },
        gridLineColor: 'rgba(255, 255, 255, 0.1)'
      },
      series: [
        {
          type: 'areaspline',
          name: this.selectedStock ? `${this.selectedStock} Price` : 'Stock Price',
          data: this.chartData,
          fillOpacity: 0.3,
          color: '#ffbf00',
          fillColor: {
            linearGradient: { x1: 0, y1: 0, x2: 0, y2: 1 },
            stops: [
              [0, 'rgba(255, 191, 0, 0.3)'],
              [1, 'rgba(255, 191, 0, 0.05)']
            ]
          },
          tooltip: { valueDecimals: 2, valuePrefix: '$' },
        },
      ],
    } as Highcharts.Options);
  }

  exportToCSV(): void {
    if (this.chartData.length === 0) {
      return;
    }

    let dataToExport = this.stockData;
    
    // Filter data based on current view
    if (this.selectedStock) {
      dataToExport = dataToExport.filter(item => item.symbol === this.selectedStock);
    }

    if (this.searchTerm.trim() && !this.selectedStock) {
      const searchLower = this.searchTerm.toLowerCase();
      dataToExport = dataToExport.filter(item => 
        item.symbol.toLowerCase().includes(searchLower)
      );
    }

    const headers = ['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume'];
    const csvContent = [
      headers.join(','),
      ...dataToExport.map(row => [
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
    const filename = this.selectedStock 
      ? `${this.selectedStock}-historical-data-${new Date().toISOString().split('T')[0]}.csv`
      : `historical-stock-data-${new Date().toISOString().split('T')[0]}.csv`;
    link.download = filename;
    link.click();
    window.URL.revokeObjectURL(url);
  }
}