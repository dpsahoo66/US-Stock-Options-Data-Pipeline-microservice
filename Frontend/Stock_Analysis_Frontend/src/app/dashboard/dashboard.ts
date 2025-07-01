import {
  Component,
  OnInit,
  AfterViewInit,
  ElementRef,
  ViewChild,
  QueryList,
  ViewChildren,
} from '@angular/core';
import * as Highcharts from 'highcharts';

import { StockDataService } from '../services/stock-data.service';
import { StockData } from '../models/stock-data.interface';

interface StockChartData {
  symbol: string;
  data: [number, number][];
  lastPrice: number;
  change: number;
  changePercent: number;
  volume: number;
  hasData: boolean;
  high52Week?: number;
  low52Week?: number;
  marketCap?: string;
}

@Component({
  selector: 'app-dashboard',
  standalone: false,
  templateUrl: './dashboard.html',
  styleUrls: ['./dashboard.scss'],
})
export class Dashboard implements OnInit, AfterViewInit {
  /* ---------------- view state ---------------- */
  stockData: StockData[] = [];
  stockCharts: StockChartData[] = [];
  
  isLoading = false;
  error: string | null = null;
  
  availableStocks: string[] = [];
  displayMode: 'grid' | 'list' = 'grid';
  
  /* -------------- Highcharts refs -------------- */
  @ViewChildren('chartContainer') chartElements!: QueryList<ElementRef<HTMLDivElement>>;
  
  private charts: { [symbol: string]: Highcharts.Chart } = {};
  
  /* -------------- Enhanced mock data for demo -------------- */
  private mockStockData: StockData[] = [
    // AAPL data with more realistic progression
    { symbol: 'AAPL', date: '2024-01-01', open: 185.64, high: 186.95, low: 185.01, close: 185.14, volume: 48360000 },
    { symbol: 'AAPL', date: '2024-01-02', open: 184.98, high: 186.40, low: 183.43, close: 185.84, volume: 58280000 },
    { symbol: 'AAPL', date: '2024-01-03', open: 185.84, high: 187.05, low: 184.35, close: 184.25, volume: 41490000 },
    { symbol: 'AAPL', date: '2024-01-04', open: 184.25, high: 185.99, low: 182.90, close: 181.91, volume: 59130000 },
    { symbol: 'AAPL', date: '2024-01-05', open: 181.91, high: 182.76, low: 180.17, close: 181.18, volume: 58290000 },
    
    // GOOGL data
    { symbol: 'GOOGL', date: '2024-01-01', open: 140.93, high: 142.68, low: 140.54, close: 140.93, volume: 22190000 },
    { symbol: 'GOOGL', date: '2024-01-02', open: 140.69, high: 142.18, low: 139.64, close: 141.80, volume: 23470000 },
    { symbol: 'GOOGL', date: '2024-01-03', open: 141.80, high: 143.29, low: 140.70, close: 140.07, volume: 21850000 },
    { symbol: 'GOOGL', date: '2024-01-04', open: 140.07, high: 141.46, low: 138.83, close: 140.42, volume: 25960000 },
    { symbol: 'GOOGL', date: '2024-01-05', open: 140.42, high: 142.11, low: 139.45, close: 140.85, volume: 19840000 },
    
    // MSFT data
    { symbol: 'MSFT', date: '2024-01-01', open: 376.04, high: 378.24, low: 370.95, close: 373.85, volume: 19520000 },
    { symbol: 'MSFT', date: '2024-01-02', open: 373.85, high: 376.32, low: 371.09, close: 372.78, volume: 20110000 },
    { symbol: 'MSFT', date: '2024-01-03', open: 372.78, high: 374.56, low: 366.10, close: 367.52, volume: 22330000 },
    { symbol: 'MSFT', date: '2024-01-04', open: 367.52, high: 369.89, low: 362.90, close: 365.85, volume: 24710000 },
    { symbol: 'MSFT', date: '2024-01-05', open: 365.85, high: 371.42, low: 364.57, close: 370.73, volume: 21880000 },
    
    // TSLA data
    { symbol: 'TSLA', date: '2024-01-01', open: 250.08, high: 251.82, low: 245.17, close: 248.42, volume: 45680000 },
    { symbol: 'TSLA', date: '2024-01-02', open: 248.42, high: 252.75, low: 245.11, close: 251.05, volume: 42390000 },
    { symbol: 'TSLA', date: '2024-01-03', open: 251.05, high: 254.31, low: 249.25, close: 238.45, volume: 48720000 },
    { symbol: 'TSLA', date: '2024-01-04', open: 238.45, high: 241.28, low: 235.58, close: 237.93, volume: 51210000 },
    { symbol: 'TSLA', date: '2024-01-05', open: 237.93, high: 240.17, low: 235.87, close: 239.24, volume: 39850000 },
    
    // AMZN data
    { symbol: 'AMZN', date: '2024-01-01', open: 151.94, high: 153.32, low: 150.39, close: 151.50, volume: 35640000 },
    { symbol: 'AMZN', date: '2024-01-02', open: 151.50, high: 154.75, low: 150.84, close: 153.40, volume: 34280000 },
    { symbol: 'AMZN', date: '2024-01-03', open: 153.40, high: 155.18, low: 151.90, close: 149.85, volume: 36910000 },
    { symbol: 'AMZN', date: '2024-01-04', open: 149.85, high: 151.92, low: 147.43, close: 148.03, volume: 43730000 },
    { symbol: 'AMZN', date: '2024-01-05', open: 148.03, high: 150.84, low: 146.95, close: 149.43, volume: 38520000 },
    
    // META data
    { symbol: 'META', date: '2024-01-01', open: 353.96, high: 356.08, low: 351.54, close: 352.85, volume: 16830000 },
    { symbol: 'META', date: '2024-01-02', open: 352.85, high: 358.34, low: 350.92, close: 355.17, volume: 17240000 },
    { symbol: 'META', date: '2024-01-03', open: 355.17, high: 359.48, low: 353.28, close: 346.29, volume: 19650000 },
    { symbol: 'META', date: '2024-01-04', open: 346.29, high: 348.75, low: 339.56, close: 344.73, volume: 23180000 },
    { symbol: 'META', date: '2024-01-05', open: 344.73, high: 350.15, low: 342.47, close: 347.56, volume: 18970000 },
  ];

  constructor(private stockDataService: StockDataService) {}

  /* ---------- lifecycle hooks ---------- */
  ngOnInit(): void {
    this.loadStockData();
  }

  ngAfterViewInit(): void {
    // Wait for view to be ready, then render charts
    setTimeout(() => {
      this.renderAllStockCharts();
    }, 500);
  }

  /* ---------- data loading ---------- */
  loadStockData(): void {
    this.isLoading = true;
    this.error = null;

    // Try to load from API first, fallback to mock data
    this.stockDataService.getAllStockData().subscribe({
      next: (response) => {
        if (response.status === 'success' && response.data.stock_data.length > 0) {
          this.stockData = response.data.stock_data;
          this.processStockData();
        } else {
          // Use mock data if no real data available
          this.stockData = this.mockStockData;
          this.processStockData();
        }
        this.isLoading = false;
      },
      error: (err) => {
        console.warn('API unavailable, using mock data:', err);
        // Use mock data when API is unavailable
        this.stockData = this.mockStockData;
        this.processStockData();
        this.isLoading = false;
      },
    });
  }

  processStockData(): void {
    this.extractAvailableStocks();
    this.prepareStockCharts();
  }

  extractAvailableStocks(): void {
    const uniqueStocks = [...new Set(this.stockData.map((i) => i.symbol))];
    this.availableStocks = uniqueStocks.sort();
  }

  prepareStockCharts(): void {
    const mockMarketData = {
      'AAPL': { high52Week: 199.62, low52Week: 164.08, marketCap: '2.83T' },
      'GOOGL': { high52Week: 153.78, low52Week: 121.46, marketCap: '1.78T' },
      'MSFT': { high52Week: 384.30, low52Week: 309.45, marketCap: '2.75T' },
      'TSLA': { high52Week: 278.98, low52Week: 138.80, marketCap: '758.91B' },
      'AMZN': { high52Week: 170.00, low52Week: 118.35, marketCap: '1.56T' },
      'META': { high52Week: 384.33, low52Week: 279.49, marketCap: '878.89B' }
    };

    this.stockCharts = this.availableStocks.map(symbol => {
      const stockDataForSymbol = this.stockData
        .filter(item => item.symbol === symbol)
        .sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime());

      if (stockDataForSymbol.length === 0) {
        return {
          symbol,
          data: [],
          lastPrice: 0,
          change: 0,
          changePercent: 0,
          volume: 0,
          hasData: false,
          high52Week: 0,
          low52Week: 0,
          marketCap: 'N/A'
        };
      }

      const chartData: [number, number][] = stockDataForSymbol.map(item => [
        new Date(item.date).getTime(),
        item.close ?? 0,
      ]);

      const lastItem = stockDataForSymbol[stockDataForSymbol.length - 1];
      const previousItem = stockDataForSymbol[stockDataForSymbol.length - 2];
      
      const lastPrice = lastItem.close ?? 0;
      const previousPrice = previousItem?.close ?? lastPrice;
      const change = lastPrice - previousPrice;
      const changePercent = previousPrice !== 0 ? (change / previousPrice) * 100 : 0;

      const marketInfo = (mockMarketData as any)[symbol] || { high52Week: 0, low52Week: 0, marketCap: 'N/A' };

      return {
        symbol,
        data: chartData,
        lastPrice,
        change,
        changePercent,
        volume: lastItem.volume ?? 0,
        hasData: true,
        ...marketInfo
      };
    });

    // Trigger chart rendering after data is ready
    setTimeout(() => {
      this.renderAllStockCharts();
    }, 100);
  }

  /* ---------- UI handlers ---------- */
  toggleDisplayMode(): void {
    this.displayMode = this.displayMode === 'grid' ? 'list' : 'grid';
    // Re-render charts after layout change
    setTimeout(() => {
      this.renderAllStockCharts();
    }, 300);
  }

  refreshData(): void {
    this.loadStockData();
  }

  /* ---------- chart rendering ---------- */
  renderAllStockCharts(): void {
    if (!this.chartElements || this.chartElements.length === 0) {
      console.log('Chart elements not ready yet');
      return;
    }

    this.chartElements.forEach((chartEl, index) => {
      const stockChart = this.stockCharts[index];
      if (stockChart && chartEl && stockChart.hasData) {
        this.renderStockChart(chartEl.nativeElement, stockChart);
      }
    });
  }

  renderStockChart(container: HTMLDivElement, stockChart: StockChartData): void {
    if (!stockChart.hasData || stockChart.data.length === 0 || !container) {
      return;
    }

    // Destroy existing chart
    if (this.charts[stockChart.symbol]) {
      this.charts[stockChart.symbol].destroy();
    }

    const isPositive = stockChart.change >= 0;
    const chartColor = isPositive ? '#10b981' : '#ef4444'; // Green for positive, Red for negative

    try {
      this.charts[stockChart.symbol] = Highcharts.chart(container, {
        chart: {
          type: 'areaspline',
          backgroundColor: 'transparent',
          height: this.displayMode === 'grid' ? 180 : 120,
          margin: [5, 5, 5, 5],
          spacing: [0, 0, 0, 0]
        },
        title: { text: undefined },
        subtitle: { text: undefined },
        credits: { enabled: false },
        legend: { enabled: false },
        tooltip: {
          enabled: true,
          backgroundColor: 'rgba(0, 0, 0, 0.8)',
          borderColor: chartColor,
          style: { color: '#fff', fontSize: '11px' },
          formatter: function() {
            return `<b>$${(this as any).y.toFixed(2)}</b><br/>${new Date((this as any).x).toLocaleDateString()}`;
          }
        },
        xAxis: {
          type: 'datetime',
          visible: false,
          lineWidth: 0,
          tickLength: 0,
          labels: { enabled: false }
        },
        yAxis: {
          visible: false,
          title: { text: undefined },
          gridLineWidth: 0,
          labels: { enabled: false }
        },
        plotOptions: {
          areaspline: {
            fillOpacity: 0.2,
            lineWidth: 2,
            marker: {
              enabled: false,
              states: {
                hover: { enabled: true, radius: 4 }
              }
            },
            animation: { duration: 1000 }
          }
        },
        series: [{
          type: 'areaspline',
          name: stockChart.symbol,
          data: stockChart.data,
          color: chartColor,
          fillColor: {
            linearGradient: { x1: 0, y1: 0, x2: 0, y2: 1 },
            stops: [
              [0, chartColor + '40'],
              [1, chartColor + '10']
            ]
          }
        }]
      } as any);
    } catch (error) {
      console.error('Error rendering chart for', stockChart.symbol, error);
    }
  }

  /* ---------- utility methods ---------- */
  formatCurrency(value: number): string {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    }).format(value);
  }

  formatNumber(value: number): string {
    if (value >= 1000000000) {
      return (value / 1000000000).toFixed(1) + 'B';
    } else if (value >= 1000000) {
      return (value / 1000000).toFixed(1) + 'M';
    } else if (value >= 1000) {
      return (value / 1000).toFixed(1) + 'K';
    }
    return value.toFixed(0);
  }

  formatPercent(value: number): string {
    return (value >= 0 ? '+' : '') + value.toFixed(2) + '%';
  }

  getChangeClass(change: number): string {
    return change >= 0 ? 'positive-change' : 'negative-change';
  }

  /* ---------- Template helper methods ---------- */
  getActiveStocksCount(): number {
    return this.stockCharts.filter(s => s.hasData).length;
  }

  getGainersCount(): number {
    return this.stockCharts.filter(s => s.change > 0).length;
  }

  getLosersCount(): number {
    return this.stockCharts.filter(s => s.change < 0).length;
  }

  getTotalMarketValue(): string {
    // Calculate total portfolio value for demo
    const totalValue = this.stockCharts.reduce((sum, stock) => sum + stock.lastPrice, 0);
    return this.formatCurrency(totalValue);
  }

  getCompanyName(symbol: string): string {
    const companyNames: { [key: string]: string } = {
      'AAPL': 'Apple Inc.',
      'GOOGL': 'Alphabet Inc.',
      'MSFT': 'Microsoft Corp.',
      'TSLA': 'Tesla Inc.',
      'AMZN': 'Amazon.com Inc.',
      'META': 'Meta Platforms Inc.'
    };
    return companyNames[symbol] || symbol;
  }
}