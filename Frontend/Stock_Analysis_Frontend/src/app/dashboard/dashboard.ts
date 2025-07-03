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
  selector: 'app-dashboard',
  /* still part of an NgModule, so standalone stays false */
  standalone: false,
  templateUrl: './dashboard.html',
  styleUrls: ['./dashboard.scss'],
})
export class Dashboard implements OnInit, AfterViewInit {
  /* ---------------- view state ---------------- */
  stockData: StockData[] = [];
  chartData: [number, number][] = [];

  isLoading = false;
  error: string | null = null;

  selectedStock = 'MMM';          // default symbol
  availableStocks: string[] = [];

  /* -------------- Highcharts refs -------------- */
  @ViewChild('chartContainer', { static: false })
  chartEl!: ElementRef<HTMLDivElement>;

  private chart?: Highcharts.Chart;

  constructor(private stockDataService: StockDataService) {}

  /* ---------- lifecycle hooks ---------- */
  ngOnInit(): void {
    this.loadStockData();
  }

  /** Called once Angular has rendered the template */
  ngAfterViewInit(): void {
    if (this.chartData.length) {
      this.renderStockChart();
    }
  }

  /* ---------- data loading ---------- */
  loadStockData(): void {
    this.isLoading = true;
    this.error = null;

    this.stockDataService.getAllStockData().subscribe({
      next: (response) => {
        if (response.status === 'success') {
          this.stockData = response.data.stock_data;
          this.extractAvailableStocks();
          this.updateChartData();
          /* element now exists if user stayed on dashboard tab */
          if (this.chartEl) {
            this.renderStockChart();
          }
        } else {
          this.error = response.message || 'Failed to load stock data';
        }
        this.isLoading = false;
      },
      error: (err) => {
        console.error('Error loading stock data:', err);
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

  /* ---------- helpers ---------- */
  updateChartData(): void {
    const filtered = this.stockData
      .filter((i) => i.symbol === this.selectedStock)
      .sort(
        (a, b) =>
          new Date(a.date).getTime() - new Date(b.date).getTime()
      );

    this.chartData = filtered.map((i) => [
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

    this.chart = Highcharts.stockChart(this.chartEl.nativeElement, {
      chart: {
        type: 'areaspline',
        backgroundColor: 'transparent',
      },
      title: {
        text: `${this.selectedStock} Stock Analysis`,
        style: { color: '#fff' },
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
      },
      navigator : { enabled: false },
      scrollbar : { enabled: false },
      credits   : { enabled: false },
      yAxis: {
        opposite: false,
        title: { text: 'Price ($)', style: { color: '#fff' } },
        labels: { style: { color: '#fff' } },
      },
      xAxis: { labels: { style: { color: '#fff' } } },
      series: [
        {
          type : 'areaspline',
          name : `${this.selectedStock} Price`,
          data : this.chartData,
          fillOpacity: 0.2,
          color: '#ffbf00',
          tooltip: { valueDecimals: 2, valuePrefix: '$' },
        },
      ],
    } as Highcharts.Options);
  }
}
