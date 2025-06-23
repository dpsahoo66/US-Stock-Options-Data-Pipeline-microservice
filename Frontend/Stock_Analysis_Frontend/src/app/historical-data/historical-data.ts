import { Component, OnInit } from '@angular/core';
import { StockDataService, StockDataPoint, StockListResponse } from '../services/stock-data.service';
import * as Highcharts from 'highcharts';
import HighchartsStock from 'highcharts/modules/stock';

// Initialize the stock module
HighchartsStock(Highcharts);

@Component({
  selector: 'app-historical-data',
  standalone: false,
  templateUrl: './historical-data.html',
  styleUrl: './historical-data.scss'
})
export class HistoricalData implements OnInit {
  Highcharts: typeof Highcharts = Highcharts;
  
  selectedStock: string = 'AAPL';
  availableStocks: string[] = [];
  stockData: StockDataPoint[] = [];
  loading: boolean = false;
  error: string = '';
  
  // Chart configuration
  chartOptions: Highcharts.Options = {
    title: {
      text: 'Stock Price History'
    },
    xAxis: {
      type: 'datetime',
      title: {
        text: 'Date'
      }
    },
    yAxis: [{
      title: {
        text: 'Price ($)'
      },
      height: '60%',
      lineWidth: 2,
      resize: {
        enabled: true
      }
    }, {
      title: {
        text: 'Volume'
      },
      top: '65%',
      height: '35%',
      offset: 0,
      lineWidth: 2
    }],
    series: [],
    responsive: {
      rules: [{
        condition: {
          maxWidth: 500
        },
        chartOptions: {
          legend: {
            layout: 'horizontal',
            align: 'center',
            verticalAlign: 'bottom'
          }
        }
      }]
    }
  };

  constructor(private stockDataService: StockDataService) {}

  ngOnInit(): void {
    this.loadAvailableStocks();
    this.loadStockData();
  }

  loadAvailableStocks(): void {
    this.stockDataService.getStockList().subscribe({
      next: (response: StockListResponse) => {
        this.availableStocks = response.stocks;
      },
      error: (error) => {
        console.error('Error loading stock list:', error);
        this.error = 'Failed to load available stocks';
      }
    });
  }

  loadStockData(): void {
    this.loading = true;
    this.error = '';
    
    this.stockDataService.getHistoricalData(this.selectedStock, undefined, undefined, 100).subscribe({
      next: (response) => {
        this.stockData = response.data;
        this.updateChart();
        this.loading = false;
      },
      error: (error) => {
        console.error('Error loading stock data:', error);
        this.error = 'Failed to load stock data';
        this.loading = false;
      }
    });
  }

  onStockChange(): void {
    this.loadStockData();
  }

  updateChart(): void {
    const priceData = this.stockData.map(point => [
      new Date(point.date).getTime(),
      point.close
    ]);

    const volumeData = this.stockData.map(point => [
      new Date(point.date).getTime(),
      point.volume
    ]);

    const ohlcData = this.stockData.map(point => [
      new Date(point.date).getTime(),
      point.open,
      point.high,
      point.low,
      point.close
    ]);

    this.chartOptions = {
      ...this.chartOptions,
      title: {
        text: `${this.selectedStock} Stock Price History`
      },
      series: [
        {
          type: 'candlestick',
          name: `${this.selectedStock} Price`,
          data: ohlcData,
          yAxis: 0
        },
        {
          type: 'column',
          name: 'Volume',
          data: volumeData,
          yAxis: 1,
          color: 'rgba(68, 170, 213, 0.5)'
        }
      ]
    };
  }
}
