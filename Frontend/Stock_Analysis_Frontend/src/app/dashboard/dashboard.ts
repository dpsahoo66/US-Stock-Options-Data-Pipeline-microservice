import { Component } from '@angular/core';
import * as Highcharts from 'highcharts';

@Component({
  selector: 'app-dashboard',
  standalone: false,
  templateUrl: './dashboard.html',
  styleUrl: './dashboard.scss'
})

export class Dashboard {
  ngOnInit(): void {
    this.renderSplineChart(); // Called when the component loads
  }

  renderSplineChart(): void {
    Highcharts.chart("stock", {
      chart: {
        type: 'spline'
      },
      title: {
        text: 'Stock Price Trends'
      },
      xAxis: {
        categories: ['Mon', 'Tue', 'Wed', 'Thu', 'Fri']
      },
      yAxis: {
        title: {
          text: 'Price ($)'
        }
      },
      series: [{
        name: 'AAPL',
        type: 'spline',
        data: [150, 155, 160, 158, 165]
      }]
    } as any);
  }
}



// import { Component } from '@angular/core';
// // Import Highcharts base
// import * as Highcharts from 'highcharts';
// // Import Highstock
// import HighchartsStock from 'highcharts/highstock';
// // Import exporting module (optional)
// import HC_exporting from 'highcharts/modules/exporting';

// // Initialize exporting module
// HC_exporting(Highcharts);

// // Extend Highcharts with Highstock
// HighchartsStock(Highcharts);

// @Component({
//   selector: 'app-dashboard',
//   standalone: false,
//   templateUrl: './dashboard.html',
//   styleUrl: './dashboard.scss'
// })
// export class Dashboard {
//   ngOnInit(): void {
//     this.renderStockChart();
//   }

//   renderStockChart(): void {
//     // Use the extended Highcharts object
//     Highcharts.stockChart('container', {
//       rangeSelector: {
//         buttons: [{
//           type: 'day',
//           count: 1,
//           text: '1d'
//         }, {
//           type: 'day',
//           count: 5,
//           text: '5d'
//         }, {
//           type: 'month',
//           count: 1,
//           text: '1m'
//         }, {
//           type: 'month',
//           count: 6,
//           text: '6m'
//         }, {
//           type: 'year',
//           count: 1,
//           text: 'YTD'
//         }, {
//           type: 'year',
//           count: 1,
//           text: '1y'
//         }, {
//           type: 'year',
//           count: 5,
//           text: '5y'
//         }, {
//           type: 'all',
//           text: 'All'
//         }],
//         selected: 0
//       },
//       title: {
//         text: 'Stock Price Trend'
//       },
//       series: [{
//         name: 'AAPL Stock Price',
//         data: [
//           [Date.UTC(2023, 0, 1), 150],
//           [Date.UTC(2023, 0, 2), 155],
//           [Date.UTC(2023, 0, 3), 160],
//           [Date.UTC(2023, 0, 4), 158],
//           [Date.UTC(2023, 0, 5), 162],
//           [Date.UTC(2023, 0, 6), 165],
//           [Date.UTC(2023, 0, 7), 170],
//           [Date.UTC(2023, 0, 8), 168],
//           [Date.UTC(2023, 0, 9), 172],
//           [Date.UTC(2023, 0, 10), 175],
//           [Date.UTC(2023, 0, 11), 180],
//           [Date.UTC(2023, 0, 12), 178],
//           [Date.UTC(2023, 0, 13), 182],
//           [Date.UTC(2023, 0, 14), 185],
//           [Date.UTC(2023, 0, 15), 190]
//         ],
//         tooltip: {
//           valueDecimals: 2
//         }
//       }]
//     });
//   }
// }



