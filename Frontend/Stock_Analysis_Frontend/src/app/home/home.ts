import { Component } from '@angular/core';
import { Router } from '@angular/router';

@Component({
  selector: 'app-home',
  standalone: false,
  templateUrl: './home.html',
  styleUrl: './home.scss'
})
export class Home {
  searchQuery: string = '';
  
  popularStocks = [
    { symbol: 'AAPL', name: 'Apple Inc.' },
    { symbol: 'GOOGL', name: 'Alphabet Inc.' },
    { symbol: 'MSFT', name: 'Microsoft Corporation' },
    { symbol: 'AMZN', name: 'Amazon.com Inc.' },
    { symbol: 'TSLA', name: 'Tesla Inc.' },
    { symbol: 'META', name: 'Meta Platforms Inc.' },
    { symbol: 'NVDA', name: 'NVIDIA Corporation' },
    { symbol: 'NFLX', name: 'Netflix Inc.' }
  ];

  constructor(private router: Router) {}

  searchStock() {
    if (this.searchQuery.trim()) {
      // Navigate to the dashboard with the selected stock
      this.router.navigate(['/dashboard'], { 
        queryParams: { symbol: this.searchQuery.trim().toUpperCase() }
      });
    }
  }

  onKeyPress(event: KeyboardEvent) {
    if (event.key === 'Enter') {
      this.searchStock();
    }
  }

  selectPopularStock(stock: any) {
    this.searchQuery = stock.symbol;
    this.searchStock();
  }
}