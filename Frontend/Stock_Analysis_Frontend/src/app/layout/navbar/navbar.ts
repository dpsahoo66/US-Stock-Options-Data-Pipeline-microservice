import { Component, OnInit, OnDestroy } from '@angular/core';
import { Router } from '@angular/router';
import { Subject, debounceTime, distinctUntilChanged, takeUntil } from 'rxjs';
import { StockDataService } from '../../services/stock-data.service';

@Component({
  selector: 'app-navbar',
  standalone: false,
  templateUrl: './navbar.html',
  styleUrl: './navbar.scss'
})
export class Navbar implements OnInit, OnDestroy {
  searchTerm: string = '';
  searchResults: any[] = [];
  isSearching: boolean = false;
  showSearchResults: boolean = false;
  
  private searchSubject = new Subject<string>();
  private destroy$ = new Subject<void>();

  constructor(
    private router: Router,
    private stockDataService: StockDataService
  ) {}

  ngOnInit(): void {
    // Setup debounced search
    this.searchSubject.pipe(
      debounceTime(300),
      distinctUntilChanged(),
      takeUntil(this.destroy$)
    ).subscribe(searchTerm => {
      if (searchTerm.trim()) {
        this.performSearch(searchTerm);
      } else {
        this.clearSearch();
      }
    });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  onSearchInput(): void {
    this.searchSubject.next(this.searchTerm);
  }

  onSearch(): void {
    if (this.searchTerm.trim()) {
      this.performSearch(this.searchTerm);
    }
  }

  performSearch(searchTerm: string): void {
    this.isSearching = true;
    this.showSearchResults = true;

    // Search for stock names first
    this.stockDataService.getStockNames(searchTerm).subscribe({
      next: (response) => {
        if (response.status === 'success') {
          this.searchResults = response.data.stock_data.map(item => ({
            type: 'stock',
            symbol: item.stocknames,
            name: item.stocknames,
            description: 'Stock Symbol'
          }));
        }
        this.isSearching = false;
      },
      error: (err) => {
        console.error('Search error:', err);
        // Fallback to mock search results
        this.searchResults = this.getMockSearchResults(searchTerm);
        this.isSearching = false;
      }
    });
  }

  getMockSearchResults(searchTerm: string): any[] {
    const mockStocks = [
      { symbol: 'AAPL', name: 'Apple Inc.', description: 'Technology' },
      { symbol: 'GOOGL', name: 'Alphabet Inc.', description: 'Technology' },
      { symbol: 'MSFT', name: 'Microsoft Corporation', description: 'Technology' },
      { symbol: 'TSLA', name: 'Tesla Inc.', description: 'Automotive' },
      { symbol: 'AMZN', name: 'Amazon.com Inc.', description: 'E-commerce' },
      { symbol: 'META', name: 'Meta Platforms Inc.', description: 'Social Media' },
      { symbol: 'NVDA', name: 'NVIDIA Corporation', description: 'Technology' },
      { symbol: 'JPM', name: 'JPMorgan Chase & Co.', description: 'Banking' },
      { symbol: 'JNJ', name: 'Johnson & Johnson', description: 'Healthcare' },
      { symbol: 'V', name: 'Visa Inc.', description: 'Financial Services' }
    ];

    return mockStocks
      .filter(stock => 
        stock.symbol.toLowerCase().includes(searchTerm.toLowerCase()) ||
        stock.name.toLowerCase().includes(searchTerm.toLowerCase())
      )
      .slice(0, 8)
      .map(stock => ({
        type: 'stock',
        symbol: stock.symbol,
        name: stock.name,
        description: stock.description
      }));
  }

  selectSearchResult(result: any): void {
    this.searchTerm = result.symbol;
    this.clearSearch();
    
    // Navigate based on current route and pass search parameter
    const currentRoute = this.router.url;
    
    if (currentRoute.includes('HistoricalData')) {
      this.router.navigate(['/HistoricalData'], { queryParams: { search: result.symbol } });
    } else if (currentRoute.includes('OptionsData')) {
      this.router.navigate(['/OptionsData'], { queryParams: { search: result.symbol } });
    } else {
      // Default to dashboard with stock selection
      this.router.navigate(['/'], { queryParams: { stock: result.symbol } });
    }
  }

  clearSearch(): void {
    this.searchResults = [];
    this.showSearchResults = false;
    this.isSearching = false;
  }

  onSearchFocus(): void {
    if (this.searchResults.length > 0) {
      this.showSearchResults = true;
    }
  }

  onSearchBlur(): void {
    // Delay hiding results to allow clicking on them
    setTimeout(() => {
      this.showSearchResults = false;
    }, 200);
  }

  navigateToHome(): void {
    this.router.navigate(['/']);
    this.clearSearch();
  }
}