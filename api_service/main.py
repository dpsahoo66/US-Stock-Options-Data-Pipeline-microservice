from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
try:
    import pyodbc
    PYODBC_AVAILABLE = True
except ImportError:
    PYODBC_AVAILABLE = False
    print("Warning: pyodbc not available. Using mock data for testing.")

import socket
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import os
from pydantic import BaseModel

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI(title="Stock Data API", description="API for serving historical stock data from Azure SQL", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Azure SQL Connection Configuration
AZURE_SQL_CONNECTION_STRING = os.getenv(
    "AZURE_SQL_CONNECTION_STRING", 
    "Driver={ODBC Driver 18 for SQL Server};Server=dash-gtd.database.windows.net;Database=us_stock_options_db;Uid=dash_gtd;Pwd=wearethebest@69;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"
)

# Connection parameters
conn_strings = [
    AZURE_SQL_CONNECTION_STRING,
    AZURE_SQL_CONNECTION_STRING.replace("ODBC Driver 18", "ODBC Driver 17")
]

# Retry parameters
max_retries = 5
retry_delay = 20

class StockDataPoint(BaseModel):
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int

class StockDataResponse(BaseModel):
    stock_name: str
    data: List[StockDataPoint]
    total_records: int

class DatabaseConnection:
    def __init__(self):
        self.conn = None
        self.cursor = None
        if PYODBC_AVAILABLE:
            self.connect()
        else:
            logger.warning("Using mock database connection")
    
    def connect_with_retry(self):
        """Connect to Azure SQL with retry logic"""
        if not PYODBC_AVAILABLE:
            return None
            
        for conn_str in conn_strings:
            driver = conn_str.split(';')[0].split('=')[1]
            logger.info(f"Trying connection with {driver}")
            for attempt in range(max_retries):
                try:
                    logger.info(f"Connection attempt {attempt + 1}/{max_retries}")
                    # Test DNS resolution
                    server_ip = socket.gethostbyname("dash-gtd.database.windows.net")
                    logger.info(f"Server resolved to IP: {server_ip}")
                    
                    conn = pyodbc.connect(conn_str)
                    return conn
                except Exception as e:
                    sqlstate = getattr(e, 'args', [''])[0] if PYODBC_AVAILABLE else ''
                    if sqlstate in ('08S01', '40001', '40197', '40501', '40613', '23000'):
                        logger.warning(f"Transient error encountered: {e}. Retrying...")
                    else:
                        logger.error(f"Non-transient connection error: {e}")
                        break
                    if attempt < max_retries - 1:
                        logger.info(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    else:
                        logger.warning(f"Max retries reached for {driver} with error: {e}")
                except Exception as e:
                    logger.error(f"An unexpected error occurred during connection: {e}")
                    break
            logger.info(f"Failed with {driver}, trying next driver if available")
        raise Exception("All connection attempts failed")
    
    def connect(self):
        """Establish connection to Azure SQL Database"""
        if not PYODBC_AVAILABLE:
            logger.warning("pyodbc not available, using mock connection")
            return
            
        try:
            self.conn = self.connect_with_retry()
            if self.conn:
                self.cursor = self.conn.cursor()
                
                # Verify table exists
                self.cursor.execute("SELECT 1 FROM sys.tables WHERE name = 'StockData'")
                if not self.cursor.fetchone():
                    raise Exception("StockData table does not exist")
                
                logger.info("Connected successfully to Azure SQL Database")
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            logger.warning("Falling back to mock data mode")
            # Don't raise exception, just use mock data
    
    def get_mock_data(self, stock_symbol: str = "AAPL", limit: int = 100):
        """Generate mock data for testing when database is not available"""
        import random
        from datetime import datetime, timedelta
        
        mock_data = []
        base_price = 150.0
        base_date = datetime.now() - timedelta(days=limit)
        
        for i in range(limit):
            date = base_date + timedelta(days=i)
            # Generate realistic OHLCV data
            open_price = base_price + random.uniform(-5, 5)
            high_price = open_price + random.uniform(0, 10)
            low_price = open_price - random.uniform(0, 10)
            close_price = open_price + random.uniform(-8, 8)
            volume = random.randint(1000000, 50000000)
            
            mock_data.append((
                stock_symbol.upper(),
                date.strftime('%Y-%m-%d'),
                open_price,
                high_price,
                low_price,
                close_price,
                volume
            ))
            base_price = close_price  # Next day starts where this day ended
        
        return mock_data
    
    def execute_query(self, query: str, params: tuple = None):
        """Execute query with retry logic"""
        if not PYODBC_AVAILABLE:
            # Return mock data based on query type
            if "DISTINCT StockName" in query:
                return [("AAPL",), ("GOOGL",), ("MSFT",), ("AMZN",), ("TSLA",)]
            elif "SELECT TOP" in query and "StockData" in query:
                stock_symbol = params[1] if params and len(params) > 1 else "AAPL"
                limit = params[0] if params and len(params) > 0 else 100
                return self.get_mock_data(stock_symbol, limit)
            elif "COUNT(*)" in query:
                return [(1000,)]
            elif "MIN(Date)" in query:  # Summary query
                return [(500, '2023-01-01', '2024-12-31', 155.50, 120.00, 200.00, 50000000)]
            return []
            
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            # Try to reconnect and retry once
            self.connect()
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            return self.cursor.fetchall()
    
    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")

# Global database connection
try:
    db = DatabaseConnection()
except:
    logger.warning("Failed to initialize database connection, using mock mode")
    db = DatabaseConnection()  # This will use mock mode

@app.on_event("shutdown")
async def shutdown_event():
    """Close database connection on app shutdown"""
    db.close()

@app.get("/")
async def root():
    """Health check endpoint"""
    return {"message": "Stock Data API is running", "status": "healthy"}

@app.get("/api/stocks/list")
async def get_stock_list():
    """Get list of available stock symbols"""
    try:
        query = "SELECT DISTINCT StockName FROM StockData ORDER BY StockName"
        results = db.execute_query(query)
        
        stocks = [row[0] for row in results]
        return {"stocks": stocks, "total": len(stocks)}
        
    except Exception as e:
        logger.error(f"Error fetching stock list: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch stock list: {str(e)}")

@app.get("/api/stocks/{stock_symbol}/historical")
async def get_historical_data(
    stock_symbol: str,
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: Optional[int] = Query(1000, description="Maximum number of records")
):
    """Get historical data for a specific stock symbol"""
    try:
        # Base query
        query = """
            SELECT TOP (?) StockName, Date, [Open], High, Low, [Close], Volume
            FROM StockData 
            WHERE StockName = ?
        """
        params = [limit, stock_symbol.upper()]
        
        # Add date filters if provided
        if start_date:
            query += " AND Date >= ?"
            params.append(start_date)
        
        if end_date:
            query += " AND Date <= ?"
            params.append(end_date)
        
        query += " ORDER BY Date DESC"
        
        results = db.execute_query(query, tuple(params))
        
        if not results:
            raise HTTPException(status_code=404, detail=f"No data found for stock symbol: {stock_symbol}")
        
        # Format data for response
        data_points = []
        for row in results:
            # Handle different data formats (mock vs real database)
            if len(row) >= 7:  # Full database result
                data_points.append(StockDataPoint(
                    date=row[1].strftime('%Y-%m-%d') if hasattr(row[1], 'strftime') else str(row[1]),
                    open=float(row[2]) if row[2] else 0.0,
                    high=float(row[3]) if row[3] else 0.0,
                    low=float(row[4]) if row[4] else 0.0,
                    close=float(row[5]) if row[5] else 0.0,
                    volume=int(row[6]) if row[6] else 0
                ))
            else:  # Mock data format
                data_points.append(StockDataPoint(
                    date=str(row[1]),
                    open=float(row[2]) if row[2] else 0.0,
                    high=float(row[3]) if row[3] else 0.0,
                    low=float(row[4]) if row[4] else 0.0,
                    close=float(row[5]) if row[5] else 0.0,
                    volume=int(row[6]) if row[6] else 0
                ))
        
        # Reverse to get chronological order
        data_points.reverse()
        
        return StockDataResponse(
            stock_name=stock_symbol.upper(),
            data=data_points,
            total_records=len(data_points)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching historical data for {stock_symbol}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch historical data: {str(e)}")

@app.get("/api/stocks/{stock_symbol}/summary")
async def get_stock_summary(stock_symbol: str):
    """Get summary statistics for a stock"""
    try:
        query = """
            SELECT 
                COUNT(*) as total_records,
                MIN(Date) as earliest_date,
                MAX(Date) as latest_date,
                AVG([Close]) as avg_close,
                MIN([Low]) as min_low,
                MAX([High]) as max_high,
                SUM(Volume) as total_volume
            FROM StockData 
            WHERE StockName = ?
        """
        
        results = db.execute_query(query, (stock_symbol.upper(),))
        
        if not results or not results[0][0]:
            raise HTTPException(status_code=404, detail=f"No data found for stock symbol: {stock_symbol}")
        
        row = results[0]
        return {
            "stock_name": stock_symbol.upper(),
            "total_records": row[0],
            "earliest_date": row[1].strftime('%Y-%m-%d') if hasattr(row[1], 'strftime') else str(row[1]),
            "latest_date": row[2].strftime('%Y-%m-%d') if hasattr(row[2], 'strftime') else str(row[2]),
            "average_close": round(float(row[3]), 2) if row[3] else 0.0,
            "min_low": float(row[4]) if row[4] else 0.0,
            "max_high": float(row[5]) if row[5] else 0.0,
            "total_volume": int(row[6]) if row[6] else 0
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching summary for {stock_symbol}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch stock summary: {str(e)}")

@app.get("/api/test-connection")
async def test_connection():
    """Test database connection"""
    try:
        query = "SELECT COUNT(*) FROM StockData"
        results = db.execute_query(query)
        total_records = results[0][0] if results else 0
        
        return {
            "status": "connected",
            "message": "Database connection successful",
            "total_stock_records": total_records
        }
        
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        raise HTTPException(status_code=500, detail=f"Database connection test failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)