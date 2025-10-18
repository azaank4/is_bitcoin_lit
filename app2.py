"""
IsBitcoinLit - FastAPI with Redis Time Series
Single file implementation with fakeredis option
"""

import asyncio
import functools
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Iterable, Optional
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Depends, BackgroundTasks, HTTPException
from asyncio import Lock

# Load environment variables
load_dotenv()

# Configuration
SENTIMENT_API_URL = "https://api.senticrypt.com/v1/bitcoin.json"
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY", "")  # Get API key from environment
COINGECKO_API_URL = "https://api.coingecko.com/api/v3/simple/price"
HOURLY_BUCKET = 3600000  # 1 hour in milliseconds
REFRESH_INTERVAL = 30  # refresh interval in seconds
FETCH_DELAY = 10  # sleep time for API fetching
MANUAL_REFRESH_DELAY = 5  # sleep time for manual refresh on 30th second
REFRESH_DELAY = 5  # sleep time for scheduler rate limiting
CACHE_TTL = REFRESH_INTERVAL  # align cache TTL with refresh interval

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)

# Try to use real Redis, fall back to fakeredis for testing
try:
    import aioredis
    REDIS_AVAILABLE = True
    # Try to connect to Redis
    redis = aioredis.from_url(
        "redis://localhost:6379",
        decode_responses=True,
        max_connections=10
    )
    log.info("Using real Redis server")
except (ImportError, Exception) as e:
    log.warning(f"Real Redis not available, using in-memory storage: {e}")
    REDIS_AVAILABLE = False
    # Simple in-memory storage for demo purposes
    class MemoryRedis:
        def __init__(self):
            self.data = {}
            self.timeseries_data = {}
            self.locks = {}
        
        async def execute_command(self, *args):
            command = args[0].upper()
            if command == 'TS.CREATE':
                key = args[1]
                self.timeseries_data[key] = []
                return "OK"
            elif command == 'TS.MADD':
                # args: 'TS.MADD', key1, ts1, val1, key2, ts2, val2, ...
                for i in range(1, len(args), 3):
                    key = args[i]
                    timestamp = args[i+1]
                    value = args[i+2]
                    if key not in self.timeseries_data:
                        self.timeseries_data[key] = []
                    self.timeseries_data[key].append((timestamp, float(value)))
                return "OK"
            elif command == 'TS.RANGE':
                key = args[1]
                start = int(args[2])
                aggregation = args[5] if len(args) > 5 else None
                bucket_size = int(args[6]) if len(args) > 6 else None
                
                if key not in self.timeseries_data:
                    return []
                
                data = self.timeseries_data[key]
                filtered_data = [(ts, val) for ts, val in data if ts >= start]
                
                if aggregation and bucket_size:
                    # Simple aggregation simulation
                    aggregated = []
                    current_bucket = start
                    while current_bucket <= (start + 3 * HOURLY_BUCKET):
                        bucket_data = [val for ts, val in filtered_data 
                                    if current_bucket <= ts < current_bucket + bucket_size]
                        if bucket_data:
                            avg_val = sum(bucket_data) / len(bucket_data)
                            aggregated.append([current_bucket, avg_val])
                        current_bucket += bucket_size
                    return aggregated
                
                return filtered_data
            elif command == 'SET':
                key, value = args[1], args[2]
                ex = args[3] if len(args) > 3 else None
                self.data[key] = value
                return "OK"
            elif command == 'GET':
                key = args[1]
                return self.data.get(key)
            elif command == 'DEL':
                key = args[1]
                if key in self.data:
                    del self.data[key]
                return "OK"
            elif command == 'PING':
                return "PONG"
            elif command == 'TS.INFO':
                key = args[1]
                if key in self.timeseries_data:
                    return {"samples": len(self.timeseries_data[key])}
                else:
                    raise Exception("Time series not found")
            else:
                log.warning(f"Unsupported command: {command}")
                return "OK"
        
        async def close(self):
            pass
    
    redis = MemoryRedis()


class Keys:
    """Manage Redis key names"""
    
    def __init__(self, base_key: str = "bitcoin"):
        self.base_key = base_key
    
    def timeseries_price_key(self) -> str:
        return f"{self.base_key}:ts:price"
    
    def timeseries_sentiment_key(self) -> str:
        return f"{self.base_key}:ts:sentiment"
    
    def cache_key(self) -> str:
        # Cache key based on current time including seconds
        current_time = datetime.now(timezone.utc).replace(microsecond=0)
        return f"{self.base_key}:cache:{current_time.isoformat()}"
    
    def latest_price_key(self) -> str:
        return f"{self.base_key}:latest_price"
    
    def price_fetch_lock_key(self) -> str:
        return f"{self.base_key}:price_fetch_lock"
    
    def last_price_fetch_key(self) -> str:
        return f"{self.base_key}:last_price_fetch"


def make_keys() -> Keys:
    return Keys()


def now():
    return datetime.now(timezone.utc)


def datetime_parser(dct):
    """Parse datetime strings from JSON"""
    for k, v in dct.items():
        if isinstance(v, str):
            try:
                # Try to parse ISO format datetime
                dct[k] = datetime.fromisoformat(v)
            except (ValueError, AttributeError):
                pass
    return dct


async def make_timeseries(key: str):
    """
    Create a timeseries with the Redis key `key`.
    """
    try:
        await redis.execute_command(
            'TS.CREATE', key,
            'DUPLICATE_POLICY', 'first',
        )
        log.info(f"Created time series: {key}")
    except Exception as e:
        # Time series probably already exists
        log.info(f'Could not create time series {key}, error: {e}')


async def initialize_redis(keys: Keys):
    """Initialize Redis time series on startup"""
    try:
        await make_timeseries(keys.timeseries_price_key())
        await make_timeseries(keys.timeseries_sentiment_key())
        log.info("Redis time series initialized")
    except Exception as e:
        log.error(f"Failed to initialize Redis: {e}")


async def add_many_to_timeseries(
    key_pairs: Iterable[Tuple[str, str]],
    data: List[Dict]
):
    """
    Add many samples to a single timeseries key.
    """
    commands = []
    
    for datapoint in data:
        for timeseries_key, sample_key in key_pairs:
            # Convert timestamp to milliseconds
            timestamp_ms = int(float(datapoint['timestamp']) * 1000)
            value = datapoint[sample_key]
            commands.extend([timeseries_key, timestamp_ms, str(value)])
    
    if commands:
        await redis.execute_command('TS.MADD', *commands)


# Add price fetch lock
price_fetch_lock = Lock()
last_price_fetch_time = 0

async def get_real_time_price(with_delay: bool = False, force_fresh: bool = False) -> float:
    """Fetch real-time Bitcoin price from CoinGecko"""
    global last_price_fetch_time
    
    async with price_fetch_lock:
        try:
            current_time = now().timestamp()
            
            # If not forcing a fresh fetch and we fetched recently, return cached price
            if not force_fresh and current_time - last_price_fetch_time < FETCH_DELAY:
                keys = Keys()
                cached_price = await redis.execute_command('GET', keys.latest_price_key())
                if cached_price:
                    log.info("Returning recently cached price")
                    return float(cached_price)
            
            # Ensure delay when we're going to fetch fresh price
            if with_delay or force_fresh:
                log.info(f"Starting {FETCH_DELAY} second delay before fetching price...")
                await asyncio.sleep(FETCH_DELAY)
            
            params = {"ids": "bitcoin", "vs_currencies": "usd"}
            headers = {"x-cg-api-key": COINGECKO_API_KEY} if COINGECKO_API_KEY else {}
            
            log.info("Fetching fresh price from CoinGecko...")
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(COINGECKO_API_URL, params=params, headers=headers)
                response.raise_for_status()
                # use consistent name
                current_price = float(response.json()['bitcoin']['usd'])
                
                # Store price in both Redis and time series
                keys = Keys()
                timestamp_ms = int(now().timestamp() * 1000)
                
                # Store current price
                await redis.execute_command('SET', keys.latest_price_key(), str(current_price))
                
                # Store in time series
                await redis.execute_command('TS.MADD', 
                    keys.timeseries_price_key(), timestamp_ms, str(current_price))
                
                # Update last fetch time
                last_price_fetch_time = now().timestamp()
                
                log.info(f"Fresh current_price updated: {current_price}")
                return current_price
                
        except Exception as e:
            log.error(f"Error fetching real-time price: {e}")
            # Try to return cached price as fallback
            keys = Keys()
            cached_price = await redis.execute_command('GET', keys.latest_price_key())
            if cached_price:
                log.warning(f"Using cached price due to error: {e}")
                return float(cached_price)
            raise

async def scheduled_refresh():
    """Task to be run on schedule - only updates price"""
    try:
        # Only fetch price with delay for scheduled tasks
        current_price = await get_real_time_price(with_delay=True, force_fresh=True)
        log.info(f"Scheduled refresh completed, current_price={current_price}")
    except Exception as e:
        log.error(f"Scheduled refresh failed: {e}")


async def persist(keys: Keys, sentiment_data: List[Dict]):
    """Persist sentiment and price data to Redis time series"""
    ts_price_key = keys.timeseries_price_key()
    ts_sentiment_key = keys.timeseries_sentiment_key()
    
    if not sentiment_data:
        log.warning("No sentiment data available")
        return
    
    try:
        # Get current price without delay for persistence
        current_price = await get_real_time_price(with_delay=False, force_fresh=True)
        
        # Update sentiment data with real-time price
        current_time = now().timestamp()
        for data_point in sentiment_data:
            data_point['btc_price'] = str(current_price)
            if 'timestamp' not in data_point:
                data_point['timestamp'] = current_time
        
        await add_many_to_timeseries(
            [
                (ts_price_key, 'btc_price'),
                (ts_sentiment_key, 'mean'),
            ], 
            sentiment_data
        )
        log.info(f"Persisted {len(sentiment_data)} data points with real-time price to Redis time series")
    except Exception as e:
        log.error(f"Error persisting data: {e}")
        raise


async def get_latest_price(keys: Keys) -> float:
    """Get the latest stored price from Redis"""
    try:
        price_str = await redis.execute_command('GET', keys.latest_price_key())
        return float(price_str) if price_str else 0.0
    except Exception as e:
        log.error(f"Error getting latest price: {e}")
        return 0.0


async def get_hourly_average(ts_key: str, start_timestamp: int):
    """Get hourly averages from time series"""
    try:
        response = await redis.execute_command(
            'TS.RANGE', ts_key, start_timestamp, '+',
            'AGGREGATION', 'avg', HOURLY_BUCKET,
        )
        return response
    except Exception as e:
        log.error(f"Error getting hourly average for {ts_key}: {e}")
        return []


def get_direction(data: List[Dict], key: str) -> str:
    """Determine if values are trending up or down"""
    if len(data) < 2:
        return "stable"
    
    try:
        values = [item[key] for item in data if item.get(key) is not None]
        if not values:
            return "stable"
            
        if values[-1] > values[0]:
            return "up"
        elif values[-1] < values[0]:
            return "down"
        else:
            return "stable"
    except (KeyError, IndexError) as e:
        log.error(f"Error calculating direction: {e}")
        return "stable"


async def calculate_three_hours_of_data(keys: Keys) -> Dict:
    """Calculate three-hour averages"""
    try:
        sentiment_key = keys.timeseries_sentiment_key()
        price_key = keys.timeseries_price_key()
        
        # Get stored price instead of fetching new one
        current_price = await get_latest_price(keys)
        
        # Calculate timestamp for 3 hours ago in milliseconds
        three_hours_ago_ms = int((now() - timedelta(hours=3)).timestamp() * 1000)

        # Get hourly averages
        sentiment_data = await get_hourly_average(sentiment_key, three_hours_ago_ms)
        price_data = await get_hourly_average(price_key, three_hours_ago_ms)

        # If no data, return with just current price
        if not price_data or not sentiment_data:
            log.warning("No historical data found")
            return {
                'current_price': current_price,
                'hourly_average_of_averages': [],
                'sentiment_direction': 'unknown',
                'price_direction': 'unknown',
                'last_updated': now().isoformat(),
                'error': 'No historical data available'
            }

        # Combine the data
        last_three_hours = []
        min_length = min(len(price_data), len(sentiment_data))
        
        for i in range(min_length):
            try:
                price_val = float(price_data[i][1]) if price_data[i][1] else 0
                sentiment_val = float(sentiment_data[i][1]) if sentiment_data[i][1] else 0
                
                last_three_hours.append({
                    'price': price_val,
                    'sentiment': sentiment_val,
                    'time': datetime.fromtimestamp(price_data[i][0] / 1000, tz=timezone.utc),
                })
            except (ValueError, IndexError) as e:
                log.warning(f"Error processing data point {i}: {e}")
                continue

        return {
            'current_price': current_price,
            'hourly_average_of_averages': last_three_hours,
            'sentiment_direction': get_direction(last_three_hours, 'sentiment'),
            'price_direction': get_direction(last_three_hours, 'price'),
            'last_updated': now().isoformat()
        }
        
    except Exception as e:
        log.error(f"Error calculating three-hour data: {e}")
        return {
            'hourly_average_of_averages': [],
            'sentiment_direction': 'unknown',
            'price_direction': 'unknown',
            'last_updated': now().isoformat(),
            'error': str(e)
        }


async def set_cache(data: Dict, keys: Keys):
    """Cache data in Redis with expiration"""
    try:
        def serialize_dates(v):
            return v.isoformat() if isinstance(v, datetime) else v

        await redis.execute_command(
            'SET', keys.cache_key(),
            json.dumps(data, default=serialize_dates),
            'EX', CACHE_TTL
        )
        log.info("Data cached successfully")
    except Exception as e:
        log.error(f"Error setting cache: {e}")


async def get_cache(keys: Keys) -> Optional[Dict]:
    """Get cached data from Redis"""
    try:
        cache_key = keys.cache_key()
        cached_data = await redis.execute_command('GET', cache_key)

        if cached_data:
            log.info("Cache hit")
            return json.loads(cached_data, object_hook=datetime_parser)
        log.info("Cache miss")
        return None
    except Exception as e:
        log.error(f"Error getting cache: {e}")
        return None


# Initialize scheduler
scheduler = AsyncIOScheduler()

# FastAPI Application
app = FastAPI(
    title="IsBitcoinLit API",
    description="Bitcoin sentiment and price tracking API with Redis Time Series",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)


@app.on_event("startup")
async def startup_event():
    """Initialize Redis and scheduler on startup"""
    try:
        keys = Keys()
        await initialize_redis(keys)
        
        # Start the scheduler
        scheduler.add_job(
            scheduled_refresh,
            trigger=IntervalTrigger(seconds=REFRESH_INTERVAL),
            id='refresh_job',
            name='Refresh Bitcoin data',
            replace_existing=True
        )
        scheduler.start()
        
        log.info("IsBitcoinLit API started successfully")
        if not REDIS_AVAILABLE:
            log.info("Using in-memory storage (Redis not available)")
    except Exception as e:
        log.error(f"Startup error: {e}")


@app.on_event("shutdown")
async def shutdown_event():
    """Close Redis connection and shutdown scheduler"""
    try:
        scheduler.shutdown()
        await redis.close()
        log.info("IsBitcoinLit API shutdown successfully")
    except Exception as e:
        log.error(f"Shutdown error: {e}")


@app.get("/")
async def root():
    storage_type = "In-Memory (Redis not available)" if not REDIS_AVAILABLE else "Redis Server"
    return {
        "message": "IsBitcoinLit API - Bitcoin Sentiment & Price Tracker",
        "storage": storage_type,
        "endpoints": {
            "refresh": "POST /refresh - Refresh Bitcoin data from SentiCrypt",
            "status": "GET /is-bitcoin-lit - Get current Bitcoin status with 3-hour averages",
            "health": "GET /health - Health check"
        },
        "documentation": {
            "swagger": "/docs",
            "redoc": "/redoc"
        }
    }


@app.post("/refresh")
async def refresh(keys: Keys = Depends(make_keys)):
    """Refresh Bitcoin data"""
    try:
        current_second = datetime.now().second
        if current_second == 30:
            await asyncio.sleep(MANUAL_REFRESH_DELAY)
        
        # Fetch and store new price immediately (force fresh)
        current_price = await get_real_time_price(with_delay=False, force_fresh=True)
        
        # Clear cache to force recalculation
        await redis.execute_command('DEL', keys.cache_key())
        
        # Fetch sentiment data
        sentiment_data = []
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(SENTIMENT_API_URL)
                response.raise_for_status()
                sentiment_data = response.json()
        except httpx.HTTPError as e:
            log.error(f"Error fetching sentiment data: {e}")
            sentiment_data = [{
                'timestamp': now().timestamp(),
                'mean': 0.5,
                'btc_price': str(current_price)
            }]
        
        # Update sentiment data with fresh price
        timestamp = now().timestamp()
        for data_point in sentiment_data:
            data_point['btc_price'] = str(current_price)
            data_point['timestamp'] = timestamp
        
        # Store sentiment data
        await add_many_to_timeseries(
            [(keys.timeseries_sentiment_key(), 'mean')],
            sentiment_data
        )
        
        return {
            "status": "success",
            "message": "Bitcoin data refreshed successfully",
            "current_price": current_price,
            "timestamp": now().isoformat()
        }
    except Exception as e:
        log.error(f"Error refreshing data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/is-bitcoin-lit")
async def bitcoin_status(
    background_tasks: BackgroundTasks, 
    keys: Keys = Depends(make_keys)
):
    """
    Get current Bitcoin sentiment and price status with 3-hour averages
    Uses Redis cache when available
    """
    try:
        # Try to get cached data first
        data = await get_cache(keys)
        
        if not data:
            # Calculate fresh data if not cached
            log.info("Calculating fresh Bitcoin status data")
            data = await calculate_three_hours_of_data(keys)
            # Update cache in background
            background_tasks.add_task(set_cache, data, keys)
            data["source"] = "fresh_calculation"
        else:
            data["source"] = "cache"
        
        # Add metadata
        data["cache_info"] = {
            "strategy": f"{REFRESH_INTERVAL}-second TTL",
            "next_refresh": (now() + timedelta(seconds=REFRESH_INTERVAL)).isoformat()
        }
        data["storage_type"] = "In-Memory" if not REDIS_AVAILABLE else "Redis"
        data["refresh_interval"] = f"Every {REFRESH_INTERVAL} seconds"
        
        return data
        
    except Exception as e:
        log.error(f"Error getting Bitcoin status: {e}")
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to calculate Bitcoin status: {str(e)}"
        )


@app.get("/health")
async def health_check():
    """Health check endpoint with Redis connection test"""
    try:
        # Test Redis connection
        await redis.execute_command('PING')
        
        storage_type = "In-Memory" if not REDIS_AVAILABLE else "Redis Server"
        
        return {
            "status": "healthy",
            "storage": storage_type,
            "timestamp": now().isoformat(),
            "version": "1.0.0"
        }
    except Exception as e:
        log.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": now().isoformat()
        }


if __name__ == "__main__":
    import uvicorn
    
    print("Starting IsBitcoinLit API...")
    print("If Redis is not available, the app will use in-memory storage")
    print("API Documentation: http://localhost:8000/docs")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )