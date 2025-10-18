# 🪙 IsBitcoinLit

**FastAPI + RedisTimeSeries Bitcoin Sentiment & Price Tracker**

## 📖 Overview

**IsBitcoinLit** is a lightweight FastAPI-based service that tracks **Bitcoin’s real-time price** and **sentiment data** using Redis TimeSeries.
The app periodically fetches data from:

* **CoinGecko API** → for Bitcoin’s current USD price
* **SentiCrypt API** → for Bitcoin sentiment analysis

The data is stored in **Redis TimeSeries**, aggregated hourly, and made available via simple API endpoints.

---

## ⚙️ Key Features

✅ Built with **FastAPI** for high-performance async APIs
✅ Uses **Redis TimeSeries** to store and aggregate historical data
✅ **Scheduled background tasks** using APScheduler
✅ Fetches data from **CoinGecko** and **SentiCrypt** APIs
✅ Provides **cached responses** for efficient repeated access
✅ Clear logging and structured API design

---

## 🧱 Tech Stack

* **Language:** Python 3.10+
* **Framework:** FastAPI
* **Database:** Redis (with TimeSeries module)
* **Scheduler:** APScheduler
* **HTTP Client:** httpx
* **Environment Management:** python-dotenv
* **Server:** uvicorn

---

## 📦 Installation

### 1. Clone the repository

```bash
git clone https://github.com/<your-username>/isbitcoinlit.git
cd isbitcoinlit
```

### 2. Create and activate a virtual environment

```bash
python -m venv venv
source venv/bin/activate  # On Linux/Mac
venv\Scripts\activate     # On Windows
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Run Redis with TimeSeries module

You must have Redis running locally.
If you’re using Docker, you can spin it up with:

```bash
docker run -d --name redis-timeseries -p 6379:6379 redis/redis-stack-server
```

---

## 🔑 Environment Variables

Create a `.env` file in the root directory and add:

```bash
COINGECKO_API_KEY=your_coingecko_api_key_here
```

*(Optional: You can leave it blank if you don’t have one — the API works with a limited rate.)*

---

## 🚀 Run the Application

```bash
uvicorn main:app --reload
```

The server will start on:
👉 **[http://localhost:8000](http://localhost:8000)**

---

## 📡 API Endpoints

### **`GET /`**

Returns general information about the API and available endpoints.

---

### **`POST /refresh`**

Manually triggers fetching of fresh Bitcoin price and sentiment data from APIs.

**Response:**

```json
{
  "status": "success",
  "message": "Bitcoin data refreshed successfully",
  "current_price": 67890.25,
  "timestamp": "2025-10-18T14:00:00Z"
}
```

---

### **`GET /is-bitcoin-lit`**

Retrieves:

* Current Bitcoin price
* Last 3-hour averages
* Price and sentiment trends
* Cache metadata

**Response Example:**

```json
{
  "current_price": 67890.25,
  "hourly_average_of_averages": [
    {"price": 67800.1, "sentiment": 0.62, "time": "2025-10-18T11:00:00Z"},
    {"price": 67850.5, "sentiment": 0.65, "time": "2025-10-18T12:00:00Z"}
  ],
  "sentiment_direction": "up",
  "price_direction": "up",
  "last_updated": "2025-10-18T14:00:00Z",
  "source": "fresh_calculation"
}
```

---

### **`GET /health`**

Checks the health of the application and Redis connection.

**Response:**

```json
{
  "status": "healthy",
  "storage": "Redis Server",
  "timestamp": "2025-10-18T14:00:00Z",
  "version": "1.0.0"
}
```

---

## 🕓 Scheduled Jobs

The application uses **APScheduler** to automatically refresh Bitcoin price data every **30 seconds**.

---

## 📊 Redis Data Structure

| Key Type                    | Description                                    |
| --------------------------- | ---------------------------------------------- |
| `bitcoin:ts:price`          | Redis TimeSeries storing BTC price (USD)       |
| `bitcoin:ts:sentiment`      | Redis TimeSeries storing sentiment scores      |
| `bitcoin:latest_price`      | Latest fetched Bitcoin price                   |
| `bitcoin:cache:<timestamp>` | Cached response for `/is-bitcoin-lit` endpoint |

---

## 🧠 How It Works (Simplified Flow)

1. Every 30 seconds, the scheduler fetches Bitcoin price from CoinGecko.
2. Sentiment data is fetched from SentiCrypt API.
3. Both datasets are stored in Redis TimeSeries.
4. `/is-bitcoin-lit` calculates 3-hour aggregates and caches the result.
5. Subsequent calls within 30 seconds use cached data for speed.

---

## 🧩 Example Architecture

```
           ┌────────────────────────────┐
           │       FastAPI Server       │
           │    (IsBitcoinLit API)      │
           └─────────────┬──────────────┘
                         │
         ┌───────────────┴────────────────┐
         │                                │
 ┌──────────────────┐            ┌────────────────────┐
 │  CoinGecko API   │            │  SentiCrypt API    │
 └──────────────────┘            └────────────────────┘
         │                                │
         └──────────────┬─────────────────┘
                        │
              ┌────────────────────┐
              │ Redis TimeSeries   │
              │  • Price data      │
              │  • Sentiment data  │
              └────────────────────┘
```
---

## 👨‍💻 Author

**Azaan Khan**
Building intelligent systems with Python, FastAPI, and AI integration.
