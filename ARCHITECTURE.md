# Architecture Deep Dive - KIRA Platform

The KIRA Quantitative Trading Platform follows a reactive, event-driven design built around an Apache Kafka message bus. This allows individual components to scale independently and prevents network bottlenecks during highly volatile market sessions.

```mermaid
flowchart TB
    subgraph External["External Interfaces"]
        Upstox["Upstox Trading API"]
    end

    subgraph Streaming["Event Streaming Layer"]
        Kafka(("Apache Kafka\nMessage Bus"))
    end

    subgraph DataLayer["Data & Persistence Layer"]
        QDB[("QuestDB\nTime-Series Data")]
        PG[("PostgreSQL\nMetadata & Orders")]
        RS[("Redis\nState & Caching")]
        S3[("Minio S3\nModels & Assets")]
    end

    subgraph DataIngestion["Ingestion & Processing"]
        Ingestor["Ingestor\nWebSocket Feed"]
        Scanner["Scanner\nMomentum Detection"]
        FeatureEngine["Feature Engine\nMicrostructure Metrics"]
        Backfiller["Data Backfiller\nHistorical Downloader"]
        Persistor["Market Persistor\nData Storage"]
        EdgeDetector["Edge Detector\nSupport & Resistance"]
    end

    subgraph AI["Strategy & Execution"]
        Optimizer["Parameter Optimizer\nHyperparameter Tuning"]
        Runtime["Strategy Runtime\nLive Execution Engine"]
        Replayer["Historical Replayer\nBacktest Simulation"]
    end

    subgraph Interface["User Interface"]
        API["API Gateway\nFastAPI REST"]
        Frontend["Quant Frontend\nNext.js Dashboard"]
    end

    %% Connections
    Upstox <-->|WebSockets & REST| Ingestor
    Upstox -->|REST| Scanner
    Runtime -->|Execute Orders| Upstox

    Ingestor -->|Raw Market Data| Kafka
    Scanner -->|Momentum Candidates| Kafka
    FeatureEngine -->|VWAP and OBI| Kafka
    EdgeDetector -->|Price Levels| Kafka
    
    Kafka -.->|Stream| FeatureEngine
    Kafka -.->|Stream| EdgeDetector
    Kafka -.->|Stream| Persistor
    Kafka -.->|Market Events| Runtime
    
    Persistor -->|Save Ticks| QDB
    Backfiller -->|Historical OHLC| QDB
    Replayer -->|Playback Data| Kafka
    
    Optimizer -->|Optimal Parameters| PG
    Runtime -->|Load Models| S3
    Runtime -->|Portfolios and Trades| PG
    Runtime -->|Historical Data| QDB
    
    API <-->|Manage Strategies| Runtime
    API -->|Read & Write| PG
    API -->|Read| QDB
    API -->|Cache| RS
    
    Frontend <-->|REST API| API
```

---

## Core Microservices Breakdown

### 1. Ingestor
Connects directly to the Upstox V3 WebSocket feed. It subscribes to a dynamic list of instruments, including the top 100 highly liquid NSE equities globally recognized in the NIFTY index, and publishes raw tick data (Last Traded Price, Volume, Open Interest, and Level 2 Market Depth) directly to Kafka.

### 2. Market Scanner
Operates on a scheduled interval to scan the broader market for high-momentum breakout candidates. It calculates momentum scores based on price action and trading volume, alerting the Ingestor to dynamically subscribe to new, highly-active symbols.

### 3. Feature Engine
Consumes the raw market ticks from Kafka and calculates enriched technical indicators in real-time. This includes Volume-Weighted Average Price, Order Book Imbalance, and short-term Simple Moving Averages. The enriched data stream is then republished back to the event bus for downstream execution elements.

### 4. Edge Detector
A real-time analytics module that listens to the market feed and mathematically computes dynamic local support and resistance levels. It constantly maps out the structural boundaries of the market, helping algorithmic strategies identify optimal entry and exit edges based on recent price consolidation zones.

### 5. Market Persistor
Listens to all enriched market data passing through Kafka and heavily batches it for insertion into QuestDB, an ultra-fast time-series database. This ensures every single tick, quote, and calculated metric is safely and efficiently stored for long-term historical analysis.

### 6. Strategy Runtime (Algorithm Engine)
The core execution environment. This service loads trading strategies built using the native Quant SDK. It handles everything from evaluating live market signals and managing the portfolio, to strictly sizing positions and tracking daily risk compliance. It interfaces securely with the Upstox API to submit live market orders, or routes them through an internal virtual paper exchange simulator.

### 7. Parameter Optimizer
A background service responsible for continuous hyperparameter tuning. It performs grid searches across historical datasets to find the most mathematically optimal parameters (such as trailing stop-loss percentages or momentum thresholds) for active strategies, adjusting them as market regimes change.

### 8. Historical Replayer & Data Backfiller
The Data Backfiller strictly downloads historical OHLCV data from the Upstox API while elegantly managing rigorous rate limits. The Historical Replayer is then able to stream this stored historical data back into the main Kafka bus at expedited speeds, mimicking a live market and allowing for extremely accurate, event-driven time-series backtesting.

### 9. API Gateway
A robust FastAPI REST interface that acts as the secure bridge between the internal cluster and external applications. It handles routing and caching (via Redis) for real-time portfolio metrics, historical chart data, strategy management, and live performer leaderboards.

### 10. Quant Frontend
A sleek Next.js resilient dashboard providing a graphical interface for the platform. It visualizes scanner results, active portfolio positions, live equity curves, and allows users to manually backtest custom strategies or transition them cleanly into live execution.

### 11. System Doctor
A comprehensive diagnostics utility that continuously monitors the health of the Kafka broker, the databases, and broker API connectivity, ensuring the platform remains completely stable throughout the volatile trading day.
