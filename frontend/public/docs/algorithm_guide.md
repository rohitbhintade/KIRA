# KIRA: Kinetic Intelligence for Research & Alpha

Welcome to the **KIRA** Documentation. KIRA is a high-performance quantitative trading and research platform designed for microsecond execution, temporal pattern recognition, and robust backtesting.

This guide outlines the core architecture, the 6-module Deep Edge Scanner, and the Strategy execution APIs.

---

## 1. System Architecture

KIRA operates on a decoupled, asynchronous microservices architecture connected via a **Kafka Event Backbone**. 

### Data Layer
- **QuestDB (Time-Series)**: Ingests and queries millions of ticks/OHLCV data points with Sub-SQL latency.
- **PostgreSQL (Relational)**: Stores metadata, user profiles, strategy definitions, and order histories.
- **Redis (State & Caching)**: Maintains real-time portfolio states, active positions, and order book snapshots.

### Execution Layer
- **Live Strategy Engine**: Executes Python algorithms in real-time. Supports both **MIS (Intraday)** and **CNC (Delivery)** modes.
- **Backtest Runner**: A vectorized, event-driven historical simulation engine that supports custom indicators and slippage models.
- **API Gateway**: A FastAPI interface bridging the Next.js frontend to the backend execution and scanner modules.

---

## 2. KIRA Deep Edge Scanner

The **Deep Edge Scanner** is the flagship research tool of KIRA. It sweeps historical data using Vectorized Pandas and SciPy to find highly probable micro-patterns and inefficiencies across temporal horizons.

The scanner analyzes a given equity across 6 interconnected modules:

1. **Asset Personality Module**: 
   Defines the fundamental behavior of the asset (e.g., *Strong Trend Follower*, *Mean Reverting*). Analyzes Win Rate, Sortino ratio, and directional bias.

2. **Market Regime Engine**:
   Classifies the current state of the instrument (e.g., *Strong Uptrend*, *Consolidation*, *High Volatility Bear*). Calculates localized regime returns and stability scores.

3. **Temporal Inefficiency Detector**:
   Discovers "time-of-day" or "day-of-week" edges. Generates actionable insights, such as *Buy at 10:15 AM* or *Avoid trading on Fridays*.

4. **Multi-Horizon Pattern Recognition**:
   Uses SciPy's `find_peaks` and DTW (Dynamic Time Warping) to identify technical formations (Head & Shoulders, Double Bottoms, Wedges).

5. **Institutional Support/Resistance**:
   Maps liquidity zones using volume profiling and price clustering to determine high-probability bounce or breakout levels.

6. **Volatility & Risk Metrics (ATR/MaxDD)**:
   Measures absolute risk through Average True Range (ATR), Maximum Drawdown, and Sharpe Ratios, guiding position sizing logic.

---

## 3. Live Execution Terminal

The Live Terminal allows you to deploy backtested strategies into live paper-trading (and eventually real brokerage execution).

### Trading Modes
When deploying a strategy, you must select an execution mode:
- **Intraday (MIS)**: The engine automatically squares off positions at 3:20 PM IST. Intraday margin rules and flat-rate brokerage apply.
- **Delivery (CNC)**: The engine holds positions overnight. Delivery STT and standard equity brokerage apply.

### Live Telemetry
The Live Terminal connects to the `Strategy Runtime` WebSocket/HTTP long-polling to provide:
- Real-time Equity curves.
- Order Book updates and simulated fill states.
- Terminal logs containing system heartbeats and trigger events.

---

## 4. Developing Strategies 

Strategies in KIRA are written in Python and inherit from `QCAlgorithm` (modeled after QuantConnect's Lean Engine).

### Basic Template
```python
from AlgorithmImports import *

class MyStrategy(QCAlgorithm):
    def Initialize(self):
        self.SetCash(100000)
        self.SetStartDate(2024, 1, 1)
        self.AddEquity("NSE_EQ|RELIANCE", Resolution.Minute)
        
    def OnData(self, data):
        if "NSE_EQ|RELIANCE" in data:
            price = data["NSE_EQ|RELIANCE"].Close
            if not self.Portfolio.Invested:
                self.SetHoldings("NSE_EQ|RELIANCE", 1.0)
```

### Technical Indicators
Platform-native indicators are created in `Initialize` and auto-update:

```python
def Initialize(self):
    self.rsi = self.RSI("NSE_EQ|RELIANCE", 14, Resolution.Minute)
    self.sma_fast = self.SMA("NSE_EQ|RELIANCE", 50, Resolution.Minute)

def OnData(self, data):
    if not self.rsi.IsReady: return
    
    if self.rsi.Current.Value > 70:
        self.Liquidate("NSE_EQ|RELIANCE")
```

---

## 5. System Health & Telemetry

The platform includes a built-in `System Doctor` that continuously monitors:
- **Kafka Bus Latency**: Ensures event streams are flowing without lag.
- **Database Status**: Heartbeats for Postgres, Redis, and QuestDB.
- **Container Memory/CPU**: Telemetry on the isolated Docker bridge network.

If any component goes down, the Live Execution Engine immediately halts active strategies to prevent unmonitored risk.
