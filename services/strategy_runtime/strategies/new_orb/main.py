from quant_sdk import QCAlgorithm, Resolution
from datetime import time, timedelta

class NiftyIntradayMeanReversion(QCAlgorithm):
    """
    MEAN REVERSION STRATEGY - Buy dips, sell rips
    Works better in Indian market chop
    """
    
    def Initialize(self):
        self.SetCash(100000)
        self.SetStartDate(2023, 1, 1)
        self.SetEndDate(2023, 6, 30)
        self.SetLeverage(5.0)
        
        # Only most liquid
        self.symbols = [
            "NSE_EQ|INE002A01018",  # Reliance
            "NSE_EQ|INE467B01029",  # TCS
        ]
        
        for sym in self.symbols:
            self.AddEquity(sym, Resolution.Minute)
        
        # MEAN REVERSION parameters
        self.lookback = 20           # Lookback for range
        self.entry_zscore = 1.5      # Enter at 1.5 std dev from mean
        self.exit_zscore = 0.5       # Exit at 0.5 std dev (mean reversion)
        self.atr_period = 14
        self.risk_per_trade = 0.005  # 0.5% risk
        self.max_positions = 1
        self.min_hold_minutes = 15   # Minimum hold time
        
        # Tracking
        self.entry_price = {}
        self.stop_loss = {}
        self.target_price = {}
        self.position_direction = {}
        self.entry_time = {}
        self.daily_trade_count = {}
        
        # Price history for calculations
        self.price_history = {sym: [] for sym in self.symbols}
        self.volume_history = {sym: [] for sym in self.symbols}
        
        # Schedule
        self.Schedule.On(self.DateRules.EveryDay(), 
                        self.TimeRules.At(15, 15), 
                        self.LiquidateAllPositions)
        self.Schedule.On(self.DateRules.EveryDay(), 
                        self.TimeRules.At(9, 15), 
                        self.ResetDaily)
        
        self.Log("Mean Reversion Strategy Initialized")

    def ResetDaily(self):
        for sym in self.symbols:
            self.daily_trade_count[sym] = 0
            self.price_history[sym] = []
            self.volume_history[sym] = []
        self.Log("Daily reset")

    def LiquidateAllPositions(self):
        for sym in self.symbols:
            if self.IsInvested(sym):
                self.Liquidate(sym)
                self.position_direction[sym] = 0
        self.Log("EOD Liquidation")

    def IsInvested(self, symbol):
        try:
            return self.Portfolio[symbol].Invested
        except:
            return self.GetQuantity(symbol) != 0

    def GetQuantity(self, symbol):
        try:
            return self.Portfolio[symbol].Quantity
        except:
            return 0

    def GetHoldingsValue(self, symbol):
        try:
            qty = self.GetQuantity(symbol)
            price = self.Portfolio[symbol].Price
            return abs(qty * price)
        except:
            return 0

    def GetTotalExposure(self):
        return sum(self.GetHoldingsValue(s) for s in self.symbols)

    def GetBarFromTick(self, sym, tick_data):
        price = getattr(tick_data, 'Price', getattr(tick_data, 'LastPrice', getattr(tick_data, 'value', None)))
        volume = getattr(tick_data, 'Size', getattr(tick_data, 'Quantity', getattr(tick_data, 'Volume', 0)))
        
        if price is None:
            raise AttributeError("No price in tick")
        
        class SimpleBar:
            def __init__(self, p, v):
                self.Open = p
                self.High = p
                self.Low = p
                self.Close = p
                self.Volume = v
        return SimpleBar(price, volume)

    def GetTime(self, data):
        try:
            t = data.Time
            if isinstance(t, str):
                from datetime import datetime
                dt = datetime.fromisoformat(t.replace('Z', '+00:00'))
                return dt.time()
            return t.time() if hasattr(t, 'time') else t
        except:
            return None

    def CalculateStats(self, prices):
        """Calculate mean, std dev, z-score"""
        if len(prices) < self.lookback:
            return None, None, None
        
        recent = prices[-self.lookback:]
        mean = sum(recent) / len(recent)
        
        # Standard deviation
        variance = sum((p - mean) ** 2 for p in recent) / len(recent)
        std = variance ** 0.5
        
        current = prices[-1]
        zscore = (current - mean) / std if std > 0 else 0
        
        return mean, std, zscore

    def CalculateATR(self, sym):
        history = self.price_history[sym]
        if len(history) < 2:
            return None
        
        tr_values = []
        for i in range(1, min(len(history), self.atr_period + 1)):
            curr = history[-i]
            prev = history[-i-1]
            tr = max(curr['high'] - curr['low'],
                     abs(curr['high'] - prev['close']),
                     abs(curr['low'] - prev['close']))
            tr_values.append(tr)
        
        if len(tr_values) >= self.atr_period:
            return sum(tr_values[:self.atr_period]) / self.atr_period
        return None

    def OnData(self, data):
        current_time = self.GetTime(data)
        if current_time is None:
            return
        
        # Skip first hour (let range establish)
        if current_time < time(9, 30):
            return
        
        # No new entries after 2:30 PM
        if current_time > time(14, 30):
            # Manage existing only
            for sym in self.symbols:
                if self.position_direction.get(sym, 0) != 0:
                    self.ManageExit(sym, data, current_time)
            return
        
        # Force exit at 3:10 PM
        if current_time >= time(15, 10):
            for sym in self.symbols:
                if self.IsInvested(sym):
                    self.Liquidate(sym)
                    self.position_direction[sym] = 0
            return
        
        for sym in self.symbols:
            if not data.ContainsKey(sym):
                continue
            
            # Get bar
            try:
                tick_data = data[sym]
                if hasattr(tick_data, 'Open'):
                    bar = tick_data
                else:
                    bar = self.GetBarFromTick(sym, tick_data)
            except Exception as e:
                continue
            
            # Update history
            self.price_history[sym].append({
                'high': bar.High,
                'low': bar.Low,
                'close': bar.Close
            })
            self.volume_history[sym].append(bar.Volume)
            
            # Limit history
            max_hist = max(self.lookback, self.atr_period) + 10
            if len(self.price_history[sym]) > max_hist:
                self.price_history[sym].pop(0)
                self.volume_history[sym].pop(0)
            
            # Need enough data
            if len(self.price_history[sym]) < self.lookback:
                continue
            
            # Calculate stats
            closes = [p['close'] for p in self.price_history[sym]]
            mean, std, zscore = self.CalculateStats(closes)
            atr = self.CalculateATR(sym)
            
            if mean is None or atr is None or atr == 0:
                continue
            
            # Manage existing position
            if self.position_direction.get(sym, 0) != 0:
                self.ManageExit(sym, bar, mean, std, zscore, current_time)
                continue
            
            # Check entry (max 2 trades per day per symbol)
            if self.daily_trade_count.get(sym, 0) >= 2:
                continue
            
            self.CheckEntry(sym, bar, mean, std, zscore, atr, current_time)

    def CheckEntry(self, sym, bar, mean, std, zscore, atr):
        """MEAN REVERSION entries: Buy when price below mean, sell when above"""
        portfolio_value = self.Portfolio.TotalPortfolioValue
        total_exposure = self.GetTotalExposure()
        
        if total_exposure / portfolio_value > 0.5:
            return
        
        # LONG: Price is stretched below mean (oversold)
        if zscore < -self.entry_zscore:
            # Additional: Check if in uptrend (higher lows) - optional filter
            # Remove this if you want pure mean reversion
            recent_lows = [p['low'] for p in self.price_history[sym][-5:]]
            if recent_lows[-1] > min(recent_lows):  # Higher low forming
                self.EnterLong(sym, bar, mean, atr)
        
        # SHORT: Price is stretched above mean (overbought)
        elif zscore > self.entry_zscore:
            recent_highs = [p['high'] for p in self.price_history[sym][-5:]]
            if recent_highs[-1] < max(recent_highs):  # Lower high forming
                self.EnterShort(sym, bar, mean, atr)

    def EnterLong(self, sym, bar, mean, atr):
        """Enter long position targeting mean"""
        portfolio_value = self.Portfolio.TotalPortfolioValue
        risk_amount = portfolio_value * self.risk_per_trade
        
        # Stop below recent low or 2 ATR
        recent_lows = [p['low'] for p in self.price_history[sym][-5:]]
        stop_price = min(min(recent_lows), bar.Close - 2 * atr)
        stop_distance = bar.Close - stop_price
        
        if stop_distance <= 0:
            return
        
        position_value = (risk_amount / stop_distance) * bar.Close
        max_position = portfolio_value * 0.25
        position_value = min(position_value, max_position)
        target_pct = position_value / portfolio_value
        
        if target_pct < 0.05:
            return
        
        self.SetHoldings(sym, target_pct)
        self.entry_price[sym] = bar.Close
        self.stop_loss[sym] = stop_price
        self.target_price[sym] = mean  # Target is the mean (reversion)
        self.position_direction[sym] = 1
        self.entry_time[sym] = bar.Close  # Track entry for time exit
        self.daily_trade_count[sym] = self.daily_trade_count.get(sym, 0) + 1
        
        self.Log(f"LONG {sym} @ {bar.Close:.2f}, Target={mean:.2f}, SL={stop_price:.2f}")

    def EnterShort(self, sym, bar, mean, atr):
        """Enter short position targeting mean"""
        portfolio_value = self.Portfolio.TotalPortfolioValue
        risk_amount = portfolio_value * self.risk_per_trade
        
        recent_highs = [p['high'] for p in self.price_history[sym][-5:]]
        stop_price = max(max(recent_highs), bar.Close + 2 * atr)
        stop_distance = stop_price - bar.Close
        
        if stop_distance <= 0:
            return
        
        position_value = (risk_amount / stop_distance) * bar.Close
        max_position = portfolio_value * 0.25
        position_value = min(position_value, max_position)
        target_pct = -position_value / portfolio_value
        
        if abs(target_pct) < 0.05:
            return
        
        self.SetHoldings(sym, target_pct)
        self.entry_price[sym] = bar.Close
        self.stop_loss[sym] = stop_price
        self.target_price[sym] = mean
        self.position_direction[sym] = -1
        self.entry_time[sym] = bar.Close
        self.daily_trade_count[sym] = self.daily_trade_count.get(sym, 0) + 1
        
        self.Log(f"SHORT {sym} @ {bar.Close:.2f}, Target={mean:.2f}, SL={stop_price:.2f}")

    def ManageExit(self, sym, bar, mean, std, zscore, current_time):
        """Exit when price reverts to mean or stop hit"""
        direction = self.position_direction[sym]
        entry = self.entry_price[sym]
        stop = self.stop_loss[sym]
        target = self.target_price[sym]
        price = bar.Close
        
        # Minimum hold time check (avoid churn)
        # Simplified - just use bar count approximation
        
        if direction == 1:  # Long
            # Exit conditions
            hit_target = price >= target  # Reached mean
            hit_stop = price <= stop
            extended = zscore > 0  # Price now above mean (overbought)
            
            if hit_target or hit_stop or extended:
                self.Liquidate(sym)
                pnl = (price / entry - 1) * 100
                reason = 'Target' if hit_target else 'Stop' if hit_stop else 'Extended'
                self.Log(f"EXIT LONG {sym} @ {price:.2f}, PnL={pnl:.2f}%, {reason}")
                self.position_direction[sym] = 0
        
        else:  # Short
            hit_target = price <= target
            hit_stop = price >= stop
            extended = zscore < 0  # Price now below mean (oversold)
            
            if hit_target or hit_stop or extended:
                self.Liquidate(sym)
                pnl = (entry / price - 1) * 100
                reason = 'Target' if hit_target else 'Stop' if hit_stop else 'Extended'
                self.Log(f"EXIT SHORT {sym} @ {price:.2f}, PnL={pnl:.2f}%, {reason}")
                self.position_direction[sym] = 0