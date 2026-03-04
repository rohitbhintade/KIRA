from quant_sdk import QCAlgorithm, Resolution
from datetime import time, timedelta, datetime

class CNCSwingSimple(QCAlgorithm):
    """
    CNC Swing Strategy - Simplified and Working
    """
    
    def Initialize(self):
        self.SetCash(100000)
        self.SetStartDate(2023, 1, 1)
        self.SetEndDate(2024, 12, 31)
        self.SetLeverage(1.0)
        
        # 2 liquid stocks
        self.symbols = [
            "NSE_EQ|INE002A01018",  # Reliance
            "NSE_EQ|INE467B01029",  # TCS
        ]
        
        for sym in self.symbols:
            self.AddEquity(sym, Resolution.Daily)
        
        # Simple parameters
        self.sma_period = 20           # 20-day trend
        self.rsi_period = 14
        self.rsi_entry = 50            # Buy when RSI < 50 (any pullback)
        self.rsi_exit = 60             # Sell when RSI > 60
        
        self.profit_target = 0.05      # 5% profit
        self.stop_loss = 0.03          # 3% stop
        self.max_hold_days = 30        # 1 month max
        
        # Tracking
        self.entry_price = {}
        self.entry_date = {}
        self.in_position = {sym: False for sym in self.symbols}
        self.price_history = {sym: [] for sym in self.symbols}
        
        # Debug counter
        self.bar_count = 0
        
        self.Log("CNC Swing Simple Initialized")

    def CalculateSMA(self, prices, period):
        if len(prices) < period:
            return None
        return sum(prices[-period:]) / period

    def CalculateRSI(self, prices, period):
        if len(prices) < period + 1:
            return None
        
        gains = []
        losses = []
        
        for i in range(1, period + 1):
            change = prices[-i] - prices[-i - 1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))
        
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period
        
        if avg_loss == 0:
            return 100
        if avg_gain == 0:
            return 0
            
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    def GetPrice(self, data, sym):
        """Extract price from data"""
        try:
            tick = data[sym]
            if hasattr(tick, 'Close'):
                return tick.Close
            if hasattr(tick, 'Price'):
                return tick.Price
            if hasattr(tick, 'value'):
                return tick.value
            # Try common attributes
            for attr in ['LastPrice', 'Open', 'High', 'Low']:
                if hasattr(tick, attr):
                    val = getattr(tick, attr)
                    if isinstance(val, (int, float)) and val > 0:
                        return val
            return 0
        except Exception as e:
            self.Log(f"Price error for {sym}: {e}")
            return 0

    def OnData(self, data):
        self.bar_count += 1
        
        for sym in self.symbols:
            if not data.ContainsKey(sym):
                continue
            
            price = self.GetPrice(data, sym)
            if price <= 0:
                continue
            
            # Debug first few bars
            if self.bar_count <= 5:
                self.Log(f"Bar #{self.bar_count}: {sym} = {price}")
            
            # Update history
            self.price_history[sym].append(price)
            if len(self.price_history[sym]) > 50:
                self.price_history[sym].pop(0)
            
            # Wait for warm-up
            if len(self.price_history[sym]) < self.sma_period:
                if self.bar_count <= 5:
                    self.Log(f"Warming up: {len(self.price_history[sym])} / {self.sma_period}")
                continue
            
            # Calculate indicators
            prices = self.price_history[sym]
            sma = self.CalculateSMA(prices, self.sma_period)
            rsi = self.CalculateRSI(prices, self.rsi_period)
            
            if sma is None or rsi is None:
                continue
            
            # Debug indicators periodically
            if self.bar_count % 20 == 0 and sym == self.symbols[0]:
                self.Log(f"Indicators: Price={price:.2f}, SMA={sma:.2f}, RSI={rsi:.1f}")
            
            # Check position
            if self.in_position.get(sym, False):
                self.ManagePosition(sym, price, sma, rsi, data.Time)
            else:
                self.CheckEntry(sym, price, sma, rsi, data.Time)

    def CheckEntry(self, sym, price, sma, rsi, current_time):
        """Simple entry: Above SMA with RSI pullback"""
        
        # Trend: Price above 20-day SMA
        above_sma = price > sma
        
        # Pullback: RSI below 50 (recent weakness)
        rsi_pullback = rsi < self.rsi_entry
        
        # Debug entry conditions
        if self.bar_count % 10 == 0:
            self.Log(f"Entry check {sym}: AboveSMA={above_sma}, RSI={rsi:.1f} < {self.rsi_entry} = {rsi_pullback}")
        
        if above_sma and rsi_pullback:
            # Check available cash
            portfolio = self.Portfolio.TotalPortfolioValue
            cash = self.Portfolio.Cash
            
            # Invest 40% per stock
            position_value = portfolio * 0.40
            if position_value > cash * 0.90:  # Need some cash buffer
                position_value = cash * 0.90
            
            if position_value < 10000:  # Minimum ₹10k
                self.Log(f"Insufficient cash: {cash}")
                return
            
            target_pct = position_value / portfolio
            
            self.SetHoldings(sym, target_pct)
            self.entry_price[sym] = price
            self.entry_date[sym] = current_time
            self.in_position[sym] = True
            
            self.Log(f"*** BUY {sym} @ {price:.2f}, RSI={rsi:.1f}, SMA={sma:.2f}, Size={target_pct:.1%}")

    def ManagePosition(self, sym, price, sma, rsi, current_time):
        """Simple exit logic"""
        entry = self.entry_price[sym]
        entry_date = self.entry_date[sym]
        
        # Calculate days held
        days_held = 0
        try:
            if isinstance(current_time, datetime) and isinstance(entry_date, datetime):
                days_held = (current_time - entry_date).days
        except:
            pass
        
        pnl_pct = (price / entry - 1)
        
        # Exit conditions
        hit_profit = pnl_pct >= self.profit_target      # +5%
        hit_stop = pnl_pct <= -self.stop_loss           # -3%
        time_exit = days_held >= self.max_hold_days     # 30 days
        rsi_high = rsi > self.rsi_exit and pnl_pct > 0  # RSI > 60 with profit
        
        # Debug exit conditions
        if self.bar_count % 5 == 0:
            self.Log(f"Exit check {sym}: PnL={pnl_pct*100:.1f}%, Days={days_held}, RSI={rsi:.1f}")
        
        if hit_profit or hit_stop or time_exit or rsi_high:
            self.Liquidate(sym)
            self.in_position[sym] = False
            
            reason = 'Profit' if hit_profit else 'Stop' if hit_stop else 'Time' if time_exit else 'RSI'
            self.Log(f"*** SELL {sym} @ {price:.2f}, PnL={pnl_pct*100:.2f}%, Days={days_held}, Reason={reason}")