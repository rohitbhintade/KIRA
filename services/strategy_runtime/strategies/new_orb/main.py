from quant_sdk import QCAlgorithm, Resolution
from datetime import time, timedelta
import numpy as np

class NiftyIntradayMomentum(QCAlgorithm):
    """
    Multi-Factor Intraday Momentum Strategy for Indian Markets
    
    Logic:
    1. Opening Range Breakout (ORB): First 15-minute range establishes bias
    2. Volume Confirmation: Breakout must be accompanied by 1.5x average volume
    3. EMA Trend Filter: Only trade in direction of 9 EMA slope
    4. Dynamic Position Sizing: Based on ATR volatility
    5. Hard Stop-Loss: 1.5x ATR or time-based exit at 3:15 PM
    """
    
    def Initialize(self):
        # Capital & Dates
        self.SetCash(500000)  # ₹5L - realistic for Indian retail
        self.SetStartDate(2023, 1, 1)
        self.SetEndDate(2024, 12, 31)
        
        # Leverage for Intraday (MIS)
        self.SetLeverage(5.0)  # 5x intraday leverage typical in India
        
        # Universe: Top 10 Nifty 50 liquid stocks
        # Format: NSE_EQ|ISIN - Reliance, TCS, HDFC Bank, ICICI Bank, Infosys, etc.
        self.symbols = [
            "NSE_EQ|INE002A01018",  # Reliance
            "NSE_EQ|INE467B01029",  # TCS
            "NSE_EQ|INE040A01034",  # HDFC Bank
            "NSE_EQ|INE090A01021",  # ICICI Bank
            "NSE_EQ|INE009A01021",  # Infosys
            "NSE_EQ|INE154A01025",  # ITC
            "NSE_EQ|INE238A01034",  # Kotak Mahindra
            "NSE_EQ|INE062A01020",  # L&T
            "NSE_EQ|INE917I01010",  # Axis Bank
            "NSE_EQ|INE669E01016",  # Bajaj Finance
        ]
        
        # Resolution: Minute bars for precise entry/exit
        for sym in self.symbols:
            self.AddEquity(sym, Resolution.Minute)
        
        # Strategy Parameters
        self.orb_minutes = 15           # Opening range period
        self.volume_lookback = 20       # Volume SMA period
        self.atr_period = 14            # ATR for position sizing & stops
        self.ema_fast = 9               # Fast EMA for trend
        self.ema_slow = 21              # Slow EMA for trend confirmation
        self.volume_threshold = 1.5     # Volume breakout multiplier
        self.risk_per_trade = 0.01      # 1% risk per trade
        self.max_positions = 3          # Max concurrent positions
        
        # Tracking dictionaries for each symbol
        self.orb_high = {}
        self.orb_low = {}
        self.orb_volume_avg = {}
        self.daily_atr = {}
        self.entry_price = {}
        self.stop_loss = {}
        self.target_price = {}
        self.position_direction = {}    # 1 for long, -1 for short, 0 for none
        
        # Indicators per symbol
        self.indicators = {}
        for sym in self.symbols:
            self.indicators[sym] = {
                'sma_volume': self.SMA(sym, self.volume_lookback, Resolution.Minute),
                'atr': self.ATR(sym, self.atr_period, Resolution.Minute),
                'ema_fast': self.EMA(sym, self.ema_fast, Resolution.Minute),
                'ema_slow': self.EMA(sym, self.ema_slow, Resolution.Minute),
                'high_15': self.MAX(sym, self.orb_minutes, Resolution.Minute),  # Rolling 15-min high
                'low_15': self.MIN(sym, self.orb_minutes, Resolution.Minute),   # Rolling 15-min low
            }
        
        # Schedule end-of-day liquidation (3:15 PM IST)
        self.Schedule.On(self.DateRules.EveryDay(), 
                        self.TimeRules.At(15, 15), 
                        self.LiquidateAllPositions)
        
        # Reset ORB levels at market open (9:15 AM IST)
        self.Schedule.On(self.DateRules.EveryDay(), 
                        self.TimeRules.At(9, 15), 
                        self.ResetORBLevels)
        
        self.Log("Strategy Initialized - Nifty Intraday Momentum v1.0")

    def ResetORBLevels(self):
        """Reset Opening Range levels at market open"""
        for sym in self.symbols:
            self.orb_high[sym] = None
            self.orb_low[sym] = None
            self.orb_volume_avg[sym] = None
            self.daily_atr[sym] = None
        self.Log("ORB levels reset for new trading day")

    def LiquidateAllPositions(self):
        """Square off all positions at 3:15 PM IST (MIS requirement)"""
        for sym in self.symbols:
            if self.Portfolio[sym].Invested:
                self.Liquidate(sym)
                self.Log(f"EOD Liquidation: {sym}")
                self.position_direction[sym] = 0
        self.Log("All positions liquidated for EOD")

    def OnData(self, data):
        current_time = data.Time.time()
        
        # Market hours: 9:15 AM to 3:30 PM IST
        # No new trades after 2:45 PM (sufficient time to exit)
        if current_time < time(9, 15) or current_time > time(14, 45):
            return
        
        for sym in self.symbols:
            if not data.ContainsKey(sym):
                continue
            
            bar = data[sym]
            ind = self.indicators[sym]
            
            # Wait for indicators to warm up
            if not all([ind['sma_volume'].IsReady, 
                       ind['atr'].IsReady,
                       ind['ema_fast'].IsReady, 
                       ind['ema_slow'].IsReady]):
                continue
            
            # Calculate ORB levels during first 15 minutes
            if current_time <= time(9, 30):
                self.UpdateORBLevels(sym, bar, ind)
                continue
            
            # Trading logic only after ORB established and not already in position
            if self.position_direction.get(sym, 0) == 0:
                self.CheckEntrySignals(sym, bar, ind, current_time)
            else:
                self.ManageOpenPosition(sym, bar, ind)

    def UpdateORBLevels(self, sym, bar, ind):
        """Track highest high and lowest low during opening range"""
        if self.orb_high.get(sym) is None or bar.High > self.orb_high[sym]:
            self.orb_high[sym] = bar.High
        
        if self.orb_low.get(sym) is None or bar.Low < self.orb_low[sym]:
            self.orb_low[sym] = bar.Low
        
        # Store pre-market average volume for comparison
        if self.orb_volume_avg.get(sym) is None and ind['sma_volume'].IsReady:
            self.orb_volume_avg[sym] = ind['sma_volume'].Value

    def CheckEntrySignals(self, sym, bar, ind, current_time):
        """Check for ORB breakout with confirmation filters"""
        if self.orb_high.get(sym) is None or self.orb_low.get(sym) is None:
            return
        
        # Trend filter: Only trade in direction of EMA alignment
        trend_bullish = ind['ema_fast'].Value > ind['ema_slow'].Value
        trend_bearish = ind['ema_fast'].Value < ind['ema_slow'].Value
        
        # Volume confirmation: Current volume > 1.5x average
        volume_confirmed = bar.Volume > (self.orb_volume_avg.get(sym, bar.Volume) * self.volume_threshold)
        
        # Position sizing based on ATR volatility
        atr_value = ind['atr'].Value
        if atr_value == 0:
            return
        
        # Count current positions
        current_positions = sum(1 for v in self.position_direction.values() if v != 0)
        if current_positions >= self.max_positions:
            return
        
        # LONG Setup: Break above ORB high + bullish trend + volume
        if bar.Close > self.orb_high[sym] and trend_bullish and volume_confirmed:
            # Calculate position size: Risk 1% of portfolio per trade
            portfolio_value = self.Portfolio.TotalPortfolioValue
            risk_amount = portfolio_value * self.risk_per_trade
            position_size = risk_amount / (1.5 * atr_value)  # Stop loss at 1.5 ATR
            
            # Convert to percentage of portfolio (max 30% per position)
            max_position_value = portfolio_value * 0.30
            shares = min(position_size, max_position_value / bar.Close)
            target_pct = (shares * bar.Close) / portfolio_value
            
            if target_pct > 0.05:  # Minimum 5% allocation
                self.SetHoldings(sym, target_pct)
                self.entry_price[sym] = bar.Close
                self.stop_loss[sym] = bar.Close - (1.5 * atr_value)
                self.target_price[sym] = bar.Close + (3.0 * atr_value)  # 1:2 risk-reward
                self.position_direction[sym] = 1
                
                self.Log(f"LONG {sym} @ {bar.Close:.2f}, "
                        f"Size: {target_pct:.2%}, "
                        f"SL: {self.stop_loss[sym]:.2f}, "
                        f"TG: {self.target_price[sym]:.2f}, "
                        f"Vol: {bar.Volume/ind['sma_volume'].Value:.1f}x")
        
        # SHORT Setup: Break below ORB low + bearish trend + volume
        elif bar.Close < self.orb_low[sym] and trend_bearish and volume_confirmed:
            portfolio_value = self.Portfolio.TotalPortfolioValue
            risk_amount = portfolio_value * self.risk_per_trade
            position_size = risk_amount / (1.5 * atr_value)
            
            max_position_value = portfolio_value * 0.30
            shares = min(position_size, max_position_value / bar.Close)
            target_pct = -(shares * bar.Close) / portfolio_value  # Negative for short
            
            if abs(target_pct) > 0.05:
                self.SetHoldings(sym, target_pct)
                self.entry_price[sym] = bar.Close
                self.stop_loss[sym] = bar.Close + (1.5 * atr_value)
                self.target_price[sym] = bar.Close - (3.0 * atr_value)
                self.position_direction[sym] = -1
                
                self.Log(f"SHORT {sym} @ {bar.Close:.2f}, "
                        f"Size: {target_pct:.2%}, "
                        f"SL: {self.stop_loss[sym]:.2f}, "
                        f"TG: {self.target_price[sym]:.2f}")

    def ManageOpenPosition(self, sym, bar, ind):
        """Manage open positions: trailing stops, target hits, time exits"""
        direction = self.position_direction.get(sym, 0)
        if direction == 0:
            return
        
        current_price = bar.Close
        entry = self.entry_price.get(sym, current_price)
        stop = self.stop_loss.get(sym, current_price)
        target = self.target_price.get(sym, current_price)
        
        # ATR-based trailing stop logic (lock in profits)
        atr = ind['atr'].Value
        
        if direction == 1:  # Long position
            # Target hit - take profit
            if current_price >= target:
                self.Liquidate(sym)
                self.Log(f"LONG Target Hit {sym} @ {current_price:.2f}, "
                        f"PnL: {(current_price/entry - 1)*100:.2f}%")
                self.position_direction[sym] = 0
                return
            
            # Stop loss hit
            if current_price <= stop:
                self.Liquidate(sym)
                self.Log(f"LONG Stop Loss {sym} @ {current_price:.2f}, "
                        f"Loss: {(current_price/entry - 1)*100:.2f}%")
                self.position_direction[sym] = 0
                return
            
            # Trailing stop: Move SL to breakeven + 0.5 ATR once up 1 ATR
            if current_price > entry + atr and stop < entry:
                new_stop = entry + (0.5 * atr)
                if new_stop > stop:
                    self.stop_loss[sym] = new_stop
                    self.Log(f"Trailing Stop Updated {sym}: {new_stop:.2f}")
        
        else:  # Short position
            # Target hit
            if current_price <= target:
                self.Liquidate(sym)
                self.Log(f"SHORT Target Hit {sym} @ {current_price:.2f}, "
                        f"PnL: {(entry/current_price - 1)*100:.2f}%")
                self.position_direction[sym] = 0
                return
            
            # Stop loss hit
            if current_price >= stop:
                self.Liquidate(sym)
                self.Log(f"SHORT Stop Loss {sym} @ {current_price:.2f}, "
                        f"Loss: {(entry/current_price - 1)*100:.2f}%")
                self.position_direction[sym] = 0
                return
            
            # Trailing stop for shorts
            if current_price < entry - atr and stop > entry:
                new_stop = entry - (0.5 * atr)
                if new_stop < stop:
                    self.stop_loss[sym] = new_stop
                    self.Log(f"Trailing Stop Updated {sym}: {new_stop:.2f}")