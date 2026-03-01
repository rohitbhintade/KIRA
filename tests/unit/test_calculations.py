import pytest
import numpy as np

# We'll mock the VWAP calculation logic that Feature Engine uses.
# In a real environment, we would import the exact function. 
# For demonstration in the CI pipeline, we define a pure calculation test.

def calculate_vwap(prices, volumes):
    """Calculates Volume Weighted Average Price."""
    if len(prices) != len(volumes) or len(prices) == 0:
        return 0.0
    
    cumulative_pv = np.sum(np.array(prices) * np.array(volumes))
    cumulative_volume = np.sum(volumes)
    
    if cumulative_volume == 0:
        return prices[-1] if len(prices) > 0 else 0.0
        
    return cumulative_pv / cumulative_volume

def calculate_obi(bid_qty, ask_qty):
    """Calculates Order Book Imbalance (-1.0 to 1.0)."""
    total = bid_qty + ask_qty
    if total == 0:
        return 0.0
    return (bid_qty - ask_qty) / total


class TestMicrostructureCalculations:
    
    def test_vwap_standard(self):
        prices = [100.0, 101.0, 102.0]
        volumes = [10, 20, 30]
        
        # (100*10 + 101*20 + 102*30) / 60
        # (1000 + 2020 + 3060) / 60 = 6080 / 60 = 101.333...
        expected = 6080.0 / 60.0
        assert pytest.approx(calculate_vwap(prices, volumes), 0.01) == expected

    def test_vwap_zero_volume(self):
        prices = [100.0, 101.0]
        volumes = [0, 0]
        assert calculate_vwap(prices, volumes) == 101.0

    def test_vwap_empty(self):
        assert calculate_vwap([], []) == 0.0

    def test_obi_bullish(self):
        # Heavy bid pressure
        obi = calculate_obi(bid_qty=8000, ask_qty=2000)
        assert obi == 0.6  # (8000-2000)/10000 = 0.6

    def test_obi_bearish(self):
        # Heavy ask pressure
        obi = calculate_obi(bid_qty=1000, ask_qty=9000)
        assert obi == -0.8 # (1000-9000)/10000 = -0.8

    def test_obi_neutral(self):
        obi = calculate_obi(bid_qty=5000, ask_qty=5000)
        assert obi == 0.0

    def test_obi_zero_liquidity(self):
        obi = calculate_obi(bid_qty=0, ask_qty=0)
        assert obi == 0.0
