import pytest
import sys
import os

# Add the root directory to PYTHONPATH so we can import internal services
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

# Import the actual production logic we want to test
from services.feature_engine.main import QuantProcessor

class TestFeatureEngineMicrostructure:
    
    def test_vwap_and_obi_standard(self):
        processor = QuantProcessor("TEST_SYM")
        
        # Simulate an Upstox Tick with Order Book Depth
        enriched_1 = processor.process({
            "symbol": "TEST_SYM",
            "ltp": 100.0,
            "v": 10,
            "depth": {
                "buy": [{"price": 99.0, "quantity": 8000}],
                "sell": [{"price": 101.0, "quantity": 2000}]
            }
        })
        
        # VWAP: (100*10)/10 = 100.0
        assert enriched_1["vwap"] == 100.0
        
        # OBI: (8000 - 2000) / 10000 = 0.6
        assert enriched_1["obi"] == 0.6
        
        # Spread: 101.0 - 99.0 = 2.0
        assert enriched_1["spread"] == 2.0
        
        # Tick 2
        enriched_2 = processor.process({
            "symbol": "TEST_SYM",
            "ltp": 101.0,
            "v": 20,
            "depth": {
                "buy": [{"price": 100.0, "quantity": 1000}],
                "sell": [{"price": 102.0, "quantity": 9000}]
            }
        })
        
        # VWAP: (100*10 + 101*20) / 30 = (1000 + 2020)/30 = 3020/30 = 100.67
        assert enriched_2["vwap"] == 100.67
        # OBI: (1000 - 9000) / 10000 = -0.8
        assert enriched_2["obi"] == -0.8
        
    def test_vwap_zero_volume(self):
        processor = QuantProcessor("TEST_SYM")
        enriched = processor.process({
            "symbol": "TEST_SYM",
            "ltp": 105.0,
            "v": 0,
            "depth": {}
        })
        # If no volume, vwap defaults to ltp
        assert enriched["vwap"] == 105.0

    def test_missing_depth_data(self):
        processor = QuantProcessor("TEST_SYM")
        enriched = processor.process({
            "symbol": "TEST_SYM",
            "ltp": 105.0,
            "v": 100
            # no depth key
        })
        
        assert enriched["obi"] == 0.0
        assert enriched["spread"] == 0.0
        assert enriched["aggressor"] == "NEUTRAL"

    def test_aggressor_side_logic(self):
        processor = QuantProcessor("TEST_SYM")
        
        # If LTP hits Best Ask, it was an aggressive BUY
        enriched = processor.process({
            "symbol": "TEST_SYM",
            "ltp": 101.0,
            "v": 100,
            "depth": {
                "buy": [{"price": 100.0, "quantity": 10}],
                "sell": [{"price": 101.0, "quantity": 10}]
            }
        })
        assert enriched["aggressor"] == "BUY"
        
        # If LTP hits Best Bid, it was an aggressive SELL
        enriched2 = processor.process({
            "symbol": "TEST_SYM",
            "ltp": 99.0,
            "v": 100,
            "depth": {
                "buy": [{"price": 99.0, "quantity": 10}],
                "sell": [{"price": 100.0, "quantity": 10}]
            }
        })
        assert enriched2["aggressor"] == "SELL"
