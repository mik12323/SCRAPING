import logging
import time
from typing import Dict, List, Optional
from collections import defaultdict, deque
from datetime import datetime

from volume_trader.storage.trades import TradeStorage

logger = logging.getLogger(__name__)


class VolumeAnalyzer:
    def __init__(self, rolling_window_seconds: int = 600):
        self.rolling_window_seconds = rolling_window_seconds
        self.trade_storage = TradeStorage()
        
        self.current_trades = deque()
        self.last_check_time = int(time.time() * 1000)
        self.previous_window_volume = {}
        
        self.rolling_volumes = defaultdict(lambda: deque(maxlen=60))
        
    def add_trade(self, trade: Dict):
        self.current_trades.append(trade)
        
        pair = trade["symbol"]
        volume = trade["quantity"]
        
        self.rolling_volumes[pair].append({
            "timestamp": trade["timestamp"],
            "volume": volume,
            "side": trade["side"]
        })
        
    def get_pair_stats(self, pair: str) -> Dict:
        now = int(time.time() * 1000)
        window_start = now - (self.rolling_window_seconds * 1000)
        
        current_volume = 0
        buy_volume = 0
        sell_volume = 0
        buy_count = 0
        sell_count = 0
        trades = []
        
        for item in self.rolling_volumes[pair]:
            if item["timestamp"] >= window_start:
                current_volume += item["volume"]
                trades.append(item)
                if item["side"] == "buy":
                    buy_volume += item["volume"]
                    buy_count += 1
                else:
                    sell_volume += item["volume"]
                    sell_count += 1
        
        total_volume = buy_volume + sell_volume
        buy_ratio = (buy_volume / total_volume * 100) if total_volume > 0 else 50
        
        return {
            "pair": pair,
            "current_volume": current_volume,
            "buy_volume": buy_volume,
            "sell_volume": sell_volume,
            "buy_count": buy_count,
            "sell_count": sell_count,
            "buy_ratio": buy_ratio,
            "total_trades": len(trades)
        }
    
    def get_previous_window_volume(self, pair: str) -> float:
        return self.previous_window_volume.get(pair, 0)
    
    def set_previous_window_volume(self, pair: str, volume: float):
        self.previous_window_volume[pair] = volume
    
    def calculate_volume_ratio(self, pair: str) -> float:
        current = self.get_pair_stats(pair)["current_volume"]
        previous = self.get_previous_window_volume(pair)
        
        if previous == 0:
            return 1.0
        
        return current / previous
    
    def get_all_pairs_stats(self) -> Dict[str, Dict]:
        stats = {}
        for pair in self.rolling_volumes.keys():
            pair_stats = self.get_pair_stats(pair)
            pair_stats["volume_ratio"] = self.calculate_volume_ratio(pair)
            stats[pair] = pair_stats
        
        return stats
    
    def detect_volume_spikes(self, threshold: float = 3.0) -> List[Dict]:
        spikes = []
        all_stats = self.get_all_pairs_stats()
        
        for pair, stats in all_stats.items():
            if stats["current_volume"] < 0.001:
                continue
            
            volume_ratio = stats["volume_ratio"]
            
            if volume_ratio >= threshold:
                spikes.append({
                    "pair": pair,
                    "volume_ratio": volume_ratio,
                    "current_volume": stats["current_volume"],
                    "buy_volume": stats["buy_volume"],
                    "sell_volume": stats["sell_volume"],
                    "buy_ratio": stats["buy_ratio"],
                    "total_trades": stats["total_trades"]
                })
        
        return sorted(spikes, key=lambda x: x["volume_ratio"], reverse=True)
    
    def get_top_pairs(self, limit: int = 10) -> List[Dict]:
        all_stats = self.get_all_pairs_stats()
        
        sorted_pairs = sorted(
            all_stats.items(),
            key=lambda x: x[1]["current_volume"],
            reverse=True
        )
        
        return [
            {
                "pair": pair,
                "volume": stats["current_volume"],
                "buy_volume": stats["buy_volume"],
                "sell_volume": stats["sell_volume"],
                "buy_ratio": stats["buy_ratio"],
                "trades": stats["total_trades"]
            }
            for pair, stats in sorted_pairs[:limit]
        ]


class MomentumAnalyzer:
    def __init__(self):
        self.price_history = defaultdict(lambda: deque(maxlen=100))
        
    def add_price(self, pair: str, price: float, timestamp: int):
        self.price_history[pair].append({
            "price": price,
            "timestamp": timestamp
        })
    
    def calculate_momentum(self, pair: str, window_seconds: int = 60) -> float:
        now = int(time.time() * 1000)
        window_start = now - (window_seconds * 1000)
        
        prices = [
            p["price"] for p in self.price_history[pair]
            if p["timestamp"] >= window_start
        ]
        
        if len(prices) < 2:
            return 0.0
        
        first_price = prices[0]
        last_price = prices[-1]
        
        if first_price == 0:
            return 0.0
        
        return (last_price - first_price) / first_price
    
    def get_price_change(self, pair: str, minutes: int = 10) -> Dict:
        now = int(time.time() * 1000)
        window_start = now - (minutes * 60 * 1000)
        
        prices = [
            p["price"] for p in self.price_history[pair]
            if p["timestamp"] >= window_start
        ]
        
        if not prices:
            return {
                "change_percent": 0,
                "open": 0,
                "high": 0,
                "low": 0,
                "close": 0
            }
        
        return {
            "change_percent": ((prices[-1] - prices[0]) / prices[0] * 100) if prices[0] > 0 else 0,
            "open": prices[0],
            "high": max(prices),
            "low": min(prices),
            "close": prices[-1]
        }


class OrderBookAnalyzer:
    def __init__(self):
        self.order_books = {}
        
    async def fetch_order_book(self, exchange: str, symbol: str) -> Optional[Dict]:
        return None
    
    def calculate_imbalance(self, exchange: str, symbol: str) -> float:
        if exchange not in self.order_books or symbol not in self.order_books[exchange]:
            return 0.5
        
        book = self.order_books[exchange][symbol]
        
        bid_volume = sum(bid[1] for bid in book.get("bids", []))
        ask_volume = sum(ask[1] for ask in book.get("asks", []))
        
        total = bid_volume + ask_volume
        if total == 0:
            return 0.5
        
        return bid_volume / total
    
    def update_order_book(self, exchange: str, symbol: str, bids: List, asks: List):
        if exchange not in self.order_books:
            self.order_books[exchange] = {}
        
        self.order_books[exchange][symbol] = {
            "bids": bids,
            "asks": asks,
            "timestamp": int(time.time() * 1000)
        }


async def get_volume_summary(since_timestamp: int) -> Dict:
    storage = TradeStorage()
    return await storage.get_volume_stats(since_timestamp)


async def get_aggregated_trades(since_timestamp: int, aggregation_seconds: int = 10) -> Dict:
    storage = TradeStorage()
    return await storage.get_trades_aggregated(since_timestamp, aggregation_seconds)
