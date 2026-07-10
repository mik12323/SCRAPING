import asyncio
import logging
import time
from typing import Dict, List, Optional
from datetime import datetime, timedelta

from volume_trader.storage.signals import SignalStorage
from volume_trader.storage.patterns import PatternStorage

logger = logging.getLogger(__name__)


class PatternLearner:
    def __init__(self):
        self.signal_storage = SignalStorage()
        self.pattern_storage = PatternStorage()
        
        self.pending_outcomes = {}
        self.last_update = 0
        
    async def record_signal(self, signal_id: int, pair: str, direction: str, 
                          price_at_signal: float, volume_ratio: float, momentum: float):
        self.pending_outcomes[signal_id] = {
            "pair": pair,
            "direction": direction,
            "price_at_signal": price_at_signal,
            "volume_ratio": volume_ratio,
            "momentum": momentum,
            "signal_time": int(time.time() * 1000)
        }
        
        logger.info(f"Recorded signal {signal_id} for outcome tracking")
    
    async def update_outcomes(self, current_prices: Dict[str, float]):
        completed = []
        
        for signal_id, data in self.pending_outcomes.items():
            signal_time = data["signal_time"]
            pair = data["pair"]
            direction = data["direction"]
            price_at_signal = data["price_at_signal"]
            
            current_time = int(time.time() * 1000)
            
            time_1m = current_time - signal_time >= 60000
            time_3m = current_time - signal_time >= 180000
            time_5m = current_time - signal_time >= 300000
            
            price_1m = None
            price_3m = None
            price_5m = None
            
            if pair in current_prices:
                if time_5m:
                    price_5m = current_prices[pair]
                elif time_3m:
                    price_3m = current_prices[pair]
                elif time_1m:
                    price_1m = current_prices[pair]
            
            if time_5m and price_5m:
                profit_loss = 0
                if direction == "LONG":
                    profit_loss = ((price_5m - price_at_signal) / price_at_signal) * 100
                elif direction == "SHORT":
                    profit_loss = ((price_at_signal - price_5m) / price_at_signal) * 100
                
                is_profitable = profit_loss > 0.1
                
                await self.signal_storage.update_outcome(signal_id, {
                    "actual_outcome": "profit" if is_profitable else "loss",
                    "profit_loss": profit_loss,
                    "price_1m": price_1m,
                    "price_3m": price_3m,
                    "price_5m": price_5m
                })
                
                await self._update_pattern(pair, direction, is_profitable, profit_loss)
                
                completed.append(signal_id)
                logger.info(f"Completed outcome tracking for signal {signal_id}: {profit_loss:.2f}%")
        
        for signal_id in completed:
            del self.pending_outcomes[signal_id]
    
    async def _update_pattern(self, pair: str, direction: str, 
                             is_profitable: bool, price_move: float):
        patterns = await self.pattern_storage.get_patterns(pair)
        
        existing = None
        for p in patterns:
            if p["direction"] == direction:
                existing = p
                break
        
        if existing:
            await self.pattern_storage.update_pattern(
                existing["id"],
                is_profitable,
                price_move
            )
        else:
            await self.pattern_storage.insert_pattern({
                "pair": pair,
                "volume_threshold": 3.0,
                "momentum_threshold": 0.01,
                "direction": direction,
                "success_rate": 1.0 if is_profitable else 0.0,
                "total_occurrences": 1,
                "profitable_occurrences": 1 if is_profitable else 0,
                "avg_price_move": price_move
            })
    
    async def run_periodic_update(self):
        while True:
            try:
                await asyncio.sleep(60)
                
                logger.debug("Running periodic outcome update...")
                
            except Exception as e:
                logger.error(f"Error in periodic update: {e}")
    
    async def get_performance_stats(self, pair: Optional[str] = None) -> Dict:
        stats = await self.signal_storage.get_signal_stats()
        
        recent = await self.signal_storage.get_recent_signals(pair, limit=50)
        
        signals_with_outcome = [s for s in recent if s.get("profit_loss") is not None]
        
        if signals_with_outcome:
            avg_move = sum(s["profit_loss"] for s in signals_with_outcome) / len(signals_with_outcome)
            profitable = sum(1 for s in signals_with_outcome if s["profit_loss"] > 0)
            win_rate = (profitable / len(signals_with_outcome)) * 100
        else:
            avg_move = 0
            win_rate = 0
        
        return {
            "total_signals": stats["total_signals"],
            "profitable_signals": stats["profitable_signals"],
            "win_rate": win_rate,
            "avg_move": avg_move,
            "recent_signals": len(recent),
            "pending_outcomes": len(self.pending_outcomes)
        }
    
    async def learn_significant_size(self, pair: str) -> float:
        recent = await self.signal_storage.get_recent_signals(pair, limit=100)
        
        trades_with_outcome = [
            s for s in recent 
            if s.get("profit_loss") is not None and s.get("profit_loss", 0) > 0.5
        ]
        
        if not trades_with_outcome:
            return 0.01
        
        volumes = [s.get("volume_ratio", 1) for s in trades_with_outcome]
        avg_volume = sum(volumes) / len(volumes) if volumes else 3.0
        
        new_threshold = 0.01 * (avg_volume / 3.0)
        
        return max(0.005, min(0.1, new_threshold))


async def backtest_signals(days: int = 7):
    learner = PatternLearner()
    
    cutoff = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
    
    signals = await learner.signal_storage.get_recent_signals(limit=1000)
    
    print(f"\n=== Backtest Results (Last {days} days) ===")
    print(f"Total signals: {len(signals)}")
    
    with_outcome = [s for s in signals if s.get("profit_loss") is not None]
    print(f"Signals with outcome: {len(with_outcome)}")
    
    if with_outcome:
        profitable = sum(1 for s in with_outcome if s["profit_loss"] > 0)
        avg_pl = sum(s["profit_loss"] for s in with_outcome) / len(with_outcome)
        
        print(f"Win rate: {profitable/len(with_outcome)*100:.1f}%")
        print(f"Average P/L: {avg_pl:.2f}%")
        
        by_direction = {}
        for s in with_outcome:
            d = s["direction"]
            if d not in by_direction:
                by_direction[d] = {"total": 0, "profitable": 0, "pl": []}
            by_direction[d]["total"] += 1
            if s["profit_loss"] > 0:
                by_direction[d]["profitable"] += 1
            by_direction[d]["pl"].append(s["profit_loss"])
        
        print("\nBy Direction:")
        for d, data in by_direction.items():
            avg = sum(data["pl"]) / len(data["pl"]) if data["pl"] else 0
            print(f"  {d}: {data['profitable']}/{data['total']} ({data['profitable']/data['total']*100:.0f}%) - Avg: {avg:.2f}%")


if __name__ == "__main__":
    import asyncio
    asyncio.run(backtest_signals(7))
