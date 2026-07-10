import asyncio
import logging
import time
from typing import Dict, List
from datetime import datetime

from volume_trader import config
from volume_trader.storage.trades import TradeStorage
from volume_trader.storage.signals import SignalStorage
from volume_trader.ai.signal import AISignalGenerator
from volume_trader.notifications.discord import DiscordWebhook

logger = logging.getLogger(__name__)


class SummaryScheduler:
    def __init__(self):
        self.trade_storage = TradeStorage()
        self.signal_storage = SignalStorage()
        self.ai_generator = AISignalGenerator()
        self.webhook = DiscordWebhook()
        
        self.last_summary_time = int(time.time() * 1000)
        self.running = False
        self.previous_summaries = []
        self.max_previous = 2
    
    async def initialize(self):
        await self.trade_storage.ensure_tables()
        await self.signal_storage.ensure_tables()
    
    async def generate_summary(self) -> Dict:
        now = int(time.time() * 1000)
        window_start = now - (config.SUMMARY_INTERVAL_SECONDS * 1000)
        
        stats = await self.trade_storage.get_volume_stats(window_start)
        
        summary_exchanges = []
        for exchange, data in stats.items():
            total_volume_usd = data.get("buy_total", 0) + data.get("sell_total", 0)
            total_volume_btc = data.get("buy_volume", 0) + data.get("sell_volume", 0)
            
            if total_volume_usd < 100:
                continue
            
            buy_ratio = (data.get("buy_total", 0) / total_volume_usd * 100) if total_volume_usd > 0 else 50
            
            summary_exchanges.append({
                "exchange": exchange.upper(),
                "volume_usd": total_volume_usd,
                "volume_btc": total_volume_btc,
                "buy_volume_usd": data.get("buy_total", 0),
                "sell_volume_usd": data.get("sell_total", 0),
                "buy_ratio": buy_ratio,
                "trades": data.get("buy_count", 0) + data.get("sell_count", 0),
                "symbols": data.get("symbols", []),
                "avg_price": data.get("avg_price", 0),
                "price_change": ((data.get("last_price", 0) - data.get("first_price", 0)) 
                               / data.get("first_price", 1) * 100) if data.get("first_price", 0) else 0
            })
        
        summary_exchanges.sort(key=lambda x: x["volume_usd"], reverse=True)
        
        signal_stats = await self.signal_storage.get_signal_stats(window_start)
        
        comparison = self._compare_with_previous(summary_exchanges)
        
        ai_insight = None
        ai_why_no_trade = None
        if summary_exchanges:
            try:
                summary_data_for_ai = {
                    e["exchange"]: {
                        "total_volume_usd": e["volume_usd"],
                        "buy_volume_usd": e["buy_volume_usd"],
                        "sell_volume_usd": e["sell_volume_usd"],
                        "buy_ratio": e["buy_ratio"],
                        "price_change": e.get("price_change", 0)
                    }
                    for e in summary_exchanges[:10]
                }
                ai_insight = await self.ai_generator.generate_summary(summary_data_for_ai, comparison)
                ai_why_no_trade = await self.ai_generator.generate_why_no_trade(summary_exchanges, comparison)
            except Exception as e:
                logger.error(f"Error generating AI insight: {e}")
        
        self.previous_summaries.append({
            "timestamp": now,
            "exchanges": summary_exchanges,
            "total_volume": sum(e["volume_usd"] for e in summary_exchanges)
        })
        if len(self.previous_summaries) > self.max_previous:
            self.previous_summaries.pop(0)
        
        self.last_summary_time = now
        
        return {
            "pairs": summary_exchanges,
            "total_signals": signal_stats.get("total_signals", 0),
            "win_rate": signal_stats.get("win_rate", 0),
            "total_volume": sum(e["volume_usd"] for e in summary_exchanges),
            "ai_insight": ai_insight,
            "ai_why_no_trade": ai_why_no_trade,
            "comparison": comparison
        }
     
    def _compare_with_previous(self, current_exchanges: List[Dict]) -> Dict:
        if not self.previous_summaries:
            return {
                "volume_change": "N/A",
                "volume_change_pct": 0,
                "trend": "first_summary"
            }
        
        prev = self.previous_summaries[-1]
        prev_total = prev.get("total_volume", 0)
        curr_total = sum(e.get("volume_usd", 0) for e in current_exchanges)
        
        if prev_total == 0:
            return {
                "volume_change": "N/A",
                "volume_change_pct": 0,
                "trend": "insufficient_data"
            }
        
        change_pct = ((curr_total - prev_total) / prev_total) * 100
        
        trend = "stable"
        if change_pct > 20:
            trend = "increasing"
        elif change_pct < -20:
            trend = "decreasing"
        
        return {
            "volume_change": curr_total - prev_total,
            "volume_change_pct": change_pct,
            "trend": trend,
            "previous_volume": prev_total,
            "current_volume": curr_total
        }
    
    async def send_summary(self):
        try:
            logger.info("Generating 5-minute summary...")
            
            summary = await self.generate_summary()
            
            success = await self.webhook.send_summary(summary)
            
            if success:
                logger.info(f"Summary sent: {len(summary['pairs'])} exchanges analyzed")
            else:
                logger.error("Failed to send summary")
            
            return success
            
        except Exception as e:
            logger.error(f"Error sending summary: {e}")
            return False
    
    async def start(self):
        logger.info("Starting summary scheduler...")
        self.running = True
        
        await self.initialize()
        
        await self.send_test_message()
        
        while self.running:
            try:
                await asyncio.sleep(config.SUMMARY_INTERVAL_SECONDS)
                
                if self.running:
                    await self.send_summary()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Summary scheduler error: {e}")
    
    async def send_test_message(self):
        try:
            await self.webhook.send_test()
            logger.info("Test message sent")
        except Exception as e:
            logger.error(f"Error sending test message: {e}")
    
    async def stop(self):
        logger.info("Stopping summary scheduler...")
        self.running = False
        await self.webhook.close()


async def run_summary_scheduler():
    scheduler = SummaryScheduler()
    try:
        await scheduler.start()
    except KeyboardInterrupt:
        await scheduler.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_summary_scheduler())
