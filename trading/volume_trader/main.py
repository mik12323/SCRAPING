import asyncio
import logging
import signal
import sys
import time
from datetime import datetime

from volume_trader import config
from volume_trader.exchanges.collector import ExchangeCollector
from volume_trader.analyzer.volume import VolumeAnalyzer, MomentumAnalyzer
from volume_trader.ai.signal import AISignalGenerator
from volume_trader.ai.learner import PatternLearner
from volume_trader.notifications.discord import DiscordWebhook
from volume_trader.scheduler.summary import SummaryScheduler
from volume_trader.storage.trades import TradeStorage

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class VolumeTrader:
    def __init__(self):
        self.collector = None
        self.volume_analyzer = VolumeAnalyzer()
        self.momentum_analyzer = MomentumAnalyzer()
        self.ai_generator = AISignalGenerator()
        self.pattern_learner = PatternLearner()
        self.webhook = DiscordWebhook()
        self.summary_scheduler = None
        
        self.running = False
        self.last_volume_check = 0
        self.current_prices = {}
        
    async def on_trade(self, trade: Dict):
        try:
            pair = trade["symbol"]
            
            self.volume_analyzer.add_trade(trade)
            
            self.momentum_analyzer.add_price(
                pair, 
                trade["price"], 
                trade["timestamp"]
            )
            
            self.current_prices[pair] = trade["price"]
            
            pair_upper = pair.upper()
            if "BTCUSDT" in pair_upper or "BTC-USD" in pair_upper or pair_upper == "BTCUSD":
                config.BTC_USD_PRICE = trade["price"]
            
            await self.check_large_trade(trade)
            
        except Exception as e:
            logger.error(f"Error processing trade: {e}")
    
    async def check_large_trade(self, trade: Dict):
        try:
            trade_value_usd = trade.get("quantity", 0) * trade.get("price", 0)
            exchange = trade.get("exchange", "unknown")
            
            if trade_value_usd >= config.LARGE_TRADE_USD_THRESHOLD:
                logger.info(f"Large trade recorded: ${trade_value_usd:.0f} - {exchange} - {trade.get('symbol')} - {trade.get('quantity')}@{trade.get('price')}")
            
            if trade_value_usd >= config.DISCORD_TRADE_THRESHOLD:
                logger.info(f"Discord alert: ${trade_value_usd:.0f} - {exchange} - {trade.get('symbol')} - {trade.get('quantity')}@{trade.get('price')}")
                
                await self.webhook.send_large_trade_alert(trade, trade_value_usd)
                
        except Exception as e:
            logger.error(f"Error checking large trade: {e}")
    
    async def check_volume_spikes(self):
        now = time.time()
        
        if now - self.last_volume_check < config.VOLUME_SPIKE_CHECK_INTERVAL:
            return
        
        self.last_volume_check = now
        
        threshold = config.VOLUME_SPIKE_THRESHOLD
        
        spikes = self.volume_analyzer.detect_volume_spikes(threshold)
        
        if not spikes:
            return
        
        for spike in spikes:
            pair = spike["pair"]
            
            current_price = self.current_prices.get(pair, 0)
            momentum = self.momentum_analyzer.calculate_momentum(pair)
            price_change = self.momentum_analyzer.get_price_change(pair, 10)
            
            market_data = {
                "pair": pair,
                "current_volume": spike["current_volume"],
                "rolling_avg": spike["current_volume"] / spike["volume_ratio"],
                "volume_ratio": spike["volume_ratio"],
                "buy_volume": spike["buy_volume"],
                "sell_volume": spike["sell_volume"],
                "buy_ratio": spike["buy_ratio"],
                "momentum": momentum * 100,
                "price_change": price_change.get("change_percent", 0),
                "exchanges": self._get_active_exchanges(pair),
                "recent_trades": self._get_recent_large_trades(pair),
                "current_price": current_price
            }
            
            signal_result = await self.ai_generator.generate_signal(market_data)
            
            if signal_result and signal_result.get("tradeable"):
                await self.send_signal_alert(market_data, signal_result)
    
    def _get_active_exchanges(self, pair: str) -> List[str]:
        return ["Binance", "Bybit", "KuCoin", "Coinbase"]
    
    def _get_recent_large_trades(self, pair: str) -> List[Dict]:
        trades = []
        for trade in self.volume_analyzer.current_trades:
            if trade.get("symbol") == pair:
                if trade.get("quantity", 0) >= config.MIN_TRADE_SIZE_BTC:
                    trades.append({
                        "quantity": trade.get("quantity", 0),
                        "price": trade.get("price", 0),
                        "side": trade.get("side", ""),
                        "exchange": trade.get("exchange", "")
                    })
        return trades[-10:]
    
    async def send_signal_alert(self, market_data: Dict, signal: Dict):
        alert_data = {
            "pair": market_data["pair"],
            "direction": signal.get("direction", "NEUTRAL"),
            "confidence": signal.get("confidence", 50),
            "volume_ratio": market_data["volume_ratio"],
            "momentum": market_data["momentum"],
            "buy_volume": market_data["buy_volume"],
            "sell_volume": market_data["sell_volume"],
            "reason": signal.get("reasoning", ""),
            "exchanges": market_data["exchanges"]
        }
        
        await self.webhook.send_trade_signal(alert_data)
        
        logger.info(f"Trade signal sent for {market_data['pair']}: {signal.get('direction')} @ {signal.get('confidence')}%")
    
    async def update_pattern_outcomes(self):
        await self.pattern_learner.update_outcomes(self.current_prices)
    
    async def start(self):
        logger.info("=" * 50)
        logger.info("Starting Volume Trader...")
        logger.info("=" * 50)
        
        self.running = True
        
        await self._init_databases()
        
        self.collector = ExchangeCollector(self.on_trade)
        
        self.summary_scheduler = SummaryScheduler()
        summary_task = asyncio.create_task(self.summary_scheduler.start())
        
        collector_task = asyncio.create_task(self.collector.start())
        
        volume_check_task = asyncio.create_task(self._volume_check_loop())
        outcome_task = asyncio.create_task(self._outcome_update_loop())
        cleanup_task = asyncio.create_task(self._cleanup_loop())
        
        await asyncio.gather(
            collector_task,
            volume_check_task,
            outcome_task,
            cleanup_task,
            summary_task
        )
    
    async def _init_databases(self):
        logger.info("Initializing databases...")
        trade_storage = TradeStorage()
        await trade_storage.ensure_tables()
        logger.info("Databases initialized")
    
    async def _volume_check_loop(self):
        while self.running:
            try:
                await asyncio.sleep(config.VOLUME_SPIKE_CHECK_INTERVAL)
                
                if self.running:
                    await self.check_volume_spikes()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Volume check error: {e}")
    
    async def _outcome_update_loop(self):
        while self.running:
            try:
                await asyncio.sleep(60)
                
                if self.running:
                    await self.update_pattern_outcomes()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Outcome update error: {e}")
    
    async def _cleanup_loop(self):
        from volume_trader.storage.trades import TradeStorage
        trade_storage = TradeStorage()
        
        while self.running:
            try:
                cleanup_interval = config.TRADE_CLEANUP_INTERVAL_HOURS * 3600
                await asyncio.sleep(cleanup_interval)
                
                if self.running:
                    old_removed = await trade_storage.cleanup_old_trades(config.TRADE_KEEP_HOURS)
                    small_removed = await trade_storage.cleanup_small_trades(min_usd_value=100)
                    
                    db_stats = await trade_storage.get_db_size()
                    logger.info(f"Cleanup complete. Removed {old_removed} old + {small_removed} small trades. DB has {db_stats['total_trades']} trades (${db_stats['total_usd']:,.0f})")
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cleanup error: {e}")
    
    async def stop(self):
        logger.info("Stopping Volume Trader...")
        self.running = False
        
        if self.collector:
            await self.collector.stop()
        
        if self.summary_scheduler:
            await self.summary_scheduler.stop()
        
        await self.webhook.close()
        
        logger.info("Volume Trader stopped")


async def main():
    trader = VolumeTrader()
    
    def signal_handler(sig, frame):
        logger.info("Received interrupt signal, shutting down...")
        asyncio.create_task(trader.stop())
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await trader.start()
    except KeyboardInterrupt:
        await trader.stop()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        await trader.stop()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
