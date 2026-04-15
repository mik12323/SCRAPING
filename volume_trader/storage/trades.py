import asyncio
import aiosqlite
import os
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(DB_PATH, exist_ok=True)

TRADES_DB = os.path.join(DB_PATH, "trades.db")
PATTERNS_DB = os.path.join(DB_PATH, "patterns.db")
SIGNALS_DB = os.path.join(DB_PATH, "signals.db")


class TradeStorage:
    def __init__(self):
        self.db_path = TRADES_DB
        self._initialized = False
    
    async def ensure_tables(self):
        if self._initialized:
            return
        await self._create_tables()
        self._initialized = True
    
    async def _create_tables(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    price REAL NOT NULL,
                    quantity REAL NOT NULL,
                    side TEXT NOT NULL,
                    is_buyer_maker INTEGER DEFAULT 0,
                    trade_id TEXT,
                    created_at INTEGER DEFAULT (strftime('%s', 'now'))
                )
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_exchange_symbol 
                ON trades(exchange, symbol)
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_timestamp 
                ON trades(timestamp)
            """)
            await db.commit()
    
    async def insert_trade(self, trade: Dict):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO trades (exchange, symbol, timestamp, price, quantity, side, is_buyer_maker, trade_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                trade.get("exchange"),
                trade.get("symbol"),
                trade.get("timestamp"),
                trade.get("price"),
                trade.get("quantity"),
                trade.get("side"),
                1 if trade.get("is_buyer_maker") else 0,
                trade.get("trade_id")
            ))
            await db.commit()
    
    async def get_trades_since(self, since_timestamp: int, symbol: Optional[str] = None) -> List[Dict]:
        async with aiosqlite.connect(self.db_path) as db:
            if symbol:
                query = """
                    SELECT exchange, symbol, timestamp, price, quantity, side, is_buyer_maker 
                    FROM trades 
                    WHERE timestamp >= ? AND symbol = ?
                    ORDER BY timestamp DESC
                    LIMIT 10000
                """
                cursor = await db.execute(query, (since_timestamp, symbol))
            else:
                query = """
                    SELECT exchange, symbol, timestamp, price, quantity, side, is_buyer_maker 
                    FROM trades 
                    WHERE timestamp >= ?
                    ORDER BY timestamp DESC
                    LIMIT 10000
                """
                cursor = await db.execute(query, (since_timestamp,))
            
            rows = await cursor.fetchall()
            return [
                {
                    "exchange": row[0],
                    "symbol": row[1],
                    "timestamp": row[2],
                    "price": row[3],
                    "quantity": row[4],
                    "side": row[5],
                    "is_buyer_maker": bool(row[6])
                }
                for row in rows
            ]
    
    async def get_trades_aggregated(self, since_timestamp: int, aggregation_seconds: int = 10) -> Dict:
        trades = await self.get_trades_since(since_timestamp)
        
        aggregated = {}
        for trade in trades:
            key = f"{trade['exchange']}:{trade['symbol']}"
            bucket_time = (trade['timestamp'] // (aggregation_seconds * 1000)) * (aggregation_seconds * 1000)
            
            if key not in aggregated:
                aggregated[key] = {}
            
            if bucket_time not in aggregated[key]:
                aggregated[key][bucket_time] = {
                    "buy_volume": 0,
                    "sell_volume": 0,
                    "buy_count": 0,
                    "sell_count": 0,
                    "buy_total": 0,
                    "sell_total": 0,
                    "prices": [],
                    "trades": []
                }
            
            volume = trade["quantity"]
            price = trade["price"]
            
            if trade["side"] == "buy":
                aggregated[key][bucket_time]["buy_volume"] += volume
                aggregated[key][bucket_time]["buy_count"] += 1
                aggregated[key][bucket_time]["buy_total"] += volume * price
            else:
                aggregated[key][bucket_time]["sell_volume"] += volume
                aggregated[key][bucket_time]["sell_count"] += 1
                aggregated[key][bucket_time]["sell_total"] += volume * price
            
            aggregated[key][bucket_time]["prices"].append(price)
            aggregated[key][bucket_time]["trades"].append(trade)
        
        return aggregated
    
    async def get_volume_stats(self, since_timestamp: int) -> Dict:
        trades = await self.get_trades_since(since_timestamp)
        
        stats = {}
        for trade in trades:
            exchange = trade["exchange"]
            if exchange not in stats:
                stats[exchange] = {
                    "buy_volume": 0,
                    "sell_volume": 0,
                    "buy_count": 0,
                    "sell_count": 0,
                    "buy_total": 0,
                    "sell_total": 0,
                    "trades": [],
                    "symbols": set()
                }
            
            volume = trade["quantity"]
            price = trade["price"]
            stats[exchange]["symbols"].add(trade["symbol"])
            
            if trade["side"] == "buy":
                stats[exchange]["buy_volume"] += volume
                stats[exchange]["buy_count"] += 1
                stats[exchange]["buy_total"] += volume * price
            else:
                stats[exchange]["sell_volume"] += volume
                stats[exchange]["sell_count"] += 1
                stats[exchange]["sell_total"] += volume * price
            
            stats[exchange]["trades"].append(trade)
        
        for exchange in stats:
            stats[exchange]["symbols"] = list(stats[exchange]["symbols"])
            if stats[exchange]["trades"]:
                prices = [t["price"] for t in stats[exchange]["trades"]]
                stats[exchange]["avg_price"] = sum(prices) / len(prices)
                stats[exchange]["high_price"] = max(prices)
                stats[exchange]["low_price"] = min(prices)
                stats[exchange]["first_price"] = prices[0]
                stats[exchange]["last_price"] = prices[-1]
        
        return stats
    
    async def cleanup_old_trades(self, keep_hours: int = 2):
        from volume_trader import config
        cutoff = int((datetime.now() - timedelta(hours=keep_hours)).timestamp() * 1000)
        
        async with aiosqlite.connect(self.db_path) as db:
            c = await db.execute("SELECT COUNT(*) FROM trades WHERE timestamp < ?", (cutoff,))
            old_count = (await c.fetchone())[0]
            
            await db.execute("DELETE FROM trades WHERE timestamp < ?", (cutoff,))
            await db.commit()
            
            logger.info(f"Cleaned up {old_count} trades older than {keep_hours} hours")
            
            return old_count
    
    async def cleanup_small_trades(self, min_usd_value: float = 100):
        async with aiosqlite.connect(self.db_path) as db:
            c = await db.execute("""
                SELECT COUNT(*) FROM trades 
                WHERE (quantity * price) < ?
            """, (min_usd_value,))
            small_count = (await c.fetchone())[0]
            
            await db.execute("""
                DELETE FROM trades 
                WHERE (quantity * price) < ?
            """, (min_usd_value,))
            await db.commit()
            
            logger.info(f"Cleaned up {small_count} trades under ${min_usd_value}")
            
            return small_count
    
    async def get_db_size(self) -> Dict:
        async with aiosqlite.connect(self.db_path) as db:
            c = await db.execute("SELECT COUNT(*) FROM trades")
            total = (await c.fetchone())[0]
            
            c = await db.execute("SELECT SUM(quantity * price) FROM trades")
            total_usd = (await c.fetchone())[0] or 0
            
            return {"total_trades": total, "total_usd": total_usd}


async def init_databases():
    await TradeStorage()._create_tables()
    logger.info("Databases initialized")


if __name__ == "__main__":
    asyncio.run(init_databases())
