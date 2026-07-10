import asyncio
import aiosqlite
import os
import logging
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data")
SIGNALS_DB = os.path.join(DB_PATH, "signals.db")


class SignalStorage:
    def __init__(self):
        self.db_path = SIGNALS_DB
        self._initialized = False
    
    async def ensure_tables(self):
        if self._initialized:
            return
        await self._create_tables()
        self._initialized = True
    
    async def _create_tables(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pair TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    signal_type TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    confidence INTEGER DEFAULT 50,
                    volume_ratio REAL DEFAULT 1.0,
                    momentum REAL DEFAULT 0.0,
                    reason TEXT,
                    actual_outcome TEXT,
                    profit_loss REAL DEFAULT 0.0,
                    price_at_signal REAL,
                    price_1m REAL,
                    price_3m REAL,
                    price_5m REAL,
                    exchanges TEXT,
                    created_at INTEGER DEFAULT (strftime('%s', 'now'))
                )
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_pair_timestamp 
                ON signals(pair, timestamp)
            """)
            await db.commit()
    
    async def insert_signal(self, signal: Dict) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO signals (pair, timestamp, signal_type, direction, confidence, 
                                    volume_ratio, momentum, reason, price_at_signal, exchanges)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal.get("pair"),
                signal.get("timestamp"),
                signal.get("signal_type"),
                signal.get("direction"),
                signal.get("confidence"),
                signal.get("volume_ratio"),
                signal.get("momentum"),
                signal.get("reason"),
                signal.get("price_at_signal"),
                ",".join(signal.get("exchanges", []))
            ))
            await db.commit()
            
            cursor = await db.execute("SELECT last_insert_rowid()")
            row = await cursor.fetchone()
            return row[0] if row else None
    
    async def update_outcome(self, signal_id: int, outcome_data: Dict):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                UPDATE signals SET 
                    actual_outcome = ?,
                    profit_loss = ?,
                    price_1m = ?,
                    price_3m = ?,
                    price_5m = ?
                WHERE id = ?
            """, (
                outcome_data.get("actual_outcome"),
                outcome_data.get("profit_loss"),
                outcome_data.get("price_1m"),
                outcome_data.get("price_3m"),
                outcome_data.get("price_5m"),
                signal_id
            ))
            await db.commit()
    
    async def get_recent_signals(self, pair: Optional[str] = None, limit: int = 100) -> List[Dict]:
        async with aiosqlite.connect(self.db_path) as db:
            if pair:
                cursor = await db.execute("""
                    SELECT id, pair, timestamp, signal_type, direction, confidence, 
                           volume_ratio, momentum, reason, actual_outcome, profit_loss,
                           price_at_signal, price_1m, price_3m, price_5m, exchanges
                    FROM signals WHERE pair = ? 
                    ORDER BY timestamp DESC LIMIT ?
                """, (pair, limit))
            else:
                cursor = await db.execute("""
                    SELECT id, pair, timestamp, signal_type, direction, confidence,
                           volume_ratio, momentum, reason, actual_outcome, profit_loss,
                           price_at_signal, price_1m, price_3m, price_5m, exchanges
                    FROM signals ORDER BY timestamp DESC LIMIT ?
                """, (limit,))
            
            rows = await cursor.fetchall()
            return [
                {
                    "id": row[0],
                    "pair": row[1],
                    "timestamp": row[2],
                    "signal_type": row[3],
                    "direction": row[4],
                    "confidence": row[5],
                    "volume_ratio": row[6],
                    "momentum": row[7],
                    "reason": row[8],
                    "actual_outcome": row[9],
                    "profit_loss": row[10],
                    "price_at_signal": row[11],
                    "price_1m": row[12],
                    "price_3m": row[13],
                    "price_5m": row[14],
                    "exchanges": row[15].split(",") if row[15] else []
                }
                for row in rows
            ]
    
    async def get_signal_stats(self, since_timestamp: Optional[int] = None) -> Dict:
        async with aiosqlite.connect(self.db_path) as db:
            if since_timestamp:
                cursor = await db.execute("""
                    SELECT COUNT(*) as total,
                           SUM(CASE WHEN actual_outcome = 'profit' THEN 1 ELSE 0 END) as profitable,
                           AVG(profit_loss) as avg_pl
                    FROM signals WHERE timestamp >= ? AND profit_loss IS NOT NULL
                """, (since_timestamp,))
            else:
                cursor = await db.execute("""
                    SELECT COUNT(*) as total,
                           SUM(CASE WHEN actual_outcome = 'profit' THEN 1 ELSE 0 END) as profitable,
                           AVG(profit_loss) as avg_pl
                    FROM signals WHERE profit_loss IS NOT NULL
                """)
            
            row = await cursor.fetchone()
            return {
                "total_signals": row[0] or 0,
                "profitable_signals": row[1] or 0,
                "win_rate": (row[1] / row[0] * 100) if row[0] else 0,
                "avg_profit_loss": row[2] or 0
            }


async def init_signals_db():
    await SignalStorage()._create_tables()
    logger.info("Signals database initialized")


if __name__ == "__main__":
    asyncio.run(init_signals_db())
