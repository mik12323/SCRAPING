import asyncio
import aiosqlite
import os
import logging
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data")
PATTERNS_DB = os.path.join(DB_PATH, "patterns.db")


class PatternStorage:
    def __init__(self):
        self.db_path = PATTERNS_DB
        self._initialized = False
    
    async def ensure_tables(self):
        if self._initialized:
            return
        await self._create_tables()
        self._initialized = True
    
    async def _create_tables(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS patterns (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pair TEXT NOT NULL,
                    volume_threshold REAL DEFAULT 3.0,
                    momentum_threshold REAL DEFAULT 0.01,
                    direction TEXT NOT NULL,
                    success_rate REAL DEFAULT 0.0,
                    total_occurrences INTEGER DEFAULT 0,
                    profitable_occurrences INTEGER DEFAULT 0,
                    avg_price_move REAL DEFAULT 0.0,
                    last_updated INTEGER DEFAULT (strftime('%s', 'now'))
                )
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_pair_direction 
                ON patterns(pair, direction)
            """)
            await db.commit()
    
    async def insert_pattern(self, pattern: Dict):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO patterns (pair, volume_threshold, momentum_threshold, direction, 
                                    success_rate, total_occurrences, profitable_occurrences, avg_price_move)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                pattern.get("pair"),
                pattern.get("volume_threshold", 3.0),
                pattern.get("momentum_threshold", 0.01),
                pattern.get("direction"),
                pattern.get("success_rate", 0.0),
                pattern.get("total_occurrences", 1),
                pattern.get("profitable_occurrences", 0),
                pattern.get("avg_price_move", 0.0)
            ))
            await db.commit()
    
    async def update_pattern(self, pattern_id: int, success: bool, price_move: float):
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("""
                SELECT total_occurrences, profitable_occurrences, avg_price_move
                FROM patterns WHERE id = ?
            """, (pattern_id,))
            row = await cursor.fetchone()
            
            if row:
                total = row[0] + 1
                profitable = row[1] + (1 if success else 0)
                old_avg = row[2]
                new_avg = (old_avg * (total - 1) + price_move) / total
                
                await db.execute("""
                    UPDATE patterns SET 
                        total_occurrences = ?,
                        profitable_occurrences = ?,
                        success_rate = ?,
                        avg_price_move = ?,
                        last_updated = strftime('%s', 'now')
                    WHERE id = ?
                """, (total, profitable, profitable / total, new_avg, pattern_id))
                await db.commit()
    
    async def get_patterns(self, pair: Optional[str] = None) -> List[Dict]:
        async with aiosqlite.connect(self.db_path) as db:
            if pair:
                cursor = await db.execute("""
                    SELECT id, pair, volume_threshold, momentum_threshold, direction, 
                           success_rate, total_occurrences, profitable_occurrences, avg_price_move
                    FROM patterns WHERE pair = ? ORDER BY success_rate DESC
                """, (pair,))
            else:
                cursor = await db.execute("""
                    SELECT id, pair, volume_threshold, momentum_threshold, direction,
                           success_rate, total_occurrences, profitable_occurrences, avg_price_move
                    FROM patterns ORDER BY success_rate DESC
                """)
            
            rows = await cursor.fetchall()
            return [
                {
                    "id": row[0],
                    "pair": row[1],
                    "volume_threshold": row[2],
                    "momentum_threshold": row[3],
                    "direction": row[4],
                    "success_rate": row[5],
                    "total_occurrences": row[6],
                    "profitable_occurrences": row[7],
                    "avg_price_move": row[8]
                }
                for row in rows
            ]
    
    async def get_significant_size_threshold(self, pair: str) -> float:
        patterns = await self.get_patterns(pair)
        if not patterns:
            return 0.01
        
        successful = [p for p in patterns if p["success_rate"] > 0.5]
        if not successful:
            return 0.01
        
        return 0.01


async def init_patterns_db():
    await PatternStorage()._create_tables()
    logger.info("Patterns database initialized")


if __name__ == "__main__":
    asyncio.run(init_patterns_db())
