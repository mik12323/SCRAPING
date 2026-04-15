#!/usr/bin/env python3
"""
Volume Trader - Real-time BTC pairs volume monitoring with AI signals

Usage:
    python run.py

The system will:
1. Connect to multiple exchanges via WebSocket
2. Monitor all BTC quote pairs for volume spikes
3. Use LLM to analyze trade opportunities with directional bias
4. Learn from signals and update patterns in SQLite
5. Send Discord alerts for trade opportunities
6. Send 10-minute market summaries
"""

import asyncio
import logging
import sys

from volume_trader.main import main

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("Volume Trader - Starting...")
    logger.info("=" * 60)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown complete")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
