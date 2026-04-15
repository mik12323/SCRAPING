import asyncio
import logging

logging.basicConfig(level=logging.INFO)

async def test():
    from volume_trader.exchanges.collector import ExchangeCollector
    
    trade_count = 0
    
    async def on_trade(trade):
        nonlocal trade_count
        trade_count += 1
        val = trade.get('quantity', 0) * trade.get('price', 0)
        exchange = trade.get('exchange', 'unknown')
        symbol = trade.get('symbol', 'unknown')
        print(f'Trade #{trade_count}: {exchange} - {symbol} - ${val:.0f}')
        
        if trade_count >= 20:
            raise KeyboardInterrupt('Test complete')
    
    collector = ExchangeCollector(on_trade)
    try:
        await collector.start()
    except KeyboardInterrupt:
        print(f'Total trades: {trade_count}')
        await collector.stop()

asyncio.run(test())
