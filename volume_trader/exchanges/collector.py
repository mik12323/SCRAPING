import asyncio
import json
import logging
import time
from typing import Dict, List, Callable, Any, Optional
import aiohttp

from volume_trader import config
from volume_trader.storage.trades import TradeStorage

logger = logging.getLogger(__name__)

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"
BINANCE_STREAM_URL = "wss://stream.binance.com:9443/stream"
BINANCE_FUTURES_WS = "wss://fstream.binance.com/ws"

BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/spot"
BYBIT_PERP_WS = "wss://stream.bybit.com/v5/public/linear"

COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"

KUCOIN_WS_URL = "wss://ws-api.kucoin.com"

OKX_WS = "wss://ws.okx.com:8443/ws/v5/public"

KRAKEN_WS = "wss://ws.kraken.com"

BITSTAMP_WS = "wss://ws.bitstamp.net"

BITGET_WS = "wss://ws.bitget.com"

BITFINEX_WS = "wss://api-pub.bitfinex.com/ws/2"

HYPERLIQUID_WS = "wss://api.hyperliquid.xyz/ws"


EXCHANGE_WEBSOCKETS = {
    "binance": {
        "url": BINANCE_STREAM_URL,
        "symbols": ["btcusdt@trade"],
        "parse": lambda msg: _parse_binance_trade(msg)
    },
    "bybit": {
        "url": BYBIT_WS_URL,
        "symbols": ["BTCUSDT"],
        "parse": lambda msg: _parse_bybit_trade(msg)
    },
    "coinbase": {
        "url": COINBASE_WS_URL,
        "symbols": ["BTC-USD"],
        "parse": lambda msg: _parse_coinbase_trade(msg)
    }
}


def _parse_binance_trade(data: Dict) -> Optional[Dict]:
    if "data" not in data:
        return None
    
    d = data["data"]
    return {
        "exchange": "binance",
        "symbol": _normalize_symbol(d.get("s", "")),
        "timestamp": d.get("T", int(time.time() * 1000)),
        "price": float(d.get("p", 0)),
        "quantity": float(d.get("q", 0)),
        "side": "sell" if d.get("m", True) else "buy",
        "is_buyer_maker": d.get("m", True),
        "trade_id": d.get("a")
    }


def _parse_bybit_trade(data: Dict) -> Optional[Dict]:
    if data.get("topic") != "trade":
        return None
    
    for d in data.get("data", []):
        return {
            "exchange": "bybit",
            "symbol": _normalize_symbol(d.get("s", "")),
            "timestamp": int(d.get("T", time.time() * 1000)),
            "price": float(d.get("p", 0)),
            "quantity": float(d.get("q", 0)),
            "side": "buy" if d.get("S") == "Buy" else "sell",
            "is_buyer_maker": d.get("M", False),
            "trade_id": d.get("i")
        }
    return None


def _parse_bybit_perp_trade(data: Dict) -> Optional[Dict]:
    topic = data.get("topic", "")
    if "trade" not in topic:
        return None
    
    for d in data.get("data", []):
        return {
            "exchange": "bybit_perp",
            "symbol": _normalize_symbol(d.get("s", "")),
            "timestamp": int(d.get("T", time.time() * 1000)),
            "price": float(d.get("p", 0)),
            "quantity": float(d.get("q", 0)),
            "side": "buy" if d.get("S") == "Buy" else "sell",
            "is_buyer_maker": d.get("M", False),
            "trade_id": d.get("i")
        }
    return None


def _parse_binance_perp_trade(data: Dict) -> Optional[Dict]:
    if "data" not in data:
        return None
    
    d = data["data"]
    return {
        "exchange": "binance_perp",
        "symbol": _normalize_symbol(d.get("s", "")),
        "timestamp": d.get("T", int(time.time() * 1000)),
        "price": float(d.get("p", 0)),
        "quantity": float(d.get("q", 0)),
        "side": "sell" if d.get("m", True) else "buy",
        "is_buyer_maker": d.get("m", True),
        "trade_id": d.get("a")
    }


def _parse_okx_trade(data: Dict) -> Optional[Dict]:
    try:
        if data.get("arg", {}).get("channel") != "trades":
            return None
        
        for d in data.get("data", []):
            return {
                "exchange": "okx",
                "symbol": _normalize_symbol(d.get("instId", "")),
                "timestamp": int(d.get("ts", time.time() * 1000)),
                "price": float(d.get("px", 0)),
                "quantity": float(d.get("sz", 0)),
                "side": "buy" if d.get("side") == "buy" else "sell",
                "is_buyer_maker": False,
                "trade_id": d.get("tradeId")
            }
    except:
        pass
    return None


def _parse_kraken_trade(data: Dict) -> Optional[Dict]:
    try:
        if isinstance(data, list) and len(data) > 1:
            if data[0] == "trade":
                d = data[1]
                return {
                    "exchange": "kraken",
                    "symbol": _normalize_symbol(d[0]),
                    "timestamp": int(d[2] * 1000),
                    "price": float(d[1]),
                    "quantity": float(d[3]) if len(d) > 3 else 0,
                    "side": "sell" if d[4] == "s" else "buy",
                    "is_buyer_maker": False,
                    "trade_id": d[5] if len(d) > 5 else None
                }
    except:
        pass
    return None


def _parse_bitstamp_trade(data: Dict) -> Optional[Dict]:
    try:
        if data.get("channel") != "live_trades_btcusd":
            return None
        
        d = data.get("data", {})
        return {
            "exchange": "bitstamp",
            "symbol": "BTC/USD",
            "timestamp": int(float(d.get("timestamp", 0)) * 1000),
            "price": float(d.get("price", 0)),
            "quantity": float(d.get("amount", 0)),
            "side": "buy" if d.get("type") == "buy" else "sell",
            "is_buyer_maker": d.get("maker") == 1,
            "trade_id": d.get("id")
        }
    except:
        pass
    return None


def _parse_bitget_trade(data: Dict) -> Optional[Dict]:
    try:
        if data.get("op") != "subscribe":
            d = data.get("data", {})
            if "trades" in str(data):
                return {
                    "exchange": "bitget",
                    "symbol": _normalize_symbol(d.get("instId", "")),
                    "timestamp": int(d.get("ts", time.time() * 1000)),
                    "price": float(d.get("px", 0)),
                    "quantity": float(d.get("sz", 0)),
                    "side": "buy" if d.get("side") == "buy" else "sell",
                    "is_buyer_maker": False,
                    "trade_id": d.get("tradeId")
                }
    except:
        pass
    return None


def _parse_bitfinex_trade(data: Dict) -> Optional[Dict]:
    try:
        if not isinstance(data, list):
            return None
        
        for d in data:
            if isinstance(d, list) and len(d) >= 8:
                if d[0] == "te":
                    return {
                        "exchange": "bitfinex",
                        "symbol": _normalize_symbol(d[1]),
                        "timestamp": int(d[5] * 1000),
                        "price": float(d[3]),
                        "quantity": float(d[2]),
                        "side": "sell" if d[4] < 0 else "buy",
                        "is_buyer_maker": False,
                        "trade_id": d[0]
                    }
    except:
        pass
    return None


def _parse_coinbase_trade(data: Dict) -> Optional[Dict]:
    if data.get("type") != "match":
        return None
    
    time_str = data.get("time", "")
    try:
        timestamp = int(float(time_str) * 1000)
    except (ValueError, TypeError):
        from datetime import datetime
        try:
            dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
            timestamp = int(dt.timestamp() * 1000)
        except:
            timestamp = int(time.time() * 1000)
    
    return {
        "exchange": "coinbase",
        "symbol": _normalize_symbol(data.get("product_id", "")),
        "timestamp": timestamp,
        "price": float(data.get("price", 0)),
        "quantity": float(data.get("size", 0)),
        "side": "buy" if data.get("side") == "buy" else "sell",
        "is_buyer_maker": data.get("maker_order_id") is not None,
        "trade_id": data.get("trade_id")
    }


def _parse_hyperliquid_trade(data: Dict) -> Optional[Dict]:
    try:
        channel = data.get("channel", "")
        if channel != "trades":
            return None
        
        trades = data.get("data", [])
        if not trades:
            return None
        
        for trade in trades:
            side = trade.get("side", "")
            if not side:
                continue
                
            return {
                "exchange": "hyperliquid",
                "symbol": "BTC/USDT",
                "timestamp": trade.get("time", int(time.time() * 1000)),
                "price": float(trade.get("px", 0)),
                "quantity": float(trade.get("sz", 0)),
                "side": "buy" if side == "B" else "sell",
                "is_buyer_maker": side != "B",
                "trade_id": trade.get("tid")
            }
        return None
    except Exception as e:
        logger.error(f"Error parsing Hyperliquid trade: {e}")
        return None


def _parse_hyperliquid_spot_trade(data: Dict) -> Optional[Dict]:
    try:
        channel = data.get("channel", "")
        if channel != "trades":
            return None
        
        trades = data.get("data", [])
        if not trades:
            return None
        
        for trade in trades:
            side = trade.get("side", "")
            if not side:
                continue
                
            return {
                "exchange": "hyperliquid_spot",
                "symbol": "BTC/USDC",
                "timestamp": trade.get("time", int(time.time() * 1000)),
                "price": float(trade.get("px", 0)),
                "quantity": float(trade.get("sz", 0)),
                "side": "buy" if side == "B" else "sell",
                "is_buyer_maker": side != "B",
                "trade_id": trade.get("tid")
            }
        return None
    except Exception as e:
        logger.error(f"Error parsing Hyperliquid spot trade: {e}")
        return None


def _normalize_symbol(symbol: str) -> str:
    symbol = symbol.upper().replace("USDT", "/USDT").replace("BTC", "/BTC")
    if "/" not in symbol:
        if symbol.endswith("BTC"):
            symbol = symbol.replace("BTC", "/BTC")
        elif symbol.endswith("USDT"):
            symbol = symbol.replace("USDT", "/USDT")
    return symbol


class ExchangeWebSocket:
    def __init__(self, name: str, url: str, subscriptions: List[str], parser: Callable):
        self.name = name
        self.url = url
        self.subscriptions = subscriptions
        self.parser = parser
        self.ws = None
        self.session = None
        self.running = False
        self.reconnect_delay = 5
    
    async def connect(self, on_trade: Callable[[Dict], None]):
        self.session = aiohttp.ClientSession()
        self.running = True
        self.reconnect_delay = 5
        self.max_reconnect_delay = 60
        self.max_consecutive_errors = 3
        self.consecutive_errors = 0
        
        while self.running:
            try:
                async with self.session.ws_connect(self.url, timeout=30) as ws:
                    self.ws = ws
                    logger.info(f"Connected to {self.name} WebSocket")
                    self.consecutive_errors = 0
                    self.reconnect_delay = 5
                    
                    await self._subscribe(ws)
                    
                    async for msg in ws:
                        if not self.running:
                            break
                        
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                                trade = self.parser(data)
                                if trade and self._is_btc_pair(trade["symbol"]):
                                    result = on_trade(trade)
                                    if asyncio.iscoroutine(result):
                                        await result
                            except json.JSONDecodeError:
                                pass
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            logger.error(f"{self.name} WebSocket error")
                            break
                        elif msg.type == aiohttp.WSMsgType.CLOSE:
                            logger.warning(f"{self.name} WebSocket closed")
                            break
                            
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.consecutive_errors += 1
                logger.error(f"{self.name} connection error ({self.consecutive_errors}/{self.max_consecutive_errors}): {e}")
            
            if self.running and self.consecutive_errors < self.max_consecutive_errors:
                logger.info(f"Reconnecting to {self.name} in {self.reconnect_delay}s...")
                await asyncio.sleep(self.reconnect_delay)
                self.reconnect_delay = min(self.max_reconnect_delay, self.reconnect_delay * 2)
            elif self.consecutive_errors >= self.max_consecutive_errors:
                logger.warning(f"Max consecutive errors reached for {self.name}, stopping reconnection attempts")
                break
        
        if self.session:
            await self.session.close()
    
    async def _subscribe(self, ws):
        if self.name == "binance":
            await ws.send_json({
                "method": "SUBSCRIBE",
                "params": self.subscriptions,
                "id": int(time.time())
            })
        elif self.name == "bybit":
            await ws.send_json({
                "op": "subscribe",
                "args": [f"trade:{s}" for s in self.subscriptions]
            })
        elif self.name == "bybit_perp":
            await ws.send_json({
                "op": "subscribe",
                "args": [f"trade.{s}" for s in self.subscriptions]
            })
        elif self.name == "binance_perp":
            await ws.send_json({
                "method": "SUBSCRIBE",
                "params": self.subscriptions,
                "id": int(time.time())
            })
        elif self.name == "coinbase":
            await ws.send_json({
                "type": "subscribe",
                "product_ids": self.subscriptions,
                "channels": ["matches"]
            })
        elif self.name == "okx":
            await ws.send_json({
                "op": "subscribe",
                "args": [{"channel": "trades", "instId": s} for s in self.subscriptions]
            })
        elif self.name == "hyperliquid":
            await ws.send_json({
                "method": "subscribe",
                "subscription": {"type": "trades", "coin": "BTC"}
            })
        elif self.name == "binance_perp":
            await ws.send_json({
                "method": "SUBSCRIBE",
                "params": self.subscriptions,
                "id": int(time.time())
            })
        elif self.name == "kraken":
            await ws.send_json({
                "event": "subscribe",
                "pair": self.subscriptions,
                "subscription": {"name": "trade"}
            })
        elif self.name == "bitstamp":
            await ws.send_json({
                "event": "subscribe",
                "channel": "live_trades_btcusd"
            })
        elif self.name == "bitget":
            await ws.send_json({
                "op": "subscribe",
                "args": [{"instType": "usdt-futures", "channel": "publicTrade", "instId": s} for s in self.subscriptions]
            })
        elif self.name == "bitfinex":
            await ws.send_json({
                "event": "subscribe",
                "channel": "trades",
                "pair": "BTCUSD"
            })
    
    def _is_btc_pair(self, symbol: str) -> bool:
        symbol = symbol.upper()
        symbol = symbol.replace("/", "").replace("-", "")
        
        return ("BTCUSDT" in symbol or 
                "BTCUSD" in symbol or
                symbol == "BTCUSDT" or
                symbol == "BTC")
    
    async def stop(self):
        self.running = False
        if self.ws:
            await self.ws.close()


class ExchangeCollector:
    def __init__(self, on_trade_callback: Callable[[Dict], None]):
        self.on_trade_callback = on_trade_callback
        self.trade_storage = TradeStorage()
        self.websockets = []
        self.running = False
        
    async def start(self):
        logger.info("Starting exchange collector...")
        self.running = True
        
        tasks = []
        
        binance_ws = ExchangeWebSocket(
            "binance",
            BINANCE_STREAM_URL,
            ["btcusdt@aggTrade"],
            _parse_binance_trade
        )
        tasks.append(asyncio.create_task(binance_ws.connect(self._on_trade)))
        self.websockets.append(binance_ws)
        
        coinbase_ws = ExchangeWebSocket(
            "coinbase",
            COINBASE_WS_URL,
            ["BTC-USD"],
            _parse_coinbase_trade
        )
        tasks.append(asyncio.create_task(coinbase_ws.connect(self._on_trade)))
        self.websockets.append(coinbase_ws)
        
        okx_ws = ExchangeWebSocket(
            "okx",
            OKX_WS,
            ["BTC-USDT"],
            _parse_okx_trade
        )
        tasks.append(asyncio.create_task(okx_ws.connect(self._on_trade)))
        self.websockets.append(okx_ws)
        
        hyperliquid_ws = ExchangeWebSocket(
            "hyperliquid",
            HYPERLIQUID_WS,
            ["BTC"],
            _parse_hyperliquid_trade
        )
        tasks.append(asyncio.create_task(hyperliquid_ws.connect(self._on_trade)))
        self.websockets.append(hyperliquid_ws)
        
        hyperliquid_spot_ws = ExchangeWebSocket(
            "hyperliquid_spot",
            HYPERLIQUID_WS,
            ["BTC"],
            _parse_hyperliquid_spot_trade
        )
        tasks.append(asyncio.create_task(hyperliquid_spot_ws.connect(self._on_trade)))
        self.websockets.append(hyperliquid_spot_ws)
        
        binance_perp_ws = ExchangeWebSocket(
            "binance_perp",
            BINANCE_FUTURES_WS,
            ["btcusdt@trade"],
            _parse_binance_perp_trade
        )
        tasks.append(asyncio.create_task(binance_perp_ws.connect(self._on_trade)))
        self.websockets.append(binance_perp_ws)
        
        logger.info(f"Starting {len(tasks)} exchange WebSocket connections...")
        
        await asyncio.gather(*tasks)
    
    async def _on_trade(self, trade: Dict):
        try:
            await self.trade_storage.insert_trade(trade)
            result = self.on_trade_callback(trade)
            if asyncio.iscoroutine(result):
                await result
        except Exception as e:
            logger.error(f"Error processing trade: {e}")
    
    async def stop(self):
        logger.info("Stopping exchange collector...")
        self.running = False
        
        for ws in self.websockets:
            await ws.stop()


async def run_collector(on_trade_callback: Callable[[Dict], None]):
    collector = ExchangeCollector(on_trade_callback)
    try:
        await collector.start()
    except KeyboardInterrupt:
        await collector.stop()
    except Exception as e:
        logger.error(f"Collector error: {e}")
        await collector.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    def test_callback(trade):
        print(f"Trade: {trade}")
    
    asyncio.run(run_collector(test_callback))
