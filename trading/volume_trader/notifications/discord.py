import asyncio
import logging
import time
from typing import Dict, List, Optional
from datetime import datetime

import aiohttp

from volume_trader import config

logger = logging.getLogger(__name__)


class DiscordWebhook:
    def __init__(self, webhook_url: str = None):
        self.webhook_url = webhook_url or config.DISCORD_WEBHOOK_URL
        self.session = None
        self.last_send_time = 0
        self.min_interval = 0.2
        self.rate_limited = False
        self.rate_limit_until = 0
    
    async def _check_rate_limit(self):
        if self.rate_limited:
            if time.time() < self.rate_limit_until:
                return False
            self.rate_limited = False
        
        now = time.time()
        if now - self.last_send_time < self.min_interval:
            await asyncio.sleep(self.min_interval - (now - self.last_send_time))
        
        self.last_send_time = time.time()
        return True
    
    async def _ensure_session(self):
        if self.session is None:
            self.session = aiohttp.ClientSession()
    
    async def send(self, payload: Dict) -> bool:
        if not await self._check_rate_limit():
            return False
        
        await self._ensure_session()
        
        try:
            async with self.session.post(
                self.webhook_url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status == 204:
                    logger.debug("Discord webhook sent successfully")
                    return True
                elif response.status == 429:
                    self.rate_limited = True
                    self.rate_limit_until = time.time() + 5
                    logger.warning("Discord rate limited, backing off for 5 seconds")
                    return False
                else:
                    logger.error(f"Discord webhook failed: {response.status}")
                    return False
        except Exception as e:
            logger.error(f"Error sending Discord webhook: {e}")
            return False
    
    async def send_trade_signal(self, signal: Dict) -> bool:
        direction = signal.get("direction", "NEUTRAL")
        
        direction_emoji = {
            "LONG": "📈",
            "SHORT": "📉",
            "NEUTRAL": "➡️"
        }
        
        color = {
            "LONG": 0x00FF00,
            "SHORT": 0xFF0000,
            "NEUTRAL": 0xFFFF00
        }
        
        embed = {
            "title": f"🚨 TRADE SIGNAL - {signal.get('pair', 'UNKNOWN')}",
            "color": color.get(direction, 0xFFFF00),
            "fields": [
                {
                    "name": f"Direction",
                    "value": f"{direction_emoji.get(direction, '➡️')} {direction}",
                    "inline": True
                },
                {
                    "name": "Confidence",
                    "value": f"{signal.get('confidence', 50)}%",
                    "inline": True
                },
                {
                    "name": "Volume Spike",
                    "value": f"{signal.get('volume_ratio', 0):.1f}x normal",
                    "inline": True
                },
                {
                    "name": "Momentum",
                    "value": f"{signal.get('momentum', 0):.2f}%",
                    "inline": True
                },
                {
                    "name": "Buy Volume",
                    "value": f"{signal.get('buy_volume', 0):.4f} BTC",
                    "inline": True
                },
                {
                    "name": "Sell Volume",
                    "value": f"{signal.get('sell_volume', 0):.4f} BTC",
                    "inline": True
                },
                {
                    "name": "🔍 AI Reasoning",
                    "value": signal.get("reason", "Analyzing market conditions..."),
                    "inline": False
                }
            ],
            "footer": {
                "text": f"Exchanges: {', '.join(signal.get('exchanges', []))}"
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
        payload = {"embeds": [embed]}
        
        return await self.send(payload)
    
    async def send_summary(self, summary: Dict) -> bool:
        exchange_data = summary.get("pairs", [])
        
        exchange_str = ""
        for ex in exchange_data[:10]:
            buy_usd = ex.get('buy_volume_usd', 0)
            sell_usd = ex.get('sell_volume_usd', 0)
            exchange_str += (
                f"**{ex['exchange']}**: ${ex.get('volume_usd', 0):,.0f}\n"
                f"🟢 BUY: ${buy_usd:,.0f} | 🔴 SELL: ${sell_usd:,.0f} "
                f"({ex['buy_ratio']:.0f}% Buy)\n\n"
            )
        
        ai_insight = summary.get("ai_insight", {})
        ai_why_no_trade = summary.get("ai_why_no_trade", {})
        comparison = summary.get("comparison", {})
        
        comparison_str = f"{comparison.get('trend', 'N/A').upper()} ({comparison.get('volume_change_pct', 0):+.1f}%)"
        
        interval_mins = config.SUMMARY_INTERVAL_SECONDS // 60
        
        embed = {
            "title": f"📊 @everyone BTC MARKET SUMMARY - Last {interval_mins} Min",
            "color": 0x00AAFF,
            "fields": [
                {
                    "name": "📈 By Exchange",
                    "value": exchange_str or "No significant activity",
                    "inline": False
                },
                {
                    "name": "📊 Volume vs Previous",
                    "value": comparison_str,
                    "inline": True
                },
                {
                    "name": "💰 Total Volume",
                    "value": f"${summary.get('total_volume', 0):,.0f}",
                    "inline": True
                },
                {
                    "name": "🟢 Total BUY",
                    "value": f"${sum(e.get('buy_volume_usd', 0) for e in exchange_data):,.0f}",
                    "inline": True
                },
                {
                    "name": "🔴 Total SELL",
                    "value": f"${sum(e.get('sell_volume_usd', 0) for e in exchange_data):,.0f}",
                    "inline": True
                }
            ],
            "footer": {
                "text": f"Signals: {summary.get('total_signals', 0)} | Win rate: {summary.get('win_rate', 0):.0f}%"
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
        if ai_why_no_trade:
            risk = ai_why_no_trade.get("risk_assessment", "N/A")
            risk_color = {"LOW": 0x00FF00, "MEDIUM": 0xFFAA00, "HIGH": 0xFF0000}.get(risk, 0xFFFF00)
            embed["color"] = risk_color
            
            reasons = ai_why_no_trade.get("reasons", [])
            reasons_str = "\n".join([f"• {r}" for r in reasons[:3]]) if reasons else "No specific reasons"
            
            embed["fields"].append({
                "name": f"🎯 Why NO Trade (Risk: {risk})",
                "value": reasons_str[:500],
                "inline": False
            })
            
            embed["fields"].append({
                "name": "💡 What Would Help",
                "value": ai_why_no_trade.get("what_would_help", "N/A")[:300],
                "inline": False
            })
            
            embed["fields"].append({
                "name": "👤 Recommendation",
                "value": f"**{ai_why_no_trade.get('recommendation', 'WAIT')}/{ai_why_no_trade.get('risk_assessment', 'N/A')}**",
                "inline": True
            })
        
        if ai_insight.get("bias"):
            embed["fields"].append({
                "name": "🤖 AI Market Bias",
                "value": f"**{ai_insight.get('bias', 'NEUTRAL')}** ({ai_insight.get('confidence', 50)}%)\n{ai_insight.get('observations', '')}",
                "inline": False
            })
        
        payload = {
            "content": "@everyone",
            "embeds": [embed]
        }
        
        return await self.send(payload)
    
    async def send_test(self) -> bool:
        embed = {
            "title": "✅ Volume Trader Connected",
            "description": "Real-time BTC pairs volume monitoring is now active!",
            "color": 0x00FF00,
            "fields": [
                {
                    "name": "Status",
                    "value": "Monitoring for trade opportunities",
                    "inline": True
                },
                {
                    "name": "Volume Threshold",
                    "value": f"{config.VOLUME_SPIKE_THRESHOLD}x",
                    "inline": True
                },
                {
                    "name": "Exchanges",
                    "value": str(len(config.EXCHANGES_TO_MONITOR)),
                    "inline": True
                }
            ],
            "timestamp": datetime.utcnow().isoformat()
        }
        
        payload = {"embeds": [embed]}
        
        return await self.send(payload)
    
    async def close(self):
        if self.session:
            await self.session.close()
    
    async def send_large_trade_alert(self, trade: Dict, value_usd: float) -> bool:
        side = trade.get("side", "unknown")
        side_emoji = "🟢" if side == "buy" else "🔴"
        
        color = 0x00FF00 if side == "buy" else 0xFF0000
        
        embed = {
            "title": f"💰 LARGE TRADE ALERT - {trade.get('symbol', 'UNKNOWN')}",
            "description": f"Value: **${value_usd:,.0f} USD**",
            "color": color,
            "fields": [
                {
                    "name": "Side",
                    "value": f"{side_emoji} {side.upper()}",
                    "inline": True
                },
                {
                    "name": "Quantity",
                    "value": f"{trade.get('quantity', 0):.6f} BTC",
                    "inline": True
                },
                {
                    "name": "Price",
                    "value": f"{trade.get('price', 0):.8f} BTC",
                    "inline": True
                },
                {
                    "name": "Exchange",
                    "value": trade.get("exchange", "unknown").upper(),
                    "inline": True
                },
                {
                    "name": "Trade ID",
                    "value": str(trade.get("trade_id", "N/A"))[:20],
                    "inline": True
                }
            ],
            "timestamp": datetime.utcnow().isoformat()
        }
        
        payload = {"embeds": [embed]}
        
        return await self.send(payload)


async def test_webhook():
    webhook = DiscordWebhook()
    
    test_signal = {
        "pair": "ETH/BTC",
        "direction": "LONG",
        "confidence": 78,
        "volume_ratio": 4.2,
        "momentum": 2.5,
        "buy_volume": 15.5,
        "sell_volume": 8.2,
        "reason": "Large buy orders detected across Binance and Coinbase. Order book shows strong bid support.",
        "exchanges": ["Binance", "Coinbase", "KuCoin"]
    }
    
    print("Sending test signal...")
    result = await webhook.send_trade_signal(test_signal)
    print(f"Result: {result}")
    
    await webhook.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_webhook())
