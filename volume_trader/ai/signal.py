import os
import json
import logging
import time
from typing import Dict, List, Optional
from datetime import datetime

try:
    import google.genai as genai_module
    GENAI_AVAILABLE = True
    GENAI_V2 = True
except ImportError:
    try:
        import google.generativeai as genai_module
        GENAI_V2 = False
        GENAI_AVAILABLE = True
    except ImportError:
        GENAI_AVAILABLE = False
        GENAI_V2 = False
        genai_module = None

from volume_trader.storage.signals import SignalStorage
from volume_trader.storage.patterns import PatternStorage
from volume_trader import config

logger = logging.getLogger(__name__)

SIGNAL_PROMPT = """You are an expert crypto trader analyzing real-time volume and order flow data for BTC-pairs.

Given the following market data for {pair}:
- Volume: {volume} BTC (rolling avg: {rolling_avg} BTC)
- Volume spike: {volume_ratio}x normal
- Buy volume: {buy_volume} BTC ({buy_ratio}% of total)
- Sell volume: {sell_volume} BTC ({sell_ratio}% of total)
- Price momentum: {momentum}%
- Price change: {price_change}%
- Active exchanges: {exchanges}
- Recent large trades: {large_trades}

Based on this data:
1. Is this a TRADEABLE opportunity? (consider volume spike + direction + momentum)
2. Direction: LONG / SHORT / NEUTRAL
3. Confidence: 0-100%
4. What is the minimum "significant" trade size in BTC for this pair? (learn from market)
5. Brief reasoning (1-2 sentences)

Respond in JSON format:
{{
    "tradeable": true/false,
    "direction": "LONG"/"SHORT"/"NEUTRAL",
    "confidence": 0-100,
    "significant_size": 0.01-1.0,
    "reasoning": "brief explanation"
}}
"""

SUMMARY_PROMPT = """You are an expert crypto trader providing market summaries.

Given the following data for the last 60 minutes by exchange:
{pairs_data}

Compare with previous period if available: {comparison_note}

IMPORTANT RULES:
1. ALWAYS analyze BOTH BUY and SELL USD volumes - never ignore the sell side
2. If SELL volume > BUY volume at an exchange, note that as BEARISH pressure
3. If BUY volume > SELL volume, note that as BULLISH pressure
4. Calculate net flow: (BUY - SELL) to determine directional bias
5. Consider which exchanges have the strongest net buying/selling

Provide a brief AI insight:
1. Overall market bias (BULLISH/BEARISH/NEUTRAL)
2. Confidence level (0-100%)
3. Key observations - MENTION SPECIFIC BUY VS SELL NUMBERS (2-3 sentences)

Respond in JSON format:
{{
    "bias": "BULLISH"/"BEARISH"/"NEUTRAL",
    "confidence": 0-100,
    "observations": "brief summary mentioning specific buy/sell USD values"
}}
"""

WHY_NO_TRADE_PROMPT = """You are an expert crypto trader analyzing why a trade opportunity doesn't meet your standards.

You are conservative and only take high-probability trades. Given the current market data:
{current_data}

And compared to the previous period:
{comparison_data}

Analyze and respond in JSON format:
{{
    "meets_standards": true/false,
    "reasons": [
        "reason 1 why not trade",
        "reason 2 why not trade"
    ],
    "what_would_help": "what conditions would make this tradeable",
    "risk_assessment": "LOW/MEDIUM/HIGH",
    "recommendation": "WAIT/LOOK_FOR_ENTRY/CLOSE_POSITION"
}}
"""


class AISignalGenerator:
    def __init__(self):
        self.signal_storage = SignalStorage()
        self.pattern_storage = PatternStorage()
        self.model = None
        self.last_signal_time = {}
        
        try:
            if GENAI_V2:
                import google.genai as genai
                self.client = genai.Client(api_key=config.GOOGLE_API_KEY)
                self.model = self.client
            else:
                self.model = genai_module.GenerativeModel('gemini-2.0-flash')
            logger.info("AI model initialized")
        except Exception as e:
            logger.error(f"Failed to initialize AI model: {e}")
    
    async def generate_signal(self, market_data: Dict) -> Optional[Dict]:
        if not self.model:
            logger.warning("AI model not available")
            return None
        
        pair = market_data.get("pair", "UNKNOWN")
        pair_key = pair.lower().replace("/", "-")
        
        if pair in self.last_signal_time:
            elapsed = time.time() - self.last_signal_time[pair]
            if elapsed < config.SIGNAL_COOLDOWN_SECONDS:
                logger.debug(f"Signal cooldown active for {pair}")
                return None
        
        try:
            large_trades = []
            for trade in market_data.get("recent_trades", []):
                if trade.get("quantity", 0) >= config.MIN_TRADE_SIZE_BTC:
                    large_trades.append(f"{trade['quantity']:.4f} BTC @ {trade['price']}")
            
            large_trades_str = ", ".join(large_trades[:5]) if large_trades else "None"
            
            prompt = SIGNAL_PROMPT.format(
                pair=pair,
                volume=market_data.get("current_volume", 0),
                rolling_avg=market_data.get("rolling_avg", 0),
                volume_ratio=market_data.get("volume_ratio", 0),
                buy_volume=market_data.get("buy_volume", 0),
                buy_ratio=market_data.get("buy_ratio", 0),
                sell_volume=market_data.get("sell_volume", 0),
                sell_ratio=100 - market_data.get("buy_ratio", 50),
                momentum=market_data.get("momentum", 0),
                price_change=market_data.get("price_change", 0),
                exchanges=", ".join(market_data.get("exchanges", [])),
                large_trades=large_trades_str
            )
            
            response = self.client.models.generate_content(
                model='gemini-2.0-flash',
                contents=prompt
            )
            
            result_text = ""
            if GENAI_V2:
                if hasattr(response, 'text'):
                    result_text = response.text
                elif hasattr(response, 'candidates'):
                    result_text = response.candidates[0].content.parts[0].text
            else:
                result_text = response.text.strip()
            
            if "```json" in result_text:
                result_text = result_text.split("```json")[1].split("```")[0]
            elif "```" in result_text:
                result_text = result_text.split("```")[1].split("```")[0]
            
            signal = json.loads(result_text)
            
            signal_data = {
                "pair": pair,
                "timestamp": int(time.time() * 1000),
                "signal_type": "volume_spike",
                "direction": signal.get("direction", "NEUTRAL"),
                "confidence": signal.get("confidence", 50),
                "volume_ratio": market_data.get("volume_ratio", 0),
                "momentum": market_data.get("momentum", 0),
                "reason": signal.get("reasoning", ""),
                "price_at_signal": market_data.get("current_price", 0),
                "exchanges": market_data.get("exchanges", [])
            }
            
            signal_id = await self.signal_storage.insert_signal(signal_data)
            signal["signal_id"] = signal_id
            
            self.last_signal_time[pair] = time.time()
            
            logger.info(f"Generated signal for {pair}: {signal['direction']} @ {signal['confidence']}% confidence")
            
            return signal
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse AI response: {e}")
        except Exception as e:
            logger.error(f"Error generating signal: {e}")
        
        return None
    
    async def generate_summary(self, summary_data: Dict, comparison: Dict = None) -> Optional[Dict]:
        if not self.model:
            return None
        
        try:
            exchange_info = []
            total_buy = 0
            total_sell = 0
            
            for exchange, data in summary_data.items():
                buy_usd = data.get('buy_volume_usd', 0)
                sell_usd = data.get('sell_volume_usd', 0)
                total_buy += buy_usd
                total_sell += sell_usd
                
                exchange_info.append(
                    f"- {exchange}: ${buy_usd:,.0f} BUY vs ${sell_usd:,.0f} SELL "
                    f"(Buy: {data.get('buy_ratio', 50):.0f}%)"
                )
            
            exchange_str = "\n".join(exchange_info[:10])
            
            total_summary = f"TOTALS - BUY: ${total_buy:,.0f} | SELL: ${total_sell:,.0f}\n"
            
            comparison_note = "increasing" if comparison and comparison.get("trend") == "increasing" else "decreasing" if comparison and comparison.get("trend") == "decreasing" else "stable"
            comparison_note = f"Volume is {comparison_note} ({comparison.get('volume_change_pct', 0):+.1f}%) compared to previous period" if comparison else "No previous data available"
            
            prompt = SUMMARY_PROMPT.format(
                pairs_data=total_summary + exchange_str,
                comparison_note=comparison_note
            )
            
            response = self.client.models.generate_content(
                model='gemini-2.0-flash',
                contents=prompt
            )
            
            result_text = ""
            if GENAI_V2:
                result_text = response.text
            else:
                result_text = response.text.strip()
            
            if "```json" in result_text:
                result_text = result_text.split("```json")[1].split("```")[0]
            elif "```" in result_text:
                result_text = result_text.split("```")[1].split("```")[0]
            
            insight = json.loads(result_text)
            
            return insight
            
        except Exception as e:
            logger.error(f"Error generating summary: {e}")
        
        return None
    
    async def generate_why_no_trade(self, pairs_data: List[Dict], comparison: Dict) -> Optional[Dict]:
        if not self.model:
            return None
        
        try:
            current_info = "\n".join([
                f"- {p['pair']}: {p['volume']:.2f} USD, Buy {p['buy_ratio']:.0f}%, Price change {p.get('price_change', 0):.2f}%"
                for p in pairs_data[:5]
            ])
            
            comp_info = f"Volume changed by {comparison.get('volume_change_pct', 0):.1f}% ({comparison.get('trend', 'N/A')})"
            
            prompt = WHY_NO_TRADE_PROMPT.format(
                current_data=current_info,
                comparison_data=comp_info
            )
            
            response = self.client.models.generate_content(
                model='gemini-2.0-flash',
                contents=prompt
            )
            
            result_text = ""
            if GENAI_V2:
                result_text = response.text
            else:
                result_text = response.text.strip()
            
            if "```json" in result_text:
                result_text = result_text.split("```json")[1].split("```")[0]
            elif "```" in result_text:
                result_text = result_text.split("```")[1].split("```")[0]
            
            return json.loads(result_text)
            
        except Exception as e:
            logger.error(f"Error generating why no trade: {e}")
        
        return None
    
    async def update_signal_outcome(self, signal_id: int, price_data: Dict):
        if not price_data.get("price_5m"):
            return
        
        profit_loss = 0
        outcome = "neutral"
        
        if price_data.get("direction") == "LONG":
            profit_loss = ((price_data["price_5m"] - price_data["price_at_signal"]) 
                         / price_data["price_at_signal"] * 100)
        elif price_data.get("direction") == "SHORT":
            profit_loss = ((price_data["price_at_signal"] - price_data["price_5m"]) 
                         / price_data["price_at_signal"] * 100)
        
        if profit_loss > 0.1:
            outcome = "profit"
        elif profit_loss < -0.1:
            outcome = "loss"
        
        await self.signal_storage.update_outcome(signal_id, {
            "actual_outcome": outcome,
            "profit_loss": profit_loss,
            "price_1m": price_data.get("price_1m"),
            "price_3m": price_data.get("price_3m"),
            "price_5m": price_data.get("price_5m")
        })
    
    async def get_significant_size_threshold(self, pair: str) -> float:
        return await self.pattern_storage.get_significant_size_threshold(pair)


async def test_ai():
    generator = AISignalGenerator()
    
    test_data = {
        "pair": "ETH/BTC",
        "current_volume": 50,
        "rolling_avg": 15,
        "volume_ratio": 3.33,
        "buy_volume": 35,
        "sell_volume": 15,
        "buy_ratio": 70,
        "momentum": 2.5,
        "price_change": 3.2,
        "exchanges": ["binance", "bybit", "kucoin"],
        "recent_trades": [
            {"quantity": 0.5, "price": 0.035},
            {"quantity": 0.3, "price": 0.0351}
        ],
        "current_price": 0.035
    }
    
    result = await generator.generate_signal(test_data)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_ai())
