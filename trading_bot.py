import os
import sys
import json
import asyncio
import requests
import datetime
import discord
import hashlib
import time
from discord.ext import commands
from io import BytesIO
from dotenv import load_dotenv
from google import genai
from google.genai import types

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TRADING_TOKEN")
GEMINI_KEY = os.getenv("GEMINI_KEY")
NEWS_CHANNEL_IDS = os.getenv("NEWS_CHANNEL_IDS", "").split(",") if os.getenv("NEWS_CHANNEL_IDS") else []

HISTORY_FILE = "trading_history.json"
CONTEXT_FILE = "trading_context.json"
NEWS_CACHE_FILE = "news_cache.json"
ACTIVE_TRADES_FILE = "active_trades.json"
ALERTS_FILE = "alerts_history.json"
CHART_USAGE_FILE = "chart_usage.json"
CHART_DAILY_LIMIT = 50

intents = discord.Intents.default()
intents.message_content = True
intents.messages = True
intents.guilds = True
bot = commands.Bot(command_prefix="!", intents=intents)

RECOMMENDATION_CHANNEL_ID = "1491522364188528810"

ALERT_CHANNEL_ID = RECOMMENDATION_CHANNEL_ID
ALERT_DM_USER_ID = ""

CHART_IMG_KEY = os.getenv("CHART_IMG_KEY")
CHART_CHANNEL_ID = os.getenv("CHART_CHANNEL_ID")
CHART_INTERVAL = 2160
CHART_SYMBOL = "BYBIT:BTCUSDT.P"

TRADING_PAIRS = {
    "BTC": ["BTC", "BTCUSDT", "BTC/USD", "XBT"],
    "ETH": ["ETH", "ETHUSDT", "ETH/USD"],
    "SOL": ["SOL", "SOLUSDT", "SOL/USD"],
    "BNB": ["BNB", "BNBUSDT"],
    "XRP": ["XRP", "XRPUSDT"],
    "ADA": ["ADA", "ADAUSDT"],
    "DOGE": ["DOGE", "DOGEUSDT"],
    "LINK": ["LINK", "LINKUSDT"],
    "AVAX": ["AVAX", "AVAXUSDT"],
    "MATIC": ["MATIC", "MATICUSDT"],
}


TRADING_CHANNEL_ID = "1491029807507705927"


def extract_message_content(message):
    content = ""
    
    if message.embeds:
        for embed in message.embeds:
            if hasattr(embed, 'description') and embed.description:
                content += " " + embed.description
            if hasattr(embed, 'title') and embed.title:
                content += " " + embed.title
            if hasattr(embed, 'fields') and embed.fields:
                for field in embed.fields:
                    content += f" {field.name} {field.value}"
    
    if message.content:
        content += " " + message.content
    
    return content.strip()


async def send_long_message(channel, content):
    embed = discord.Embed(
        description=content[:4096],
        color=discord.Color.blue(),
        timestamp=datetime.datetime.now()
    )
    
    if len(content) <= 4096:
        await channel.send(embed=embed)
        return
    
    lines = content.split('\n')
    chunks = []
    current_chunk = ""
    
    for line in lines:
        if len(current_chunk) + len(line) + 1 <= 4096:
            current_chunk += line + "\n"
        else:
            if current_chunk:
                chunks.append(current_chunk.strip())
            current_chunk = line + "\n"
    
    if current_chunk:
        chunks.append(current_chunk.strip())
    
    first = True
    for chunk in chunks:
        if first:
            await channel.send(embed=discord.Embed(
                description=chunk[:4096],
                color=discord.Color.blue(),
                timestamp=datetime.datetime.now()
            ))
            first = False
        else:
            await channel.send(embed=discord.Embed(
                description=f"...(continued)\n{chunk[:4096]}",
                color=discord.Color.blue(),
                timestamp=datetime.datetime.now()
            ))


def detect_sentiment(text):
    text_lower = text.lower()
    
    bullish_keywords = ["long", "bullish", "buy", "up", "support", "bounce", "higher", "breakout", "positive", "gain", "rise"]
    bearish_keywords = ["short", "bearish", "sell", "down", "resistance", "drop", "lower", "breakdown", "negative", "loss", "decline"]
    
    bullish_count = sum(1 for kw in bullish_keywords if kw in text_lower)
    bearish_count = sum(1 for kw in bearish_keywords if kw in text_lower)
    
    if bullish_count > bearish_count:
        return "bullish", 0x00FF00
    elif bearish_count > bullish_count:
        return "bearish", 0xFF0000
    else:
        return "neutral", 0xFFFF00


def parse_trading_fields(text):
    entry = None
    sl = None
    tp = None
    confidence = None
    
    import re
    
    entry_patterns = [
        r"entry[:\s]+(\$?[\d,.]+)",
        r"entry zone[:\s]+(\$?[\d,.]+)\s*[-–]\s*(\$?[\d,.]+)",
        r"enter[:\s]+(\$?[\d,.]+)",
    ]
    for pattern in entry_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            entry = match.group(0).split(":", 1)[1].strip() if ":" in match.group(0) else match.group(0)
            break
    
    sl_patterns = [
        r"stop\s*loss[:\s]+(\$?[\d,.]+)",
        r"sl[:\s]+(\$?[\d,.]+)",
        r"stoploss[:\s]+(\$?[\d,.]+)",
    ]
    for pattern in sl_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            sl = match.group(0).split(":", 1)[1].strip() if ":" in match.group(0) else match.group(0)
            break
    
    tp_patterns = [
        r"take\s*profit[:\s]+(\$?[\d,.]+)",
        r"tp[:\s]+(\$?[\d,.]+)",
        r"target[:\s]+(\$?[\d,.]+)",
    ]
    for pattern in tp_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            tp = match.group(0).split(":", 1)[1].strip() if ":" in match.group(0) else match.group(0)
            break
    
    conf_patterns = [
        r"confidence[:\s]+(\d+%)",
        r"(\d+)%\s+confidence",
    ]
    for pattern in conf_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            confidence = match.group(1) + "%" if "%" in match.group(0) else match.group(1) + "%"
            break
    
    return entry, sl, tp, confidence


async def send_analysis_embed(channel, content, original_message=""):
    sentiment, color = detect_sentiment(content)
    entry, sl, tp, confidence = parse_trading_fields(content)
    
    direction_emoji = "🟢" if sentiment == "bullish" else ("🔴" if sentiment == "bearish" else "⚪")
    direction_text = f"{direction_emoji} {sentiment.upper()}" if sentiment != "neutral" else "⚪ ANALYSIS"
    
    embed = discord.Embed(
        title=f"📊 {direction_text}",
        description=content[:3500],
        color=color,
        timestamp=datetime.datetime.now()
    )
    
    if original_message:
        embed.set_footer(text=f"Requested by: {original_message.author.display_name}")
    
    if entry:
        embed.add_field(name="📍 Entry Zone", value=entry, inline=True)
    if sl:
        embed.add_field(name="🛡️ Stop Loss", value=sl, inline=True)
    if tp:
        embed.add_field(name="🎯 Take Profit", value=tp, inline=True)
    if confidence:
        embed.add_field(name="📈 Confidence", value=confidence, inline=True)
    
    if len(content) > 3500:
        await channel.send(embed=embed)
        await channel.send(f"...(continued)\n{content[3500:]}")
    else:
        await channel.send(embed=embed)

SYSTEM_PROMPT = """You are a strict, no-nonsense crypto trading assistant. You specialize in DAY TRADING (1H-4H timeframe trades held for the session).

RULES:
1. Be direct and blunt - no fluff
2. Always consider: Entry point, Stop loss, Take profit, Risk/Reward ratio
3. Only trade setups with minimum 1:2 risk/reward
4. Be strict - if a trade doesn't meet criteria, say NO
5. Consider the user's day trading style
6. Be aware of key levels, liquidity, market structure, and indicators
7. Do not use JSON. Write your analysis in plain, conversational English.

DIRECTIONAL BIAS (IMPORTANT):
- Maintain a consistent LONG or SHORT bias until big news/events or price action invalidates it
- When chart/indicators/news counter your bias, explicitly note: "BIAS UNDER PRESSURE: [reason]"
- Only change bias with clear structure break: "**BIAS CHANGE: LONG → SHORT** [reason]"
- If NEUTRAL or no prior bias, start fresh - no inherited direction

Always analyze indicators: RSI, MACD, EMAs, volume, candle patterns.

Your analysis should include:
- Direction (Long/Short/Neutral)
- Entry zone
- Stop loss (exact price)
- Take profit levels (with priority)
- Risk/Reward ratio
- Confidence level (High/Medium/Low)
- Current bias status (e.g., "LONG - strong" or "LONG under pressure (RSI 78, price rejecting at resistance)")
- Timeframe alignment

IMPORTANT: News from monitored channels is provided ABOVE in the prompt. Use that information to inform your analysis. Do NOT say you don't have access to news - the news is already provided to you.
"""


CONVERSATION_PROMPT = """You are a knowledgeable crypto friend having a casual conversation.

STYLE:
- Be conversational, friendly, and direct
- Keep answers concise but informative
- Explain things simply without over-complicating
- When discussing trades/charts, be specific with numbers
- You can suggest trades but don't force it - be helpful, not pushy
- Use casual language, like talking to a friend

LEARNING & MEMORY:
- Remember details from this conversation for continuity
- Reference previous points when relevant to build on the discussion
- If user shares a chart/image, analyze it and remember what you discussed
- Build on earlier discussions rather than starting fresh each time

This is a chat - write naturally, not like a formal report.
"""


def load_history():
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r") as f:
            return json.load(f)
    return []


def save_history(history):
    with open(HISTORY_FILE, "w") as f:
        json.dump(history, f, indent=2)


def load_context():
    if os.path.exists(CONTEXT_FILE):
        with open(CONTEXT_FILE, "r") as f:
            return json.load(f)
    return {
        "trader_type": "daytrade",
        "bias": "NEUTRAL",
        "bias_confidence": "none",
        "notes": "",
        "trades": []
    }


def save_context(context):
    with open(CONTEXT_FILE, "w") as f:
        json.dump(context, f, indent=2)


def load_bias():
    context = load_context()
    return context.get("bias", "NEUTRAL"), context.get("bias_confidence", "none")


def save_bias(direction, confidence="medium"):
    context = load_context()
    context["bias"] = direction.upper()
    context["bias_confidence"] = confidence
    save_context(context)


def get_bias_context():
    bias, confidence = load_bias()
    if bias == "NEUTRAL":
        return "Current directional bias: NEUTRAL (no active bias)"
    return f"Current directional bias: {bias} ({confidence} confidence)"


def load_active_trades():
    if os.path.exists(ACTIVE_TRADES_FILE):
        try:
            with open(ACTIVE_TRADES_FILE, "r") as f:
                return json.load(f)
        except:
            return {}
    return {}


def save_active_trades(trades):
    with open(ACTIVE_TRADES_FILE, "w") as f:
        json.dump(trades, f, indent=2)


def load_alerts():
    if os.path.exists(ALERTS_FILE):
        try:
            with open(ALERTS_FILE, "r") as f:
                return json.load(f)
        except:
            return []
    return []


def save_alerts(alerts):
    with open(ALERTS_FILE, "w") as f:
        json.dump(alerts, f, indent=2)


def load_chart_usage():
    if os.path.exists(CHART_USAGE_FILE):
        try:
            with open(CHART_USAGE_FILE, "r") as f:
                return json.load(f)
        except:
            return {"date": "", "count": 0}
    return {"date": "", "count": 0}


def save_chart_usage(data):
    with open(CHART_USAGE_FILE, "w") as f:
        json.dump(data, f, indent=2)


def get_chart_usage():
    usage = load_chart_usage()
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    
    if usage.get("date") != today:
        return 0
    
    return usage.get("count", 0)


def increment_chart_usage():
    usage = load_chart_usage()
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    
    if usage.get("date") != today:
        usage = {"date": today, "count": 0}
    
    usage["count"] += 1
    save_chart_usage(usage)
    return usage["count"]


ALERT_COOLDOWN_FILE = "alert_cooldown.json"
ALERT_COOLDOWN_SECONDS = 60

def load_cooldown():
    if os.path.exists(ALERT_COOLDOWN_FILE):
        try:
            with open(ALERT_COOLDOWN_FILE, "r") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_cooldown(data):
    with open(ALERT_COOLDOWN_FILE, "w") as f:
        json.dump(data, f, indent=2)

def is_alert_on_cooldown(pair):
    cooldown_data = load_cooldown()
    current_time = asyncio.get_event_loop().time()
    
    if pair in cooldown_data:
        last_alert_time = cooldown_data[pair]
        if current_time - last_alert_time < ALERT_COOLDOWN_SECONDS:
            return True
    
    return False

def update_cooldown(pair):
    cooldown_data = load_cooldown()
    cooldown_data[pair] = asyncio.get_event_loop().time()
    save_cooldown(cooldown_data)


ANALYZED_EVENTS_FILE = "analyzed_events.json"
EVENT_RETENTION_HOURS = 4


def hash_event(text):
    if not text:
        return ""
    return hashlib.sha256(text[:200].encode()).hexdigest()


def load_analyzed_events():
    if os.path.exists(ANALYZED_EVENTS_FILE):
        try:
            with open(ANALYZED_EVENTS_FILE, "r") as f:
                return json.load(f)
        except:
            return []
    return []


def save_analyzed_events(events):
    with open(ANALYZED_EVENTS_FILE, "w") as f:
        json.dump(events, f, indent=2)


def is_event_analyzed(event_text):
    if not event_text:
        return True
    event_hash = hash_event(event_text)
    cutoff_time = time.time() - (EVENT_RETENTION_HOURS * 3600)
    events = load_analyzed_events()
    for event in events:
        if event.get("hash") == event_hash and event.get("timestamp", 0) > cutoff_time:
            return True
    return False


def mark_event_analyzed(event_text, direction=""):
    if not event_text:
        return
    events = load_analyzed_events()
    cutoff_time = time.time() - (EVENT_RETENTION_HOURS * 3600)
    events = [e for e in events if e.get("timestamp", 0) > cutoff_time]
    events.append({
        "hash": hash_event(event_text),
        "timestamp": time.time(),
        "direction": direction
    })
    save_analyzed_events(events)


def add_alert(alert_type, message, pair=None, severity="medium"):
    alerts = load_alerts()
    alert = {
        "type": alert_type,
        "message": message,
        "pair": pair,
        "severity": severity,
        "timestamp": str(asyncio.get_event_loop().time())
    }
    alerts.append(alert)
    alerts = alerts[-100:]
    save_alerts(alerts)
    return alert


def detect_pair_from_message(message):
    message_upper = message.upper()
    for pair_name, aliases in TRADING_PAIRS.items():
        for alias in aliases:
            if alias.upper() in message_upper:
                return pair_name
    return None


def format_history_for_gemini():
    history = load_history()
    context = load_context()
    
    formatted = "=== TRADING HISTORY ===\n\n"
    
    for i, entry in enumerate(history[-20:], 1):
        role = entry.get("role", "user")
        content = entry.get("content", "")
        timestamp = entry.get("timestamp", "")
        
        if role == "image":
            formatted += f"[{timestamp}] User sent a CHART SCREENSHOT\n"
        else:
            formatted += f"[{timestamp}] {role.upper()}: {content[:500]}\n"
    
    if context.get("trades"):
        formatted += "\n=== RECENT TRADES ===\n"
        for trade in context["trades"][-10:]:
            formatted += f"- {trade}\n"
    
    return formatted


async def get_news_context(num_messages=3):
    if not NEWS_CHANNEL_IDS:
        return ""
    
    formatted = "\n" + "="*60 + "\n"
    formatted += "📰 LATEST NEWS FROM EACH CHANNEL:\n"
    formatted += "="*60 + "\n"
    
    for channel_id in NEWS_CHANNEL_IDS:
        channel_id = channel_id.strip()
        if not channel_id:
            continue
        
        try:
            channel = bot.get_channel(int(channel_id))
            if channel:
                formatted += f"\n--- {channel.name} ---\n"
                
                count = 0
                async for msg in channel.history(limit=num_messages):
                    content = ""
                    
                    if msg.embeds:
                        for embed in msg.embeds:
                            if hasattr(embed, 'description') and embed.description:
                                content = embed.description
                                break
                            elif hasattr(embed, 'title') and embed.title and 'BULLISH' not in embed.title and 'BEARISH' not in embed.title:
                                content = embed.title
                    
                    if not content and msg.content and msg.content.strip():
                        content = msg.content.strip()
                    
                    if content and len(content) > 20:
                        count += 1
                        formatted += f"{count}. [{str(msg.created_at)[:19]}] {content[:150]}...\n"
                
                if count == 0:
                    formatted += "No recent news\n"
                    
        except Exception as e:
            print(f"Error fetching channel {channel_id}: {e}")
    
    return formatted


async def get_news_summary():
    if not NEWS_CHANNEL_IDS:
        return ""
    
    news_items = []
    
    for channel_id in NEWS_CHANNEL_IDS:
        channel_id = channel_id.strip()
        if not channel_id:
            continue
        
        try:
            channel = bot.get_channel(int(channel_id))
            if channel:
                async for msg in channel.history(limit=10):
                    content = ""
                    
                    if msg.embeds:
                        for embed in msg.embeds:
                            if hasattr(embed, 'description') and embed.description:
                                content = embed.description
                                break
                            elif hasattr(embed, 'title') and embed.title and 'BULLISH' not in embed.title and 'BEARISH' not in embed.title:
                                content = embed.title
                    
                    if not content and msg.content and msg.content.strip():
                        content = msg.content.strip()
                    
                    if content and len(content) > 20:
                        news_items.append({
                            "channel": channel.name,
                            "content": content[:200],
                            "time": str(msg.created_at)[:19]
                        })
        except Exception as e:
            print(f"Error fetching channel {channel_id}: {e}")
    
    if not news_items:
        return ""
    
    summary = "\n\n" + "="*50 + "\n"
    summary += "📰 **LATEST NEWS FROM CHANNELS:**\n"
    summary += "="*50 + "\n"
    for i, item in enumerate(news_items[:3], 1):
        summary += f"\n{i}. **[{item['channel']}]** {item['content']}\n"
    
    return summary


def save_news_cache(news_items):
    with open(NEWS_CACHE_FILE, "w") as f:
        json.dump(news_items, f, indent=2)


def load_news_cache():
    if os.path.exists(NEWS_CACHE_FILE):
        try:
            with open(NEWS_CACHE_FILE, "r") as f:
                return json.load(f)
        except:
            return []
    return []


async def update_news_cache():
    all_news = []
    
    for channel_id in NEWS_CHANNEL_IDS:
        channel_id = channel_id.strip()
        if not channel_id:
            continue
        
        try:
            channel = bot.get_channel(int(channel_id))
            if channel:
                async for msg in channel.history(limit=5):
                    content = ""
                    
                    if msg.embeds:
                        for embed in msg.embeds:
                            if hasattr(embed, 'description') and embed.description:
                                content = embed.description
                                break
                            elif hasattr(embed, 'title') and embed.title and 'BULLISH' not in embed.title and 'BEARISH' not in embed.title:
                                content = embed.title
                    
                    if not content and msg.content and msg.content.strip():
                        content = msg.content.strip()
                    
                    if content and len(content) > 20:
                        all_news.append({
                            "channel": channel.name,
                            "content": content[:300],
                            "time": str(msg.created_at)[:19],
                            "msg_id": msg.id
                        })
        except:
            pass
    
    all_news.sort(key=lambda x: x['time'], reverse=True)
    all_news = all_news[:30]
    save_news_cache(all_news)
    print(f"News cache updated: {len(all_news)} items")
    return all_news


async def cleanup_old_news():
    news = load_news_cache()
    if len(news) > 50:
        news = news[:50]
        save_news_cache(news)
        print(f"News cache cleaned: kept {len(news)} items")
    return news


async def background_news_updater():
    update_count = 0
    while True:
        try:
            await asyncio.sleep(600)
            
            await update_news_cache()
            update_count += 1
            
            if update_count >= 72:
                await cleanup_old_news()
                update_count = 0
                print("12-hour cleanup completed")
                
        except Exception as e:
            print(f"Background update error: {e}")
            await asyncio.sleep(60)


def fetch_chart_image():
    print(f"[Chart] fetch_chart_image called - KEY: {'set' if CHART_IMG_KEY else 'NOT SET'}")
    
    if not CHART_IMG_KEY:
        print("[Chart] CHART_IMG_KEY not set in .env")
        return None
    
    current_usage = get_chart_usage()
    print(f"[Chart] Current usage: {current_usage}/{CHART_DAILY_LIMIT}")
    
    if current_usage >= CHART_DAILY_LIMIT:
        print(f"[Chart] Daily limit reached ({CHART_DAILY_LIMIT}/{CHART_DAILY_LIMIT})")
        return None
    
    url = "https://api.chart-img.com/v1/tradingview/advanced-chart"
    params = {
        "key": CHART_IMG_KEY,
        "symbol": CHART_SYMBOL,
        "interval": "15m",
        "width": "800",
        "height": "600"
    }
    
    print(f"[Chart] Calling API: symbol={CHART_SYMBOL}, interval=15m")
    
    try:
        response = requests.get(url, params=params, timeout=30)
        print(f"[Chart] API Response: status={response.status_code}")
        
        if response.status_code == 200:
            increment_chart_usage()
            new_count = get_chart_usage()
            print(f"[Chart] Chart fetched successfully ({new_count}/{CHART_DAILY_LIMIT} today), size={len(response.content)} bytes")
            return response.content
        else:
            print(f"[Chart] API error: {response.status_code} - {response.text[:200]}")
            return None
    except Exception as e:
        print(f"[Chart] Fetch exception: {e}")
        return None


async def background_chart_poster():
    await asyncio.sleep(10)
    while True:
        try:
            print(f"[Chart] Fetching chart... (Interval: {CHART_INTERVAL}s)")
            
            image_bytes = await asyncio.to_thread(fetch_chart_image)
            
            print(f"[Chart] Fetch result: {'Success' if image_bytes else 'Failed/None'}")
            
            if not image_bytes:
                current_usage = get_chart_usage()
                if current_usage >= CHART_DAILY_LIMIT:
                    print(f"[Chart] Daily limit reached ({CHART_DAILY_LIMIT}/{CHART_DAILY_LIMIT}) - skipping post")
                else:
                    print(f"[Chart] Fetch failed - CHART_CHANNEL_ID: {CHART_CHANNEL_ID}")
                await asyncio.sleep(CHART_INTERVAL)
                continue
            
            if CHART_CHANNEL_ID:
                channel = bot.get_channel(int(CHART_CHANNEL_ID))
                if channel:
                    current_usage = get_chart_usage()
                    remaining = CHART_DAILY_LIMIT - current_usage
                    await channel.send(
                        f"📊 **BTC/USDT 15M Chart** - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}\nRemaining today: {remaining}/{CHART_DAILY_LIMIT}",
                        file=discord.File(BytesIO(image_bytes), filename="btc_chart.png")
                    )
                    print(f"[Chart] Posted to channel {CHART_CHANNEL_ID} ({current_usage}/{CHART_DAILY_LIMIT} today)")
                else:
                    print(f"[Chart] Channel not found: {CHART_CHANNEL_ID}")
            else:
                print("[Chart] CHART_CHANNEL_ID not set")
                
        except Exception as e:
            print(f"[Chart] Background chart poster error: {e}")
        
        await asyncio.sleep(CHART_INTERVAL)


def fetch_hyperliquid_volume():
    url = "https://api.hyperliquid.xyz/info"
    payload = {"type": "metaAndAssetCtxs"}
    
    try:
        response = requests.post(url, json=payload, timeout=15)
        if response.status_code == 200:
            data = response.json()
            for asset in data.get("assetCtxs", []):
                if asset.get("coin") == "BTC":
                    return {
                        "coin": "BTC",
                        "dayNtlVlm": float(asset.get("dayNtlVlm", 0)),
                        "dayBaseVlm": float(asset.get("dayBaseVlm", 0)),
                        "openInterest": float(asset.get("openInterest", 0)),
                        "markPx": float(asset.get("markPx", 0)),
                        "prevDayPx": float(asset.get("prevDayPx", 0)),
                        "funding": float(asset.get("funding", 0)),
                        "oraclePx": float(asset.get("oraclePx", 0))
                    }
        print(f"[Hyperliquid] API error: {response.status_code}")
        return None
    except Exception as e:
        print(f"[Hyperliquid] Fetch error: {e}")
        return None


async def get_latest_news_for_ai():
    news = load_news_cache()
    
    if not news:
        news = await update_news_cache()
    
    formatted = "\n" + "="*60 + "\n"
    formatted += "📰 LIVE NEWS CONTEXT:\n"
    formatted += "="*60 + "\n"
    
    for i, item in enumerate(news[:10], 1):
        formatted += f"{i}. [{item['channel']}] {item['content'][:150]}...\n"
    
    return formatted


async def analyze_with_gemini(user_message, attachments=None, include_news_summary=False, image_bytes=None):
    if not GEMINI_KEY:
        return "Error: GEMINI_KEY not set in .env"
    
    try:
        await update_news_cache()
        
        client = genai.Client(api_key=GEMINI_KEY)
        
        history_context = format_history_for_gemini()
        news_context = await get_latest_news_for_ai()
        
        print(f"News context loaded: {len(news_context)} chars")
        
        bias_context = get_bias_context()
        
        prompt = f"""{SYSTEM_PROMPT}

{history_context}

{news_context}

{bias_context}

Based on the news above and your trading expertise, answer the following question about this chart/trade:

{user_message}

Remember: You MUST use the news provided above in your analysis."""

        contents = [prompt]
        
        if attachments:
            for attachment in attachments:
                if attachment.content_type and attachment.content_type.startswith('image/'):
                    img_bytes = await attachment.read()
                    img_part = types.Part.from_bytes(data=img_bytes, mime_type="image/jpeg")
                    contents.append(img_part)
        elif image_bytes:
            img_part = types.Part.from_bytes(data=image_bytes, mime_type="image/png")
            contents.append(img_part)
        
        response = client.models.generate_content(
            model='gemini-2.0-flash',
            contents=contents
        )
        
        result = response.text.strip()
        
        if include_news_summary:
            news_summary = await get_news_summary()
            result += news_summary
        
        import re
        bias_change_match = re.search(r'\*\*BIAS CHANGE:\s*(LONG|SHORT|NEUTRAL)\s*→\s*(LONG|SHORT|NEUTRAL)\s*\*\*', result, re.IGNORECASE)
        if bias_change_match:
            old_dir = bias_change_match.group(1).upper()
            new_dir = bias_change_match.group(2).upper()
            confidence = "high" if "strong" in result.lower() or "clear structure" in result.lower() else "medium"
            save_bias(new_dir, confidence)
            print(f"[Bias] Changed: {old_dir} → {new_dir}")
        
        return result
        
    except Exception as e:
        return f"Error: {e}"


async def converse_with_gemini(user_message, attachments=None):
    """For casual conversation in trading-chat - pure chat unless context requested"""
    if not GEMINI_KEY:
        return "Error: GEMINI_KEY not set in .env"
    
    try:
        client = genai.Client(api_key=GEMINI_KEY)
        
        history_context = format_history_for_gemini()
        bias_context = get_bias_context()
        
        prompt = f"""{CONVERSATION_PROMPT}

{bias_context}

{history_context}

User: {user_message}"""
        
        contents = [prompt]
        
        if attachments:
            for attachment in attachments:
                if attachment.content_type and attachment.content_type.startswith('image/'):
                    img_bytes = await attachment.read()
                    img_part = types.Part.from_bytes(data=img_bytes, mime_type="image/jpeg")
                    contents.append(img_part)
        
        response = client.models.generate_content(
            model='gemini-2.0-flash',
            contents=contents
        )
        
        result = response.text.strip()
        
        import re
        bias_change_match = re.search(r'\*\*BIAS CHANGE:\s*(LONG|SHORT|NEUTRAL)\s*→\s*(LONG|SHORT|NEUTRAL)\s*\*\*', result, re.IGNORECASE)
        if bias_change_match:
            old_dir = bias_change_match.group(1).upper()
            new_dir = bias_change_match.group(2).upper()
            confidence = "high" if "strong" in result.lower() or "clear structure" in result.lower() else "medium"
            save_bias(new_dir, confidence)
            print(f"[Bias] Changed: {old_dir} → {new_dir}")
        
        return result
        
    except Exception as e:
        return f"Oops, something went wrong: {e}"


async def analyze_message_for_opportunity(message_content, channel_name):
    if not GEMINI_KEY:
        return None
    
    active_trades = load_active_trades()
    has_active_trades = bool(active_trades)
    
    try:
        client = genai.Client(api_key=GEMINI_KEY)
        
        if has_active_trades:
            pair_info = []
            for pair, info in active_trades.items():
                position = info.get("position", "unknown")
                pair_info.append(f"{pair} ({position})")
            
            prompt = f"""Analyze this Discord message for trading implications related to: {', '.join(pair_info)}

Message: {message_content[:500]}
Channel: {channel_name}

CRITICAL: The user has ACTIVE positions in these pairs. Only respond to MAJOR events that will move price SIGNIFICANTLY:
- Geopolitical: WAR (IRAN/ISRAEL), oil/gas crises, major conflicts
- Economic: Fed decision, CPI, GDP, Jobs report
- Regulatory: ETF approval/rejection, bans, landmark rulings
- Black swan: major hacks, exchange collapse, huge partnership
- Strong breakout/breakdown with volume confirmation

For LONG positions: threats are price going DOWN significantly
For SHORT positions: threats are price going UP significantly

Respond ONLY if truly MAJOR. If unsure or message is minor, respond "neutral".

Respond in JSON format:
{{
    "is_actionable": true/false,
    "type": "opportunity"/"threat"/"neutral",
    "severity": "critical"/"high"/"medium"/"low",
    "pairs_affected": ["BTC", "ETH"],
    "summary": "brief explanation",
    "recommendation": "action to take for their specific long/short position"
}}
"""
        else:
            prompt = f"""Analyze this Discord message for MAJOR trading opportunities.

Message: {message_content[:500]}
Channel: {channel_name}

The user has NO active trades. Only respond to TRULY MAJOR events:
- Geopolitical: WAR (IRAN/ISRAEL), oil/gas crises, major conflicts
- Economic: Fed decision, CPI, GDP, Jobs report
- Regulatory: ETF approval/rejection, bans, landmark rulings
- Black swan: major hacks, exchange collapse, huge partnership
- Strong breakout/breakdown with volume confirmation

Respond "neutral" if:
- Just opinions or predictions without basis
- Casual chat, memes, jokes
- Minor price action without news
- Any unsure or minor content

Respond in JSON format:
{{
    "is_actionable": true/false,
    "type": "opportunity"/"threat"/"neutral",
    "severity": "critical"/"high"/"medium"/"low",
    "pairs_affected": ["BTC", "ETH"],
    "summary": "brief explanation",
    "recommendation": "action to take - enter trade or wait"
}}
"""
        
        response = client.models.generate_content(
            model='gemini-2.0-flash',
            contents=[prompt]
        )
        
        result_text = response.text.strip()
        if "```json" in result_text:
            result_text = result_text.split("```json")[1].split("```")[0]
        
        return json.loads(result_text)
        
    except Exception as e:
        print(f"Error analyzing message: {e}")
        return None


async def send_alert(direction, pair, alert_type, message, severity="medium"):
    mark_event_analyzed(message, direction)
    
    alert = add_alert(alert_type, message, [pair] if pair else None, severity)
    
    ping_msg = f"@everyone {direction} {pair}" if pair else f"@everyone {direction}"
    
    color_map = {
        "LONG": 0x00FF00,
        "SHORT": 0xFF0000,
        "NEUTRAL": 0xFFFF00
    }
    
    if ALERT_CHANNEL_ID:
        try:
            channel = bot.get_channel(int(ALERT_CHANNEL_ID))
            if channel:
                await channel.send(ping_msg)
                embed = discord.Embed(
                    description=message[:4000],
                    color=color_map.get(direction.upper(), 0xFFFF00),
                    timestamp=datetime.datetime.now()
                )
                if alert_type:
                    embed.set_footer(text=f"Source: {alert_type}")
                await channel.send(embed=embed)
        except Exception as e:
            print(f"Error sending to alert channel: {e}")
    
    return alert


@bot.event
async def on_ready():
    print(f"Trading Bot logged in as {bot.user}")
    print(f"Monitoring {len(NEWS_CHANNEL_IDS)} news channel(s)")
    print("Commands: !help, !analyze, !trade, !history, !addtrade, !trades, !notes, !reset")
    
    print("\nVerifying news channels...")
    for channel_id in NEWS_CHANNEL_IDS:
        channel_id = channel_id.strip()
        if channel_id:
            try:
                channel = bot.get_channel(int(channel_id))
                if channel:
                    print(f"  ✓ {channel.name} (ID: {channel_id})")
                else:
                    print(f"  ✗ Channel not found: {channel_id}")
            except Exception as e:
                print(f"  ✗ Error accessing {channel_id}: {e}")
    
    print("\nUpdating news cache...")
    await update_news_cache()
    print("News cache updated!")
    
    asyncio.create_task(background_news_updater())
    print("Background news updater started (updates every 10 mins, cleanup every 12 hrs)")
    
    if CHART_IMG_KEY and CHART_CHANNEL_ID:
        asyncio.create_task(background_chart_poster())
        print(f"Background chart poster started (every {CHART_INTERVAL} seconds)")
    else:
        print("Chart posting disabled - CHART_IMG_KEY or CHART_CHANNEL_ID not set")


@bot.event
async def on_message(message):
    if message.author == bot.user or message.author.id == bot.user.id:
        return
    
    if message.channel.id == int(RECOMMENDATION_CHANNEL_ID):
        return
    
    full_content = extract_message_content(message)
    
    if not full_content:
        return
    
    if full_content.startswith('!'):
        await bot.process_commands(message)
        return
    
    if message.guild is None:
        return
    
    is_news_channel = message.channel.id in [int(cid.strip()) for cid in NEWS_CHANNEL_IDS if cid.strip()]
    print(f"[DEBUG] Message from: {message.channel.name} (ID: {message.channel.id}), is_news_channel: {is_news_channel}")
    
    active_trades = load_active_trades()
    print(f"[DEBUG] Active trades: {active_trades}")
    
    if active_trades:
        try:
            print(f"Analyzing message from {message.channel.name}: {full_content[:100]}...")
            
            analysis = await analyze_message_for_opportunity(full_content, message.channel.name)
            
            print(f"Analysis result: {analysis}")
            
            if analysis and analysis.get("is_actionable"):
                pairs_affected = analysis.get("pairs_affected", [])
                alert_type = analysis.get("type", "neutral")
                severity = analysis.get("severity", "low")
                summary = analysis.get("summary", "")
                recommendation = analysis.get("recommendation", "")
                
                if alert_type != "neutral" and severity in ["critical", "high"]:
                    matching_pairs = [p for p in pairs_affected if p in active_trades]
                    
                    if matching_pairs:
                        if is_event_analyzed(full_content):
                            print(f"Event already analyzed, skipping")
                        else:
                            direction = "LONG" if alert_type == "opportunity" else "SHORT"
                            
                            for pair in matching_pairs:
                                if is_alert_on_cooldown(pair):
                                    print(f"Alert for {pair} on cooldown, skipping")
                                    continue
                                update_cooldown(pair)
                            
                            severity_level = "high" if alert_type == "threat" or severity == "critical" else "medium"
                            alert_msg = f"{summary}\n\n💡 *Recommendation:* {recommendation}"
                            
                            await send_alert(direction, matching_pairs[0], f"{alert_type.upper()} - {message.channel.name}", alert_msg, severity_level)
                            print(f"Alert sent for {matching_pairs}: {alert_type}")
            
        except Exception as e:
            print(f"Error in active trade monitoring: {e}")
        
        if is_news_channel:
            return
    
    if is_news_channel:
        try:
            print(f"[DEBUG] Analyzing news channel message: {full_content[:100]}...")
            analysis = await analyze_message_for_opportunity(full_content, message.channel.name)
            print(f"[DEBUG] News channel analysis result: {analysis}")
            
            if analysis and analysis.get("is_actionable"):
                alert_type = analysis.get("type", "neutral")
                severity = analysis.get("severity", "low")
                
                print(f"[DEBUG] Alert type: {alert_type}, Severity: {severity}")
                
                if alert_type != "neutral" and severity in ["critical", "high"]:
                    if is_event_analyzed(full_content):
                        print(f"Event already analyzed, skipping")
                        return
                    
                    summary = analysis.get("summary", "")
                    recommendation = analysis.get("recommendation", "")
                    pairs_affected = analysis.get("pairs_affected", [])
                    
                    direction = "LONG" if alert_type == "opportunity" else "SHORT"
                    pair = pairs_affected[0] if pairs_affected else "CRYPTO"
                    
                    severity_level = "high" if severity == "critical" else "medium"
                    alert_msg = f"📰 **From {message.channel.name}:**\n{summary}\n\n💡 *Recommendation:* {recommendation}"
                    
                    await send_alert(direction, pair, f"OPPORTUNITY - {message.channel.name}", alert_msg, severity_level)
                    print(f"News channel opportunity alert sent: {alert_type} - {severity}")
                    
        except Exception as e:
            print(f"Error analyzing news channel message: {e}")
        
        return
    
    is_trading_channel = message.channel.id == int(TRADING_CHANNEL_ID)
    
    should_respond = False
    
    if is_trading_channel:
        should_respond = True
    else:
        try:
            analysis = await analyze_message_for_opportunity(full_content, message.channel.name)
            
            if analysis and analysis.get("is_actionable"):
                alert_type = analysis.get("type", "neutral")
                severity = analysis.get("severity", "low")
                
                if alert_type != "neutral" and severity in ["critical", "high"]:
                    should_respond = True
                else:
                    should_respond = True
        
        except Exception as e:
            print(f"Error analyzing for response: {e}")
            should_respond = True
    
    if should_respond:
        try:
            async with message.channel.typing():
                if is_trading_channel:
                    result = await converse_with_gemini(full_content, message.attachments)
                    await message.channel.send(result)
                else:
                    result = await analyze_with_gemini(full_content, message.attachments, include_news_summary=False)
                    trading_channel = bot.get_channel(int(TRADING_CHANNEL_ID))
                    if trading_channel:
                        sentiment, color = detect_sentiment(result)
                        direction_emoji = "🟢" if sentiment == "bullish" else ("🔴" if sentiment == "bearish" else "⚪")
                        embed = discord.Embed(
                            title=f"📊 {direction_emoji} Analysis",
                            description=result[:4096],
                            color=color,
                            timestamp=message.created_at
                        )
                        await trading_channel.send(embed=embed)
                
                history = load_history()
                history.append({
                    "role": "user",
                    "content": full_content[:500] if full_content else "[Chart screenshot]",
                    "timestamp": str(message.created_at),
                    "has_image": bool(message.attachments)
                })
                history.append({
                    "role": "assistant",
                    "content": result,
                    "timestamp": str(message.created_at)
                })
                save_history(history)
        except Exception as e:
            print(f"Error in on_message: {e}")


@bot.command(name="analyze")
async def analyze(ctx, *, message=""):
    attachments = ctx.message.attachments
    
    if not message and not attachments:
        await ctx.reply("Send me a chart screenshot and/or your question about the trade!")
        return
    
    try:
        async with ctx.typing():
            result = await analyze_with_gemini(message, attachments, include_news_summary=False)
            
            await send_analysis_embed(ctx.channel, result, ctx.message)
            
            history = load_history()
            history.append({
                "role": "user",
                "content": message if message else "[Chart screenshot]",
                "timestamp": str(ctx.message.created_at),
                "has_image": bool(attachments)
            })
            history.append({
                "role": "assistant",
                "content": result,
                "timestamp": str(ctx.message.created_at)
            })
            save_history(history)
    except Exception as e:
        print(f"Error in analyze: {e}")
        await ctx.reply("Sorry, something went wrong.")


@bot.command(name="trade")
async def trade(ctx, *, trade_setup):
    try:
        async with ctx.typing():
            prompt = f"Evaluate this scalp trade setup and give feedback:\n{trade_setup}"
            result = await analyze_with_gemini(prompt, include_news_summary=False)
            
            await send_analysis_embed(ctx.channel, result, ctx.message)
            
            history = load_history()
            history.append({
                "role": "user",
                "content": f"Trade setup: {trade_setup}",
                "timestamp": str(ctx.message.created_at)
            })
            history.append({
                "role": "assistant",
                "content": result,
                "timestamp": str(ctx.message.created_at)
            })
            save_history(history)
    except Exception as e:
        print(f"Error in trade: {e}")
        await ctx.reply("Sorry, something went wrong.")


@bot.command(name="history")
async def show_history(ctx):
    history = load_history()
    
    if not history:
        await ctx.reply("No trading history yet!")
        return
    
    msg = "=== LAST 10 CONVERSATIONS ===\n\n"
    for entry in history[-10:]:
        role = entry.get("role", "")
        content = entry.get("content", "")[:150]
        timestamp = entry.get("timestamp", "")[:19]
        msg += f"[{timestamp}] {role.upper()}: {content}...\n\n"
    
    if len(msg) > 2000:
        msg = msg[:1997] + "..."
    
    await ctx.reply(msg)


@bot.command(name="addtrade")
async def add_trade(ctx, *, trade_details):
    context = load_context()
    
    if "trades" not in context:
        context["trades"] = []
    
    context["trades"].append({
        "details": trade_details,
        "timestamp": str(ctx.message.created_at)
    })
    
    context["trades"] = context["trades"][-50:]
    
    save_context(context)
    await ctx.reply(f"Trade recorded: {trade_details[:100]}")


@bot.command(name="trades")
async def show_trades(ctx):
    context = load_context()
    trades = context.get("trades", [])
    
    if not trades:
        await ctx.reply("No trades recorded yet! Use !addtrade <details>")
        return
    
    msg = "=== RECORDED TRADES ===\n\n"
    for trade in trades[-10:]:
        msg += f"- {trade['details'][:150]}\n"
    
    await ctx.reply(msg)


@bot.command(name="reset")
async def reset_history(ctx):
    save_history([])
    save_context({"trader_type": "daytrade", "bias": "NEUTRAL", "bias_confidence": "none", "notes": "", "trades": []})
    await ctx.reply("Trading history and bias reset!")


@bot.command(name="notes")
async def update_notes(ctx, *, notes):
    context = load_context()
    context["notes"] = notes
    save_context(context)
    await ctx.reply("Notes updated!")


@bot.command(name="news")
async def show_news(ctx):
    async with ctx.typing():
        news = await update_news_cache()
        
        if not news:
            await ctx.reply("No news found!")
            return
        
        msg = "**📰 LATEST NEWS:**\n\n"
        for i, item in enumerate(news[:10], 1):
            msg += f"{i}. **[{item['channel']}]**\n{item['content'][:150]}...\n\n"
        
        await ctx.reply(msg)


@bot.command(name="newsrefresh")
async def refresh_news(ctx):
    async with ctx.typing():
        await update_news_cache()
        await ctx.reply("News cache refreshed! AI will now have the latest news.")


@bot.command(name="cmds")
async def show_help(ctx):
    help_text = """**📊 TRADING BOT COMMANDS**

**💬 CHAT (Just type normally!)**
Type any message and I'll automatically respond!
I'll use the latest news from monitored channels for analysis.
Example: What do you think about BTC?

**!news**
View latest 10 news from all monitored channels.
Example: !news

**!newsrefresh**
Refresh the news cache to get latest news.
Example: !newsrefresh

**!analyze [question]**
Analyze a chart screenshot with a question.
Example: !analyze What do you think about this setup?

**!trade <setup>**
Evaluate a trade setup with entry, stop loss, take profit.
Example: !trade Long BTC at 67000, SL 66500, TP 68000

**!history**
View your last 10 conversations with the AI.
Example: !history

**!addtrade <details>**
Record a completed trade for future reference.
Example: !addtrade Long BTC 67000->67800, +1.2%

**!trades**
View your recorded trades.
Example: !trades

**!active**
Set an active trade to monitor. Specify pair and position (long/short), with optional details.
Example: !active BTC long 65k entry

**!status**
Show all active trades being monitored.
Example: !status

**!stop**
Stop monitoring a specific trade pair.
Example: !stop BTC

**!alerts**
Show recent trading alerts.
Example: !alerts 5

**!chart**
Get current BTC/USDT 15M chart screenshot.
Example: !chart

**!volume**
Get Hyperliquid BTC volume and stats.
Example: !volume

**!reset**
Clear all conversation history and start fresh.
Example: !reset

**!cmds**
Show this help message.
Example: !cmds"""
    
    await ctx.reply(help_text)


@bot.command(name="active")
async def set_active_trade(ctx, pair: str, position: str, *, details: str = ""):
    pair_upper = pair.upper()
    position_lower = position.lower()
    
    if position_lower not in ["long", "short"]:
        await ctx.reply("Please specify position type: !active BTC long or !active BTC short")
        return
    
    trades = load_active_trades()
    trades[pair_upper] = {
        "details": details,
        "position": position_lower,
        "entry_time": str(ctx.message.created_at),
        "user": str(ctx.author)
    }
    save_active_trades(trades)
    
    direction_emoji = "📈" if position_lower == "long" else "📉"
    await ctx.reply(f"✅ @everyone Active trade MONITORING enabled for **{pair_upper}** {direction_emoji} **{position_lower.upper()}**\nI'll alert you in real-time for threats and opportunities!")
    
    dir_for_alert = position.upper()
    await send_alert(dir_for_alert, pair_upper, "ACTIVE TRADE START", f"Monitoring {pair_upper} {position.upper()} for {ctx.author}", "high")


@bot.command(name="status")
async def show_active_status(ctx):
    trades = load_active_trades()
    bias, confidence = load_bias()
    
    msg = f"📊 **CURRENT BIAS:** {bias} ({confidence})\n\n"
    
    if not trades:
        msg += "No active trades being monitored. Use `!active BTC <details>` to start monitoring."
        await ctx.reply(msg)
        return
    
    msg += "📊 **ACTIVE TRADES MONITORED:**\n\n"
    for pair, info in trades.items():
        msg += f"• **{pair}** - {info.get('details', 'No details')}\n"
        msg += f"  Started: {info.get('entry_time', 'N/A')}\n\n"
    
    await ctx.reply(msg)


@bot.command(name="bias")
async def show_bias(ctx):
    bias, confidence = load_bias()
    emoji = "📈" if bias == "LONG" else ("📉" if bias == "SHORT" else "⚪")
    await ctx.reply(f"📊 **CURRENT BIAS:** {emoji} **{bias}** ({confidence} confidence)")


@bot.command(name="stop")
async def stop_active_trade(ctx, pair: str):
    pair_upper = pair.upper()
    
    trades = load_active_trades()
    
    if pair_upper in trades:
        del trades[pair_upper]
        save_active_trades(trades)
        
        await ctx.reply(f"✅ Stopped monitoring **{pair_upper}**")
        await send_alert("NEUTRAL", pair_upper, "ACTIVE TRADE END", f"Stopped monitoring {pair_upper}", "medium")
    else:
        await ctx.reply(f"⚠️ **{pair_upper}** is not being monitored")


@bot.command(name="alerts")
async def show_alerts(ctx, limit: int = 10):
    alerts = load_alerts()
    
    if not alerts:
        await ctx.reply("No alerts yet!")
        return
    
    msg = "📊 **RECENT ALERTS:**\n\n"
    for alert in alerts[-limit:]:
        severity_emoji = "🔴" if alert.get("severity") == "high" else "🟡" if alert.get("severity") == "medium" else "🟢"
        msg += f"{severity_emoji} **[{alert.get('type', 'INFO').upper()}]**"
        if alert.get("pair"):
            msg += f" {alert.get('pair')}"
        msg += f"\n{alert.get('message', '')}\n\n"
    
    await ctx.reply(msg[:2000])


@bot.command(name="chart")
async def send_chart(ctx):
    current_usage = get_chart_usage()
    remaining = CHART_DAILY_LIMIT - current_usage
    
    if remaining <= 0:
        await ctx.reply(f"❌ Daily chart limit reached ({CHART_DAILY_LIMIT}/{CHART_DAILY_LIMIT}). Try again tomorrow!")
        return
    
    async with ctx.typing():
        image_bytes = await asyncio.to_thread(fetch_chart_image)
        
        if image_bytes:
            await ctx.send(
                f"📊 **BTC/USDT 15M Chart** - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}\nRemaining today: {remaining}/{CHART_DAILY_LIMIT}",
                file=discord.File(BytesIO(image_bytes), filename="btc_chart.png")
            )
            
            analysis_prompt = "Analyze this BTC/USDT 1H chart from Bybit. Provide: 1) Current trend direction, 2) Key support/resistance levels, 3) Any clear chart patterns, 4) Potential entry points with risk/reward assessment"
            analysis_result = await analyze_with_gemini(analysis_prompt, image_bytes=image_bytes)
            
            await send_analysis_embed(ctx.channel, analysis_result, ctx.message)
        else:
            await ctx.reply("Failed to fetch chart. Please check CHART_IMG_KEY in .env")


@bot.command(name="volume")
async def show_volume(ctx):
    async with ctx.typing():
        volume_data = await asyncio.to_thread(fetch_hyperliquid_volume)
        
        if volume_data:
            mark_price = volume_data.get("markPx", 0)
            prev_price = volume_data.get("prevDayPx", 0)
            price_change = ((mark_price - prev_price) / prev_price * 100) if prev_price > 0 else 0
            
            funding = volume_data.get("funding", 0)
            funding_emoji = "🟢" if funding > 0 else "🔴"
            
            msg = f"""**🔮 HYPERLIQUID BTC STATS**

💰 **24h Volume:** ${volume_data.get('dayNtlVlm', 0):,.0f} USDC
📊 **24h Base Vol:** {volume_data.get('dayBaseVlm', 0):,.4f} BTC
📈 **Open Interest:** ${volume_data.get('openInterest', 0):,.0f} USDC
💵 **Mark Price:** ${mark_price:,.2f}
📉 **24h Change:** {price_change:+.2f}%
{funding_emoji} **Funding Rate:** {funding*100:.4f}%"""
            
            await ctx.reply(msg)
        else:
            await ctx.reply("Failed to fetch Hyperliquid data. Please try again.")


if __name__ == "__main__":
    if not DISCORD_TOKEN:
        print("Error: DISCORD_TRADING_TOKEN not set in .env")
        sys.exit(1)
    
    if not GEMINI_KEY:
        print("Error: GEMINI_KEY not set in .env")
        sys.exit(1)
    
    print("Starting Trading Bot...")
    bot.run(DISCORD_TOKEN)
