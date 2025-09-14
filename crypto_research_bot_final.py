# crypto_research_bot_final.py
# Telegram Crypto Research Bot (OKX-focused) — Liquidity-first, Multi-timeframe Trend Scanner, Smarter AI notes
# - Prioritizes high-liquidity symbols using OKX tickers (volCcy24h)
# - Confirms clear trends using multi-timeframe signals (15m/1H/4H/1D)
# - Computes composite trend & quality scores
# - Adds richer AI analysis text
#
# Notes:
#   - Keep your .env with TELEGRAM_TOKEN
#   - Requires: python-telegram-bot >= 20, pandas, pandas_ta, matplotlib, python-dotenv, requests
#   - This file is a drop-in replacement for your current bot. API endpoints are OKX public endpoints.
#
# Author: ChatGPT (GPT-5 Thinking)
#!/usr/bin/env python3

import os
import requests
import logging
import pandas as pd
import pandas_ta as ta
import matplotlib.pyplot as plt
import logging
import threading
import time
from io import BytesIO
import datetime as dt
import html
import base64, hmac, hashlib
import datetime
from telegram.request import HTTPXRequest
import hashlib
import hmac
import json
import socketserver
import http.server
import httpx


from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
from flask import Flask, request
from urllib.parse import urlencode
import asyncio
from urllib.parse import urlencode, urljoin
from typing import List, Dict, Any, Optional, Tuple


from telegram import Bot
from telegram.ext import Application
from flask import request as flask_request
from telegram import ReplyKeyboardMarkup

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, MessageHandler, ContextTypes, filters
)

CRYPTOPANIC_KEY = "e7e42ec66da05ffb971daa4a81ab716ed3dbcee6"
logger = logging.getLogger(__name__)

OKX_BASE = "https://www.okx.com"
PROXY = None 

NEWS_BUFFER: List[str] = []
LAST_NEWS_FETCH: Optional[dt.datetime] = None
SENT_NEWS_IDS = set()

#=================== Hàm AI tóm tắt tin tức bằng Groq LLM==============
from groq import Groq
def ai_summarize(prompt: str) -> str:
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        return "⚠️ Chưa có GROQ_API_KEY."

    try:
        client = Groq(api_key=api_key)
        resp = client.chat.completions.create(
            model="llama3-8b-8192",  # SỬA: Thay model sai bằng model hợp lệ của Groq (llama3-8b-8192 thay vì llama-3.1-8b-instruct)
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Bạn là trợ lý AI chuyên tóm tắt và dịch tin tức crypto. "
                        "Hãy dịch toàn bộ nội dung sang tiếng Việt và tóm tắt ngắn gọn, dễ hiểu."
                    )
                },
                {"role": "user", "content": prompt}
            ],
            max_tokens=400,
            temperature=0.6
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        return f"❌ Lỗi Groq API: {e}"


# ================== ENV & LOG ==================
load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
if not TELEGRAM_TOKEN:
    raise RuntimeError("❌ TELEGRAM_TOKEN not found! Please set it in Railway Variables or .env")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("crypto_bot_opt")

request = HTTPXRequest(
    connect_timeout=30,
    read_timeout=30
)

app = Application.builder() \
    .token(TELEGRAM_TOKEN) \
    .request(request) \
    .build()

OKX_API_KEY = os.getenv("OKX_API_KEY")
OKX_API_SECRET = os.getenv("OKX_API_SECRET")
OKX_API_PASSPHRASE = os.getenv("OKX_API_PASSPHRASE")

PROXY = os.getenv("PROXY")
proxies = {"http": PROXY, "https": PROXY} if PROXY else None
# ================== GLOBAL STATE ==================
COINS_LIST = []
MARKET_MAP = {
    "BTC-USDT": {"current_price": 30000.0, "vol_quote_24h": 500000000.0, "inst_id": "BTC-USDT"},
    "ETH-USDT": {"current_price": 1800.0, "vol_quote_24h": 200000000.0, "inst_id": "ETH-USDT"},
    "SATS-USDT": {"current_price": 0.0005, "vol_quote_24h": 15000000.0, "inst_id": "SATS-USDT"},
    "PEPE-USDT": {"current_price": 0.0000012, "vol_quote_24h": 80000000.0, "inst_id": "PEPE-USDT"},
    "SHIB-USDT": {"current_price": 0.00001, "vol_quote_24h": 50000000.0, "inst_id": "SHIB-USDT"},
}
TOP_COINS = list(MARKET_MAP.keys())
PAGE_SIZE = 10
PRICE_CACHE = {}
LAST_ALERT = {}
ALERT_CHAT_IDS = set()
ALERT_THRESHOLD = 4.0  # % change between checks to alert
MIN_QUOTE_VOL = 10_000_000  # USDT, 24h quote volume filter (liquidity floor)
MAX_SCAN = 200  # max instruments to scan from OKX

TOKEN = os.getenv("TELEGRAM_TOKEN")  # Đặt trong Render → Environment Variables
PORT = int(os.getenv("PORT", 8080))

# Render URL (chính  fallback)
# Railway URL (set trong Railway Variables)
RAILWAY_URL = os.getenv("RAILWAY_URL", "").strip()
if not RAILWAY_URL:
    raise RuntimeError("❌ RAILWAY_URL chưa được set trong Railway Variables!")

PTB_WEBHOOK_PATH = f"webhook/{TOKEN}"
PTB_WEBHOOK_URL  = f"{RAILWAY_URL}/{PTB_WEBHOOK_PATH}"

# Flask cần route CÓ dấu "/"
#FLASK_WEBHOOK_PATH = f"/{PTB_WEBHOOK_PATH}"

OKX_DOMAINS = ["https://www.okx.com"]

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (CryptoResearchBot)",
    "Content-Type": "application/json"
}

logger.info(f"✅ Using PTB webhook URL: {PTB_WEBHOOK_URL}")
#logger.info(f"✅ Flask will listen on: {FLASK_WEBHOOK_PATH}")

# Telegram Application
application = Application.builder().token(TOKEN).build()
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

# Flow detection globals
LAST_HOURLY_INFLOW_ALERT = {}   # key: coin -> datetime of last hourly inflow alert
LAST_IMMEDIATE_OUTFLOW_ALERT = {}  # key: coin -> datetime of last immediate outflow alert (cooldown short)
HOURLY_INFLOW_COOLDOWN = dt.timedelta(hours=1)
IMMEDIATE_OUTFLOW_COOLDOWN = dt.timedelta(minutes=10)

# thresholds (tuneable)
INFLOW_VOL_MULTIPLIER = 3.0   # nếu vol(1H) >= mean(prev 24 x 1H) * 3 -> inflow mạnh
OUTFLOW_VOL_MULTIPLIER = 2.5  # vol spike
OUTFLOW_PRICE_DROP_PCT = -2.0 # trong 1h giảm <= -2% kèm vol spike -> outflow cảnh báo

LAST_NEWS_IDS = set()             # store unique identifiers (urls or titles) already sent
LAST_NEWS_HOUR = None             # last time hourly market news was broadcast (UTC)
NEWS_HOURLY_COOLDOWN = dt.timedelta(hours=1)

# Flow alert dedupe: key = (coin, timeframe, type) -> datetime
LAST_FLOW_ALERTS = {}

# Timeframes priority for immediate alerts (prefer short timeframes)
FLOW_TFS = ["3m", "15m", "1H", "4H"]
FLOW_IMMEDIATE_COOLDOWN = dt.timedelta(minutes=10)  # per (coin,tf,type)

# API Key cho CryptoPanic (nếu có), nếu không có thì để trống -> bot sẽ fallback CoinStats
CRYPTOPANIC_KEY = "e7e42ec66da05ffb971daa4a81ab716ed3dbcee6"
logger = logging.getLogger(__name__)

BASE_URL = "https://www.okx.com"
DEFAULT_HEADERS = {
"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
"Accept": "application/json, text/plain, */*",
"Accept-Language": "en-US,en;q=0.9",
"Referer": "https://www.okx.com/",
"Origin": "https://www.okx.com",
"Cache-Control": "no-cache",
"Pragma": "no-cache",
"Connection": "keep-alive",
}
RATE_LIMIT_DELAY = 0.2 # giãn cách giữa các request (200ms)
MAX_RETRY = 5

LAST_ALERT_TIME = {}
# Biến toàn cục để theo dõi thời gian gửi tin cuối cùng
last_sent = None

alerts = {}

# ================== TELEGRAM UI  =====================
def main_menu(user_id=None) -> InlineKeyboardMarkup:
    """
    main_menu can accept either an int user_id or an update-like object.
    It will attempt to extract an integer user id in a few common ways.
    """
    # normalize user_id
    try:
        if isinstance(user_id, int):
            uid = user_id
        elif hasattr(user_id, 'effective_user') and getattr(user_id, 'effective_user') is not None:
            uid = user_id.effective_user.id
        elif hasattr(user_id, 'from_user') and getattr(user_id, 'from_user') is not None:
            uid = user_id.from_user.id
        else:
            uid = 0
    except Exception:
        uid = 0
    is_alert_on = alerts.get(uid, False)
    buttons = [
        [
            InlineKeyboardButton("🔎 Research", callback_data="research_btn"),
            InlineKeyboardButton("🤖 Bot DCA", callback_data="bot_dca_btn")
        ],
        [
            InlineKeyboardButton("📊 Top Coins", callback_data="topcoins:0"),
            InlineKeyboardButton("📰 Tin tức", callback_data="news_market_menu")
        ],
        [
            InlineKeyboardButton(f"{'✅' if is_alert_on else '❌'} Alerts", callback_data="toggle_alert")
        ]
    ]
    return InlineKeyboardMarkup(buttons)

def research_choice_markup() -> InlineKeyboardMarkup:
    """
    Tạo menu lựa chọn chế độ research (Long/Short).
    """
    buttons = [
        [
            InlineKeyboardButton("📈 Long", callback_data="research_long"),
            InlineKeyboardButton("📉 Short", callback_data="research_short")
        ],
        [
            InlineKeyboardButton("🔙 Back", callback_data="main")
        ]
    ]
    return InlineKeyboardMarkup(buttons)

def coin_actions_markup(coin: str) -> InlineKeyboardMarkup:
    """
    Tạo menu hành động cho một coin cụ thể.
    """
    buttons = [
        [
            InlineKeyboardButton("📈 Chart", callback_data=f"chart:{coin}"),
            InlineKeyboardButton("📋 Indicators", callback_data=f"ind:{coin}")
        ],
        [
            InlineKeyboardButton("🧠 AI Analysis", callback_data=f"ai:{coin}"),
            InlineKeyboardButton("💡 News", callback_data=f"news_coin:{coin}")
        ],
        [
            InlineKeyboardButton("🔙 Back", callback_data="back_coins")
        ]
    ]
    return InlineKeyboardMarkup(buttons)

def coins_page_markup(page: int) -> InlineKeyboardMarkup:
    """
    Tạo menu phân trang cho danh sách coins.
    """
    global COINS_LIST
    # populate COINS_LIST from OKX/top-percentage-change if empty
    if not COINS_LIST:
        try:
            COINS_LIST = get_top_coins(limit=200)
        except Exception:
            COINS_LIST = list(MARKET_MAP.keys())
    buttons = []
    start = page * 10
    end = start + 10
    for coin in COINS_LIST[start:end]:
        buttons.append([InlineKeyboardButton(coin, callback_data=f"coin:{coin}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"topcoins:{page-1}"))
    if end < len(COINS_LIST):
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"topcoins:{page+1}"))
    if nav:
        buttons.append(nav)
    buttons.append([InlineKeyboardButton("🔙 Back", callback_data="main")])
    return InlineKeyboardMarkup(buttons)

def news_menu_markup(coin: str) -> InlineKeyboardMarkup:
    """
    Tạo menu tin tức (thị trường hoặc coin cụ thể).
    """
    buttons = [
        [
            InlineKeyboardButton("🌍 Thị trường", callback_data=f"news_market:{coin}"),
            InlineKeyboardButton(f"💡 {coin}", callback_data=f"news_coin:{coin}")
        ],
        [
            InlineKeyboardButton("🔙 Back", callback_data=f"coin:{coin}")
        ]
    ]
    return InlineKeyboardMarkup(buttons)


def bot_dca_menu() -> InlineKeyboardMarkup:
    """Tạo menu lựa chọn chế độ Bot DCA."""
    buttons = [
        [
            InlineKeyboardButton("🐂 Bull", callback_data="bot_dca_bull")
        ],
        [
            InlineKeyboardButton("🔙 Back", callback_data="main")
        ]
    ]
    return InlineKeyboardMarkup(buttons)

def main_menu_markup(user=None) -> InlineKeyboardMarkup:
    """Compatibility wrapper used across older code: returns main menu markup.
    If user is None, menu will be shown with default (alerts off).
    """
    return main_menu(user)
# ================== Flask routes =====================
#@flask_app.route("/")
#def home():
#    return "✅ Bot is running with Flask  Polling!"

#@flask_app.route(FLASK_WEBHOOK_PATH, methods=["GET", "POST", "HEAD"])
#def webhook():
 #   if flask_request.method in ("GET", "HEAD"):
#        return "ok", 200

##    try:
#        update = Update.de_json(flask_request.get_json(force=True), application.bot)
#        application.update_queue.put_nowait(update)
#    except Exception as e:
##        logger.error(f"Webhook error: {e}")
###        return "error", 500
#    return "ok", 200

# === Support/Resistance & scoring helpers (simplified) ===
def compute_support_resistance_from_df(df: pd.DataFrame, window: int = 90) -> (Optional[float], Optional[float]):
    """
    Tính toán hỗ trợ và kháng cự cứng từ dữ liệu OHLC.
    - support = giá thấp nhất trong khoảng window
    - resistance = giá cao nhất trong khoảng window
    """
    if df is None or len(df) < 2:
        return None, None

    try:
        # đảm bảo chỉ lấy cột close/low/high
        recent = df.tail(window)
        support = recent["low"].min()
        resistance = recent["high"].max()
        return float(support), float(resistance)
    except Exception:
        return None, None

def compute_trend_score(df: pd.DataFrame, mode: str = "long") -> Tuple[int, Dict[str, Any]]:
    if df is None or df.empty or len(df) < 30:
        return 0, {}
    d = df.copy()
    d["ema20"] = d["close"].ewm(span=20).mean()
    d["ema50"] = d["close"].ewm(span=50).mean()
    delta = d["close"].diff().fillna(0)
    gain = delta.where(delta>0, 0).rolling(14).mean()
    loss = -delta.where(delta<0, 0).rolling(14).mean()
    rs = gain / (loss + 1e-9)
    d["rsi14"] = 100 - (100 / (1 + rs))
    last = d.iloc[-1]
    score = 0
    if last["ema20"] > last["ema50"]:
        score += 30
    else:
        score -= 30
    if last.get("rsi14", 50) > 55 and mode == "long":
        score += 30
    if last.get("rsi14", 50) < 45 and mode == "short":
        score -= 30
    if last["close"] > last["ema50"]:
        score += 20
    else:
        score -= 20
    trend = "bullish" if score >= 40 else "bearish" if score <= -40 else "neutral"
    return score, {"rsi": round(float(last.get("rsi14") or 0), 2), "trend": trend}

def multi_tf_score(symbol: str) -> Tuple[float, Dict[str, Any]]:
    """
    Compute average trend score across multiple timeframes.
    Returns (avg_score, details_per_tf)
    """
    tfs = {"15m": 100, "1H": 200, "4H": 200, "1D": 90}
    scores = {}
    total = 0.0
    count = 0

    for tf, lim in tfs.items():
        df = get_ohlc_okx(symbol, bar=tf, limit=lim)
        if df.empty:
            logger.warning(f"Dataframe rỗng cho timeframe {tf} của {symbol}. Bỏ qua.")
            scores[tf] = {"score": 0, "inds": {}}  # Fallback score 0 nếu dataframe rỗng
            continue
        score, trend_type = compute_trend_score(df, mode=mode)
        inds = {}  # Thêm nếu cần chi tiết inds
        scores[tf] = {"score": score, "inds": inds}
        total += score
        count += 1

    avg = total / count if count > 0 else 0.0
    return avg, scores

def can_alert(coin: str, cooldown: int = 3600):
    """
    Kiểm tra xem coin này có thể cảnh báo tiếp không (tránh spam).
    cooldown: thời gian chờ (giây), mặc định 1h.
    """
    now = time.time()
    last_time = LAST_ALERT_TIME.get(coin, 0)
    if now - last_time >= cooldown:
        LAST_ALERT_TIME[coin] = now
        return True
    return False

def check_liquidity_strength(df):
    """
    Kiểm tra thanh khoản tại thời điểm pump/dump.
    Phân biệt pump/dump thật hay giả dựa vào volume và spread.
    Trả về (ok, text) để đưa vào Alerts.
    """
    try:
        if df.empty or len(df) < 20:
            return False, "⚠️ Không đủ dữ liệu thanh khoản."

        # Lấy nến cuối cùng
        last = df.iloc[-1]
        volume = last["volume"]
        high, low, close, open_ = last["high"], last["low"], last["close"], last["open"]

        # Volume trung bình 20 nến gần nhất
        avg_vol = df["volume"].tail(20).mean()

        # Spread giá (mức dao động)
        spread = (high - low) / low if low > 0 else 0

        # Điều kiện pump thật
        if volume > 2 * avg_vol and spread > 0.01 and close > (open_ + (high - open_) * 0.5):
            return True, f"✅ Pump thật: Volume tăng mạnh ({volume:.2f}), spread {spread:.2%}"

        # Điều kiện pump giả (volume tăng nhưng spread nhỏ)
        if volume > 2 * avg_vol and spread <= 0.01:
            return False, f"❌ Pump ảo: Volume cao ({volume:.2f}) nhưng spread nhỏ ({spread:.2%})"

        # Điều kiện dump thật
        if volume > 2 * avg_vol and spread > 0.01 and close < (open_ - (open_ - low) * 0.5):
            return True, f"⚠️ Dump thật: Volume tăng mạnh ({volume:.2f}), spread {spread:.2%}"

        # Điều kiện dump giả
        if volume > 2 * avg_vol and spread <= 0.01:
            return False, f"❌ Dump ảo: Volume cao ({volume:.2f}) nhưng spread nhỏ ({spread:.2%})"

        return False, "ℹ️ Không có tín hiệu pump/dump rõ ràng."
    except Exception as e:
        return False, f"⚠️ Lỗi khi check thanh khoản: {e}"


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logging.error(msg="Exception while handling update:", exc_info=context.error)
app.add_error_handler(error_handler)


from telegram import BotCommand

async def set_main_menu(application):
    commands = [
        BotCommand("start", "Bắt đầu"),
        BotCommand("research", "Research Long/Short"),
        BotCommand("deepcoin", "Phân tích sâu"),
        BotCommand("topcoins", "Top Coins"),
    ]
    await application.bot.set_my_commands(commands)


# ================== OKX HELPERS ==================
def refresh_markets(limit: int = 60):
    try:
        inst_data = fetch_instruments_okx()
        tickers = fetch_tickers_okx()
        tick_map = {t.get("instId"): t for t in tickers}

        out = {}
        for item in inst_data:
            inst_id = item.get("instId", "")
            if not inst_id.endswith("USDT-SWAP"):
                continue
            base = item.get("uly")
            if not base or not base.endswith("USDT"):
                continue
            coin_id = base

            t = tick_map.get(inst_id, {})
            try:
                last = float(t.get("last")) if t.get("last") is not None else None
            except Exception:
                last = None
            try:
                vol_quote = float(t.get("volCcy24h")) if t.get("volCcy24h") is not None else 0.0
            except Exception:
                vol_quote = 0.0
            try:
                vol_base = float(t.get("vol24h")) if t.get("vol24h") is not None else 0.0
            except Exception:
                vol_base = 0.0

            out[coin_id] = {
                "inst_id": inst_id,
                "base": base.split("-")[0],
                "quote": "USDT",
                "category": "SWAP",
                "current_price": last,
                "vol_quote_24h": vol_quote,
                "vol_base_24h": vol_base,
            }

        liquid = sorted(out.values(), key=lambda x: x.get("vol_quote_24h", 0.0), reverse=True)[:limit]

        global MARKET_MAP, COINS_LIST
        MARKET_MAP = {f"{x['base']}-USDT": x for x in liquid}
        COINS_LIST = list(MARKET_MAP.keys())

        logger.info(f"Refreshed markets: {len(COINS_LIST)} USDT SWAP coins (top by 24h quote vol)")

    except Exception as e:
        logger.exception("refresh_markets error: %s", e)


def okx_get_json(url: str, params: dict | None = None, timeout: int = 15, headers: dict | None = None, retries: int = 3):
    default_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json",
        "Referer": "https://www.okx.com/",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    if headers:
        default_headers.update(headers)

    proxies = {"http": PROXY, "https": PROXY} if globals().get("PROXY") else None

    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=default_headers, timeout=timeout, proxies=proxies)
            if r.status_code in (403, 429, 500, 502, 503, 504):
                wait = 2 ** attempt
                logger.warning(f"Public request {url} → {r.status_code}. retry after {wait}s (attempt {attempt+1}/{retries})")
                time.sleep(wait)
                continue
            r.raise_for_status()
            j = r.json()
            if isinstance(j, dict) and j.get("code") not in (None, "0"):
                logger.debug(f"okx public non-zero code: {j}")
            return j
        except requests.exceptions.HTTPError as e:
            logger.exception(f"OKX HTTPError: {url} {params} {e}")
            time.sleep(1)
        except Exception as e:
            logger.exception(f"OKX request error: {url} {params} {e}")
            time.sleep(1)
    return {}



def okx_public_get(path: str, params: dict = None, timeout: int = 10) -> Optional[dict]:
    url = OKX_BASE.rstrip("/") + path
    headers = {"User-Agent": "Mozilla/5.0 (CryptoResearchBot)"}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.warning(f"okx_public_get error {url}: {e}")
        return None

def get_ohlc_okx(symbol: str, bar: str = "1H", limit: int = 200) -> pd.DataFrame:
    """
    Fetch OHLC from OKX public /market/candles endpoint safely.
    Returns chronological dataframe with columns: ts (datetime UTC), open, high, low, close, vol
    If no data or error -> returns empty DataFrame.
    """
    endpoint = "/api/v5/market/candles"
    params = {"instId": symbol, "bar": bar, "limit": str(limit)}
    j = okx_public_get(endpoint, params=params)
    data = []
    if j and isinstance(j, dict):
        data = j.get("data") or []
    if not data:
        logger.warning(f"⚠️ Không có dữ liệu OHLC cho {symbol} ({bar})")
        return pd.DataFrame()

    try:
        df = pd.DataFrame(data)
        df = df.iloc[:, :6]
        df.columns = ["ts", "open", "high", "low", "close", "vol"]
        for c in ["open", "high", "low", "close", "vol"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        # parse ts robustly
        df["ts"] = pd.to_datetime(pd.to_numeric(df["ts"], errors="coerce"), unit="ms", utc=True)
        df = df.sort_values("ts").reset_index(drop=True)
        logger.info(f"✅ Nhận {len(df)} rows cho {symbol} ({bar})")
        return df
    except Exception as e:
        logger.exception(f"parse candles error for {symbol}: {e}")
        return pd.DataFrame()

def detect_flow_signals(coin: str):
    """
    Return dict with:
      { 'inflow': bool, 'inflow_strength': float, 'outflow': bool, 'outflow_strength': float, 'details': {...} }
    Logic:
      - lấy 1H candles limit=25 (last hour  24 previous 1H)
      - so sánh vol của last candle với mean(vol of previous 24 candles)
      - inflow nếu vol_last >= mean_prev * INFLOW_VOL_MULTIPLIER
      - outflow nếu vol_last >= mean_prev * OUTFLOW_VOL_MULTIPLIER and price change <= OUTFLOW_PRICE_DROP_PCT
    """
    try:
        df1h = get_ohlc_okx(coin, bar="1H", limit=25)
        if df1h.empty or len(df1h) < 6:
            return {"inflow": False, "outflow": False, "details": {}}

        # last candle is the most recent row
        last = df1h.iloc[-1]
        prev = df1h.iloc[:-1]
        mean_prev_vol = float(prev["vol"].mean()) if not prev.empty else 0.0
        last_vol = float(last["vol"]) if not pd.isna(last["vol"]) else 0.0
        price_now = float(last["close"])
        prev_price = float(prev.iloc[-1]["close"]) if len(prev) >= 1 else price_now
        price_change_pct = ((price_now - prev_price) / prev_price) * 100.0 if prev_price != 0 else 0.0

        inflow = False
        outflow = False
        inflow_strength = 0.0
        outflow_strength = 0.0

        if mean_prev_vol > 0:
            # inflow: big volume spike upwards or strong volume (we keep price change positive or not strictly required)
            if last_vol >= mean_prev_vol * INFLOW_VOL_MULTIPLIER and price_change_pct > 0:
                inflow = True
                inflow_strength = last_vol / mean_prev_vol

            # outflow: big volume spike and price drop
            if last_vol >= mean_prev_vol * OUTFLOW_VOL_MULTIPLIER and price_change_pct <= OUTFLOW_PRICE_DROP_PCT:
                outflow = True
                outflow_strength = last_vol / mean_prev_vol

        details = {
            "last_vol": last_vol,
            "mean_prev_vol": mean_prev_vol,
            "price_change_pct": price_change_pct,
            "price_now": price_now
        }
        return {"inflow": inflow, "inflow_strength": inflow_strength,
                "outflow": outflow, "outflow_strength": outflow_strength,
                "details": details}
    except Exception:
        logger.exception(f"detect_flow_signals error for {coin}")
        return {"inflow": False, "outflow": False, "details": {}}

def get_ohlc_okx_sync(instId: str = "BTC-USDT", bar: str = "1H", limit: int = 200) -> pd.DataFrame:
    """
    Synchronous wrapper calling OKX via aiohttp inside event loop is inconvenient,
    so here we perform a blocking request via requests if needed.
    For simplicity we'll implement an asyncio-compatible fetch used by async functions.
    """
    # This synchronous function is left minimal. Prefer using async function get_ohlc_okx in code.
    raise RuntimeError("Please use get_ohlc_okx (async) in async contexts.")

def _normalize_okx_candles_to_df(data: List[List[Any]]) -> pd.DataFrame:
    # OKX returns lists like [ts, open, high, low, close, volume]
    df = pd.DataFrame(data)
    # map first columns
    mapping = {0: "ts", 1: "open", 2: "high", 3: "low", 4: "close", 5: "vol"}
    df = df.rename(columns=mapping)
    # numeric conversions
    for c in ["open", "high", "low", "close", "vol"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    # try parse ts
    if "ts" in df.columns:
        try:
            df["ts"] = pd.to_datetime(df["ts"].astype(float), unit="ms", utc=True)
        except Exception:
            try:
                df["ts"] = pd.to_datetime(df["ts"], utc=True)
            except Exception:
                pass
    df = df.sort_values("ts").reset_index(drop=True)
    return df[["ts", "open", "high", "low", "close", "vol"]]

def fetch_okx(url, params=None, retries=3, timeout=10):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/119.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
        "Referer": "https://www.okx.com/"
    }

    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout, proxies={"http": PROXY, "https": PROXY} if PROXY else None)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if r.status_code == 403:
                logger.warning(f"403 Forbidden, retrying ({attempt+1}/{retries}) {url} {params}")
                time.sleep(1.5)
            else:
                logger.error(f"OKX HTTPError: {url} {params} {e}")
                break
        except Exception as e:
            logger.error(f"OKX request error: {url} {params} {e}")
            time.sleep(1.0)
    return None

def fetch_instruments_okx():
    """Fetch instruments from OKX (USDT-SWAP only)."""
    params = {"instType": "SWAP"}
    for base in OKX_DOMAINS:
        url = f"{base}/api/v5/public/instruments"
        j = okx_get_json_with_proxy(url, params=params, headers=DEFAULT_HEADERS)
        if j and j.get("data"):
            return j["data"]
    logger.warning("⚠️ Public instruments API rỗng hoặc lỗi → trả []")
    return []


def fetch_tickers_okx():
    """Fetch tickers from OKX (USDT-SWAP only)."""
    params = {"instType": "SWAP"}
    for base in OKX_DOMAINS:
        url = f"{base}/api/v5/market/tickers"
        j = okx_get_json_with_proxy(url, params=params, headers=DEFAULT_HEADERS)
        if j and j.get("data"):
            return j["data"]
    logger.warning("⚠️ Public tickers API rỗng hoặc lỗi → trả []")
    return []

# Lấy nến (candlestick) cho 1 coin
def fetch_candles_okx(inst_id: str, bar: str = "1H", limit: int = 200):
    url = f"{OKX_BASE}/market/candles"
    params = {"instId": inst_id, "bar": bar, "limit": limit}
    data = fetch_okx(url, params)
    # Delay nhẹ để tránh spam
    time.sleep(0.2)
    return data.get("data", []) if data else []

# Ví dụ sử dụng API key (tùy chọn)
def fetch_okx_with_key(url, params=None, api_key="", api_pass="", api_secret=""):
    headers = {
        "OK-ACCESS-KEY": api_key,
        "OK-ACCESS-PASSPHRASE": api_pass,
        # Bạn có thể thêm signature  timestamp nếu gọi private endpoints
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Referer": "https://www.okx.com/"
    }
    r = requests.get(url, params=params, headers=headers)
    r.raise_for_status()
    return r.json()

import base64, hmac, hashlib, time, os
import requests

def okx_sign_request(method: str, path: str, body: str = ""):
    """Tạo headers với chữ ký OKX cho request private."""
    api_key = os.getenv("OKX_API_KEY")
    secret_key = os.getenv("OKX_API_SECRET")
    passphrase = os.getenv("OKX_API_PASSPHRASE")

    if not (api_key and secret_key and passphrase):
        return {}

    # timestamp dạng 2025-09-05T03:30:00.000Z
    timestamp = dt.datetime.now(dt.UTC).isoformat(timespec="milliseconds").replace("00:00", "Z")

    # message cần ký
    prehash = f"{timestamp}{method.upper()}{path}{body}"
    sign = hmac.new(
        secret_key.encode("utf-8"),
        prehash.encode("utf-8"),
        hashlib.sha256
    ).digest()
    sign_b64 = base64.b64encode(sign).decode()

    return {
        "OK-ACCESS-KEY": api_key,
        "OK-ACCESS-SIGN": sign_b64,
        "OK-ACCESS-TIMESTAMP": timestamp,
        "OK-ACCESS-PASSPHRASE": passphrase,
        "Content-Type": "application/json"
    }



def okx_get_json_signed(endpoint: str, params=None, method: str = "GET", timeout: int = 15, retries: int = 3):
    """
    endpoint: string like "/api/v5/market/tickers" (must start with '/')
    params: dict for query (GET) or body (POST)
    """
    api_key = os.getenv("OKX_API_KEY", "")
    api_secret = os.getenv("OKX_API_SECRET", "")
    api_pass = os.getenv("OKX_API_PASSPHRASE", "")

    if not (api_key and api_secret and api_pass):
        logger.debug("OKX API keys not configured -> signed request skipped")
        return {}

    base_url = OKX_BASE.rstrip("/") if globals().get("OKX_BASE") else "https://www.okx.com/api/v5"
    # ensure endpoint begins with '/'
    path = endpoint if endpoint.startswith("/") else f"/{endpoint}"
    query = ""
    body = ""

    if method.upper() == "GET" and params:
        query = "?" + urlencode(params)
    elif method.upper() in ("POST", "PUT") and params is not None:
        body = json.dumps(params, separators=(",", ":"), ensure_ascii=False)

    url = base_url + path + query
    proxies = {"http": PROXY, "https": PROXY} if globals().get("PROXY") else None

    for attempt in range(retries):
        try:
            # timestamp in ISO with milliseconds and trailing Z
            ts = dt.datetime.now(dt.UTC).isoformat(timespec="milliseconds").replace("00:00", "Z")
            # prehash: timestamp  method  requestPath  body  (OKX expects full path incl. query if present)
            prehash = f"{ts}{method.upper()}{path}{query}{body}"
            sig = hmac.new(api_secret.encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).digest()
            sign_b64 = base64.b64encode(sig).decode()

            headers = {
                "OK-ACCESS-KEY": api_key,
                "OK-ACCESS-SIGN": sign_b64,
                "OK-ACCESS-TIMESTAMP": ts,
                "OK-ACCESS-PASSPHRASE": api_pass,
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Accept": "application/json",
                "Referer": "https://www.okx.com/",
            }

            r = requests.request(method.upper(), url, headers=headers, data=body if body else None, timeout=timeout, proxies=proxies)
            if r.status_code in (403, 429, 500, 502, 503, 504):
                wait = 2 ** attempt
                logger.warning(f"Signed request {url} → {r.status_code}. retry after {wait}s (attempt {attempt+1}/{retries})")
                time.sleep(wait)
                continue
            r.raise_for_status()
            j = r.json()
            if isinstance(j, dict) and j.get("code") not in (None, "0"):
                logger.debug(f"OKX signed non-zero code: {j}")
            return j
        except requests.exceptions.HTTPError as e:
            logger.exception(f"OKX signed HTTPError: {url} {params} {e}")
            time.sleep(1)
        except Exception as e:
            logger.exception(f"OKX signed request error: {url} {params} {e}")
            time.sleep(1)
    return {}


def get_ticker_okx(inst: str):
    inst_id = MARKET_MAP.get(inst, {}).get("inst_id", f"{inst}-SWAP")
    url_endpoint = "/api/v5/market/ticker"
    params = {"instId": inst_id}

    j = okx_get_json(OKX_BASE.rstrip("/") + url_endpoint, params=params, headers={"User-Agent": "Mozilla/5.0"})
    data_list = j.get("data", []) if j else []
    if not data_list:
        logger.warning(f"Public ticker API rỗng → fallback signed {inst_id}")
        j = okx_get_json_signed(url_endpoint, params=params, method="GET")
        data_list = j.get("data", []) if j else []
    return data_list[0] if data_list else {}


def get_orderbook_okx(inst: str, depth: int = 50):
    inst_id = MARKET_MAP.get(inst, {}).get("inst_id", f"{inst}-SWAP")
    endpoint = "/api/v5/market/books"
    params = {"instId": inst_id, "sz": depth}
    j = okx_get_json(OKX_BASE.rstrip("/") + endpoint, params=params, headers={"User-Agent": "Mozilla/5.0"})
    data = j.get("data", []) if j else []
    if not data:
        logger.warning(f"Public orderbook API rỗng → fallback signed {inst_id}")
        j = okx_get_json_signed(endpoint, params=params, method="GET")
        data = j.get("data", []) if j else []
    return data[0] if data else {}


def fetch_okx_data(endpoint: str, params: dict = None, method: str = "GET"):
    j = okx_get_json(endpoint, params)
    data = j.get("data", []) if j else []
    if not data:
        logger.warning(f"Public API rỗng/403 → fallback signed {endpoint}")
        j = okx_get_json_signed(endpoint, params, method)
        data = j.get("data", []) if j else []
    return data

def okx_get_json_with_proxy(url, params=None, headers=None, timeout=10):
    """Try request with optional proxy fallback."""
    try:
        r = requests.get(url, params=params, headers=headers, timeout=timeout)
        if r.status_code == 403 and proxies:
            # nếu 403 → thử lại với proxy
            logger.warning(f"⚠️ {url} trả 403 → thử lại với proxy")
            r = requests.get(url, params=params, headers=headers, timeout=timeout, proxies=proxies)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.error(f"OKX request error: {url} {params} {e}")
        return None
#==============health-check server==============
class HealthHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/":
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Bot OKX is alive")
        else:
            self.send_error(404)

def start_healthcheck_server(port: int = 8081):
    from http.server import HTTPServer, BaseHTTPRequestHandler
    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"ok")
    try:
        server = HTTPServer(("0.0.0.0", port), H)
        logger.info(f"✅ Healthcheck server listening on {port}")
        server.serve_forever()
    except Exception as e:
        logger.warning(f"Healthcheck server error: {e}")

# ================== FLOW DETECTION ==================
async def detect_flow_signals_async(symbol: str, df: pd.DataFrame):
    if len(df) < 2:
        return None

    coin = symbol.upper()
    now = dt.datetime.now(dt.timezone.utc)

    last_row = df.iloc[-1]
    prev_row = df.iloc[-2]

    inflow = last_row.get("inflow", 0)
    outflow = last_row.get("outflow", 0)
    prev_inflow = prev_row.get("inflow", 0)
    prev_outflow = prev_row.get("outflow", 0)
    price_change = (last_row["close"] - prev_row["close"]) / prev_row["close"] * 100

    alerts = []

    # inflow mạnh -> cảnh báo 1h/lần
    if inflow > prev_inflow * INFLOW_VOL_MULTIPLIER:
        last_alert = LAST_HOURLY_INFLOW_ALERT.get(coin)
        if not last_alert or (now - last_alert) > HOURLY_INFLOW_COOLDOWN:
            alerts.append(f"🚀 Mạnh mẽ dòng tiền vào {coin}!")
            LAST_HOURLY_INFLOW_ALERT[coin] = now

    # outflow mạnh -> cảnh báo ngay
    if outflow > prev_outflow * OUTFLOW_VOL_MULTIPLIER and price_change < OUTFLOW_PRICE_DROP_PCT:
        last_alert = LAST_IMMEDIATE_OUTFLOW_ALERT.get(coin)
        if not last_alert or (now - last_alert) > IMMEDIATE_OUTFLOW_COOLDOWN:
            alerts.append(f"⚠️ Dòng tiền rút ra nhanh {coin}! Giá thay đổi {price_change:.2f}%")
            LAST_IMMEDIATE_OUTFLOW_ALERT[coin] = now

    return alerts if alerts else None


async def detect_flow_multi_tf(symbol: str):
    """
    Multi-timeframe flow detection (prefer short TFs).
    Check FLOW_TFS (3m,15m,1H,4H) theo thứ tự ưu tiên.
    Return:
      { 'inflow': bool, 'outflow': bool, 'tf': tf_string, 'details': {...} }
    """
    try:
        for tf in FLOW_TFS:
            lim = 25 if tf in ("3m", "15m") else 50
            df = get_ohlc_okx(symbol, bar=tf, limit=lim)

            if df is None or df.empty or len(df) < 6:
                continue

            last = df.iloc[-1]
            prev = df.iloc[:-1]

            mean_prev_vol = float(prev["vol"].mean()) if not prev.empty else 0.0
            last_vol = float(last["vol"]) if not pd.isna(last["vol"]) else 0.0
            price_now = float(last["close"])
            prev_price = float(prev.iloc[-1]["close"]) if len(prev) >= 1 else price_now
            price_change_pct = ((price_now - prev_price) / prev_price) * 100.0 if prev_price != 0 else 0.0

            inflow = False
            outflow = False
            inflow_strength = 0.0
            outflow_strength = 0.0

            if mean_prev_vol > 0:
                if last_vol >= mean_prev_vol * INFLOW_VOL_MULTIPLIER and price_change_pct > 0:
                    inflow = True
                    inflow_strength = last_vol / mean_prev_vol
                if last_vol >= mean_prev_vol * OUTFLOW_VOL_MULTIPLIER and price_change_pct <= OUTFLOW_PRICE_DROP_PCT:
                    outflow = True
                    outflow_strength = last_vol / mean_prev_vol

            if inflow or outflow:
                return {
                    "inflow": inflow,
                    "outflow": outflow,
                    "tf": tf,
                    "details": {
                        "last_vol": last_vol,
                        "mean_prev_vol": mean_prev_vol,
                        "price_now": price_now,
                        "price_change_pct": price_change_pct,
                        "inflow_strength": inflow_strength,
                        "outflow_strength": outflow_strength
                    }
                }

        # nếu không có tín hiệu
        return {"inflow": False, "outflow": False, "tf": None, "details": {}}

    except Exception:
        logger.exception(f"detect_flow_multi_tf error for {symbol}")
        return {"inflow": False, "outflow": False, "tf": None, "details": {}}

#=========== message ===========
import html
import logging

async def safe_send(bot, chat_id, text, **kwargs):
    MAX_LEN = 4000
    # Nếu text None hoặc rỗng → thay bằng thông báo fallback
    if not text or not str(text).strip():
        text = "⚠️ Không có dữ liệu để hiển thị."

    if len(text) > MAX_LEN:
        text = text[:MAX_LEN] + "\n... (cắt bớt)"
    try:
        # Escape toàn bộ text trước khi gửi (khi dùng HTML)
        if kwargs.get("parse_mode") == "HTML":
            text = html.escape(text)
        return await bot.send_message(chat_id=chat_id, text=text, **kwargs)
    except Exception as e:
        logging.error(f"send_message failed: {e}")
        return None

async def safe_edit(message, text, **kwargs):
    MAX_LEN = 4000
    # Nếu text None hoặc rỗng → thay bằng thông báo fallback
    if not text or not str(text).strip():
        text = "⚠️ Không có dữ liệu để hiển thị."

    if len(text) > MAX_LEN:
        text = text[:MAX_LEN] + "\n... (cắt bớt)"
    try:
        return await message.edit_text(text, **kwargs)
    except Exception:
        return await message.reply_text(text, **kwargs)



async def okx_public_request(path: str, params: dict | None = None):
    """Call OKX public API with fallback and headers"""
    if params is None:
        params = {}

    # auto add instType=SWAP cho endpoints cần
    if "instruments" in path or "tickers" in path:
        params.setdefault("instType", "SWAP")

    last_exc = None
    for base in OKX_DOMAINS:
        url = f"{base}{path}"
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.get(url, params=params, headers=DEFAULT_HEADERS)
                if r.status_code == 200:
                    return r.json()
                else:
                    logger.warning(f"Public request {url} → {r.status_code}. Response: {r.text[:200]}")
        except Exception as e:
            last_exc = e
            logger.warning(f"Error request {url}: {e}")

    raise last_exc if last_exc else Exception("All OKX domains failed")


# ================== NEWS API ==================
LAST_NEWS_FETCH = None
NEWS_CACHE_TTL = dt.timedelta(minutes=35) 
NEWS_BUFFER = []   # hàng đợi tin còn lại chưa gửi
LAST_NEWS_IDS = set()  # để tránh gửi trùng
NEWS_FETCH_LIMIT = 20
NEWS_PAGE_SIZE = 5

def fetch_all_news(limit: int = NEWS_FETCH_LIMIT) -> List[str]:
    items: List[str] = []
    # 1) CryptoPanic (if key present)
    if CRYPTOPANIC_KEY:
        try:
            url = "https://cryptopanic.com/api/v1/posts/"
            params = {"auth_token": CRYPTOPANIC_KEY, "filter": "hot"}
            r = requests.get(url, params=params, timeout=10)
            if r.status_code == 429:
                logger.warning("CryptoPanic 429 - rate limited, will fallback")
            else:
                r.raise_for_status()
                j = r.json()
                for a in j.get("results", [])[:limit]:
                    title = a.get("title") or ""
                    link = a.get("url") or ""
                    if title and link:
                        nid = f"{title}|{link}"
                        if nid not in SENT_NEWS_IDS:
                            SENT_NEWS_IDS.add(nid)
                            items.append(f"- {title}\\n🔗 {link}")
        except Exception as e:
            logger.warning(f"CryptoPanic error: {e}")

    # 2) CoinStats
    try:
        r = requests.get("https://api.coinstats.app/public/v1/news", params={"skip": 0, "limit": limit}, timeout=10)
        r.raise_for_status()
        js = r.json()
        for a in js.get("news", []):
            title = a.get("title") or ""
            link = a.get("link") or ""
            if title and link:
                nid = f"{title}|{link}"
                if nid not in SENT_NEWS_IDS:
                    SENT_NEWS_IDS.add(nid)
                    items.append(f"- {title}\\n🔗 {link}")
    except Exception as e:
        logger.warning(f"CoinStats error: {e}")

    # 3) CryptoCompare fallback
    try:
        r = requests.get("https://min-api.cryptocompare.com/data/v2/news/?lang=EN", timeout=10)
        r.raise_for_status()
        js = r.json()
        for a in js.get("Data", [])[:limit]:
            title = a.get("title") or ""
            link = a.get("url") or ""
            if title and link:
                nid = f"{title}|{link}"
                if nid not in SENT_NEWS_IDS:
                    SENT_NEWS_IDS.add(nid)
                    items.append(f"- {title}\\n🔗 {link}")
    except Exception as e:
        logger.warning(f"CryptoCompare error: {e}")

    return items[:limit]


def ensure_news_buffer():
    global NEWS_BUFFER, LAST_NEWS_FETCH
    now = dt.datetime.utcnow()
    if NEWS_BUFFER and LAST_NEWS_FETCH and (now - LAST_NEWS_FETCH) < NEWS_CACHE_TTL:
        return
    NEWS_BUFFER = fetch_all_news(limit=NEWS_FETCH_LIMIT)
    LAST_NEWS_FETCH = now

def get_news_page(page: int = 0, page_size: int = NEWS_PAGE_SIZE) -> Tuple[List[str], bool, int]:
    ensure_news_buffer()
    total = len(NEWS_BUFFER)
    if total == 0:
        return (["Không tìm thấy tin tức."], False, 1)
    total_pages = (total + page_size - 1) // page_size
    page = max(0, min(page, total_pages - 1))
    start = page * page_size
    end = start + page_size
    items = NEWS_BUFFER[start:end]
    has_more = end < total
    return items, has_more, total_pages

def get_news_today(limit: int = 10) -> List[str]:
    return fetch_all_news(limit=limit)

# ================== TECHNICALS ==================
def _indicators(df: pd.DataFrame):
    if df.empty or len(df) < 50:
        return {}
    d = df.copy().set_index("ts")
    c = d["close"]
    try:
        d["ema12"] = ta.ema(c, length=12)
        d["ema26"] = ta.ema(c, length=26)
        macd = ta.macd(c, fast=12, slow=26, signal=9)
        if isinstance(macd, pd.DataFrame):
            d["macd"] = macd["MACD_12_26_9"]
            d["macd_signal"] = macd["MACDs_12_26_9"]
            d["macd_hist"] = macd["MACDh_12_26_9"]
        d["rsi14"] = ta.rsi(c, length=14)
        d["adx14"] = ta.adx(d["high"], d["low"], d["close"], length=14)["ADX_14"]
        d["atr14"] = ta.atr(d["high"], d["low"], d["close"], length=14)
    except Exception:
        logger.exception("indicator calc error")
    latest = d.iloc[-1].to_dict()
    # primary signal
    signal = "neutral"
    try:
        rsi = latest.get("rsi14")
        ema12 = latest.get("ema12")
        ema26 = latest.get("ema26")
        macd_v = latest.get("macd")
        macd_s = latest.get("macd_signal")
        if rsi is not None:
            if rsi > 70: signal = "overbought"
            elif rsi < 30: signal = "oversold"
        if macd_v is not None and macd_s is not None and ema12 is not None and ema26 is not None:
            if macd_v > macd_s and ema12 > ema26: signal = "bullish"
            if macd_v < macd_s and ema12 < ema26: signal = "bearish"
    except Exception:
        pass
    out = {
        "latest_close": float(latest.get("close", float("nan"))),
        "rsi": float(latest.get("rsi14")) if latest.get("rsi14") is not None else None,
        "ema12": float(latest.get("ema12")) if latest.get("ema12") is not None else None,
        "ema26": float(latest.get("ema26")) if latest.get("ema26") is not None else None,
        "macd": float(latest.get("macd")) if latest.get("macd") is not None else None,
        "macd_signal": float(latest.get("macd_signal")) if latest.get("macd_signal") is not None else None,
        "macd_hist": float(latest.get("macd_hist")) if latest.get("macd_hist") is not None else None,
        "adx": float(latest.get("ADX_14")) if latest.get("ADX_14") is not None else None,
        "atr": float(latest.get("ATRr_14")) if latest.get("ATRr_14") is not None else None,
        "signal": signal
    }
    return out

def multi_tf_score(symbol: str, mode: str = "long") -> Tuple[float, Dict[str, Any]]:
    """
    Compute average trend score across multiple timeframes.
    Returns (avg_score, details_per_tf)
    """
    tfs = {"15m": 100, "1H": 200, "4H": 200, "1D": 90}
    scores = {}
    total = 0.0
    count = 0

    for tf, lim in tfs.items():
        df = get_ohlc_okx(symbol, bar=tf, limit=lim)
        if df.empty:
            logger.warning(f"Dataframe rỗng cho timeframe {tf} của {symbol}. Bỏ qua.")
            scores[tf] = {"score": 0, "inds": {}}  # Fallback score 0 nếu dataframe rỗng
            continue
        score, inds = compute_trend_score(df, mode=mode)
        scores[tf] = {"score": score, "inds": inds}
        total += score
        count += 1

    avg = total / count if count > 0 else 0.0
    return avg, scores

def percent_change_over_period(df: pd.DataFrame, lookback: int = 24) -> Optional[float]:
    if df.empty or len(df) < lookback + 1:
        return None
    start = df.iloc[-lookback - 1]["close"]
    end = df.iloc[-1]["close"]
    return ((end - start) / start) * 100.0 if start != 0 else None

def compute_support_resistance(df: pd.DataFrame, window: int = 90) -> (Optional[float], Optional[float]):
    """
    Simplified support/resistance calculation from OHLC data.
    """
    if df is None or len(df) < 2:
        return None, None

    try:
        recent = df.tail(window)
        support = recent["low"].min()
        resistance = recent["high"].max()
        return float(support), float(resistance)
    except Exception:
        return None, None

def suggest_entry(inds: dict, price: float, support: Optional[float], resistance: Optional[float], mode: str = "long") -> str:
    """
    Suggest entry price based on indicators.
    """
    if not inds or not price:
        return "N/A"

    signal = inds.get("signal", "neutral")
    if mode == "long" and signal == "bullish":
        return f"Buy near {price:.6f} (bullish signal)"
    elif mode == "short" and signal == "bearish":
        return f"Sell near {price:.6f} (bearish signal)"
    else:
        return "Hold (neutral)"

def suggest_dca_future(price: float, num_orders: int, support: Optional[float] = None) -> Dict[str, Any]:
    if not price or price <= 0 or num_orders <= 0:
        return {}
    if support is None or support <= 0 or support >= price:
        support = price * 0.85  # fallback assume 15% down to support
    max_dd_pct = ((price - support) / price) * 100.0
    avg_step_pct = max_dd_pct / num_orders if num_orders > 0 else 0.0
    steps = []
    for i in range(num_orders):
        entry = price * (1 - avg_step_pct / 100 * (i + 1))
        steps.append(round(entry, 6))
    return {"price_now": price, "max_dd_pct": round(max_dd_pct, 2), "steps": steps, "avg_step_pct": round(avg_step_pct, 4)}

def dca_result_to_text(results):
    """Convert DCA scan results (list or dict) into a user-friendly text.
    Accepts:
      - list[str] -> preformatted lines
      - dict -> {symbol: {..}} -> formatted table
    """
    lines = ["🤖 Kết quả Bot DCA (các ứng viên):"]
    if isinstance(results, dict):
        for sym, info in results.items():
            roi = info.get('roi') if isinstance(info, dict) else None
            price = info.get('price') if isinstance(info, dict) else None
            avg_entry = info.get('avg_entry') if isinstance(info, dict) else None
            extra = []
            if roi is not None:
                extra.append(f"ROI={roi:.2f}%")
            if avg_entry is not None:
                extra.append(f"AvgEntry={avg_entry}")
            if price is not None:
                extra.append(f"Price={price}")
            lines.append(f"{sym}: " + " | ".join(extra))
    elif isinstance(results, (list, tuple)):
        for r in results:
            lines.append(str(r))
    else:
        lines.append(str(results))
    return "\n".join(lines)


def grid_levels(price: float, support: Optional[float] = None, resistance: Optional[float] = None, grids: int = 10):
    if support is None or resistance is None or resistance <= support:
        support = price * 0.95
        resistance = price * 1.05

    levels = [support + i * (resistance - support) / grids for i in range(grids + 1)]
    return [round(lvl, 6) for lvl in levels]

def is_new_coin(symbol: str, days: int = 30) -> bool:
    """
    Kiểm tra xem coin có phải là coin mới list không.
    Logic: nếu trong MARKET_MAP có 'list_date' thì dùng,
    nếu không có thì fallback dựa vào dữ liệu OHLC (thời điểm nến đầu tiên).
    """
    info = MARKET_MAP.get(symbol, {})
    list_date = info.get("list_date")
    if list_date:
        try:
            first_date = pd.to_datetime(list_date, utc=True)
            return (dt.datetime.now(dt.UTC) - first_date).days <= days
        except Exception:
            pass

    try:
        df = get_ohlc_okx(symbol, bar="1D", limit=400)
        if isinstance(df, pd.DataFrame) and not df.empty:
            first_date = df["ts"].min()
            return (dt.datetime.now(dt.UTC) - first_date).days <= days
    except Exception:
        pass

    return False


def get_funding_rate(symbol: str) -> float | None:
    """
    Lấy funding rate của perpetual futures từ OKX.
    Nếu không có trả về None.
    """
    try:
        endpoint = "/api/v5/public/funding-rate"
        params = {"instId": f"{symbol}-SWAP"}  # funding áp dụng cho contract SWAP
        j = okx_get_json(OKX_BASE.rstrip("/") + endpoint, params=params)
        data = j.get("data", []) if j else []
        if data:
            return float(data[0].get("fundingRate", 0))
    except Exception as e:
        logger.warning(f"⚠️ funding rate fetch failed for {symbol}: {e}")
    return None


def filter_dca_candidates(symbols: list[str]) -> list[str]:
    """
    Lọc danh sách coin theo tiêu chí đủ điều kiện DCA:
    - Thanh khoản cao
    - Coin mới
    - Funding âm
    - Tăng trưởng mạnh trong 24h
    """
    candidates = []
    for cid in symbols:
        try:
            if not is_new_coin(cid):
                continue
            funding = get_funding_rate(cid)
            if funding is None or funding >= 0:
                continue
            df1h = get_ohlc_okx(cid, bar="1H", limit=200)
            growth = percent_change_over_period(df1h, lookback=24) or 0.0
            if growth < 10:  # ví dụ yêu cầu >=10%
                continue
            candidates.append(cid)
        except Exception:
            logger.exception(f"filter_dca_candidates error for {cid}")
            continue
    return candidates

# === Bot commands & handlers ===
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start_handler(update, context)

async def research_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ /research [coin1 coin2 ...] - quick research for given coins or defaults in MARKET_MAP
    This function will:
      - compute simple scores for multiple timeframes
      - compute support/resistance D1
      - produce DCA (15/20/30)  Grid suggestion (10)
    """
    args = context.args or []
    coins = [a.upper() for a in args] if args else list(MARKET_MAP.keys())[:20]
    if not coins:
        await update.message.reply_text("No coins available in MARKET_MAP. Please populate MARKET_MAP or pass coins as args.")
        return

    lines = []
    for coin in coins:
        try:
            # fetch candles for multiple timeframes
            df15 = get_ohlc_okx(coin, bar="15m", limit=200)
            df1h = get_ohlc_okx(coin, bar="1H", limit=200)
            df4h = get_ohlc_okx(coin, bar="4H", limit=200)
            df1d = get_ohlc_okx(coin, bar="1D", limit=200)

            price = None
            if not df1h.empty:
                price = float(df1h.iloc[-1]["close"])
            elif coin in MARKET_MAP:
                price = MARKET_MAP[coin].get("current_price")

            if not price:
                continue

            s15, _ = compute_trend_score(df15)
            s1h, _ = compute_trend_score(df1h)
            s4h, _ = compute_trend_score(df4h)
            s1d, _ = compute_trend_score(df1d)
            avg_score = round((s15 + s1h + s4h + s1d) / 4.0, 1)

            # compute support/resistance on D1 for robust "hard" SR
            sup_d1, res_d1 = compute_support_resistance_from_df(df1d, window=90)
            # fallback to H1 if D1 not available
            if sup_d1 is None or res_d1 is None:
                sup_d1, res_d1 = compute_support_resistance_from_df(df1h, window=90)

            # compute short pct 24h if available
            pct_24h = 0.0
            if not df1d.empty and len(df1d) >= 2:
                last = df1d.iloc[-1]["close"]
                prev = df1d.iloc[-2]["close"]
                pct_24h = ((last - prev) / prev) * 100.0 if prev != 0 else 0.0

            # DCA & grid suggestions
            cfg15 = suggest_dca_future(price, 15, support=sup_d1)
            cfg20 = suggest_dca_future(price, 20, support=sup_d1)
            cfg30 = suggest_dca_future(price, 30, support=sup_d1)
            grid_cfg = suggest_grid_future(price, support=sup_d1, resistance=res_d1, grids=10)

            # produce text compact
            line = (
                f" {coin}  | Score(avg):  {avg_score} \n"
                f"15m/1H/4H/1D:  {int(s15)}/{int(s1h)}/{int(s4h)}/{int(s1d)} \n"
                f"Price:  {price:.8f}  | 1DΔ:  {pct_24h:.2f}% \n"
                f"Support(D1):  {sup_d1}  | Resistance(D1):  {res_d1} \n"
                f"🤖 DCA Future (TP={cfg15['tp_pct']}%, Lev=x{cfg15['leverage']}):\n"
                f" • Base step%: {cfg15['base_step_pct']}% | Money×: {cfg15['money_multiplier']} | Step×: {cfg15['step_multiplier']}\n"
                f" • MaxDrawdownNeeded ≈  {cfg15['max_drawdown_pct']}% \n"
                f" • 15 orders sample: {', '.join(map(lambda x: str(x['price']), cfg15['steps'][:5]))}...\n"
                f" • 20 orders sample: {', '.join(map(lambda x: str(x['price']), cfg20['steps'][:5]))}...\n"
                f" • 30 orders sample: {', '.join(map(lambda x: str(x['price']), cfg30['steps'][:5]))}...\n"
				f"Sức chống chịu (max drawdown): {cfg15['max_drawdown_pct']}%"
                f"🔲 Grid: {len(grid_cfg['grid_levels'])-1} grids | step% ≈ {grid_cfg['grid_step_pct']}% | Range: {grid_cfg['support']} ↔ {grid_cfg['resistance']}\n"
            )
            lines.append(line)
        except Exception:
            logger.exception(f"research_command error for {coin}")

    out = "\n\n".join(lines) if lines else "No results"
    await update.message.reply_text(out, parse_mode="HTML", disable_web_page_preview=True)

async def research_dca_bot(update: Update, context: ContextTypes.DEFAULT_TYPE, mode: str = "bull"):
    """
    Optimized scan for Bot DCA candidates.
    Only Bull mode supported (as requested). Conditions:
      - vol_quote_24h >= MIN_QUOTE_VOL
      - is_new_coin(...) == True  (if REQUIRE_NEW_COIN)
      - funding rate < 0 (if REQUIRE_FUNDING_NEGATIVE)
      - 24h growth >= MIN_24H_GROWTH_PCT
    """
    # Tuning parameters (you can adjust these constants)
    MIN_QUOTE_VOL_LOCAL = globals().get("MIN_QUOTE_VOL", 10_000_000)
    MIN_24H_GROWTH_PCT = 5.0          # minimal 24h increase (%) to consider
    REQUIRE_NEW_COIN = True           # only consider new listings
    REQUIRE_FUNDING_NEGATIVE = True   # require funding < 0
    MAX_COINS_TO_CHECK = 120         # limit to speed up scan

    lines = []
    checked = 0
    # iterate top liquidity coins first
    for coin, info in sorted(MARKET_MAP.items(), key=lambda kv: kv[1].get("vol_quote_24h", 0), reverse=True):
        if checked >= MAX_COINS_TO_CHECK:
            break
        checked += 1
        try:
            price = info.get("current_price")
            volq = info.get("vol_quote_24h", 0)
            if not price or volq < MIN_QUOTE_VOL_LOCAL:
                continue

            # D1 candles to compute trendscore/support
            df1d = get_ohlc_okx(coin, bar="1D", limit=200)
            if df1d.empty or len(df1d) < 30:
                continue

            # require bullish trend on D1 (compute_trend_score returns (score, type))
            trend_score, trend_type = compute_trend_score(df1d)
            if trend_type != "bullish":
                continue

            # optional require new coin (helps pick momentum listings)
            if REQUIRE_NEW_COIN and not is_new_coin(coin, days=60):
                continue

            # funding rate negative requirement
            if REQUIRE_FUNDING_NEGATIVE:
                try:
                    funding = get_funding_rate(coin.replace("-USDT",""))  # get_funding_rate expects symbol format mostly
                    if funding is None or funding >= 0:
                        continue
                except Exception:
                    continue

            # 24h growth
            # compute approximate 24h change using df1d last two rows
            try:
                last = float(df1d.iloc[-1]["close"])
                prev = float(df1d.iloc[-2]["close"])
                growth_24h = ((last - prev) / prev) * 100.0 if prev != 0 else 0.0
            except Exception:
                growth_24h = 0.0
            if growth_24h < MIN_24H_GROWTH_PCT:
                continue

            # support (D1)
            sup, res = compute_support_resistance_from_df(df1d, window=90)
            if not sup or sup >= price:
                continue

            max_dd_pct = ((price - sup) / price) * 100.0
            step15 = round(max_dd_pct / 15, 3)
            step20 = round(max_dd_pct / 20, 3)
            step30 = round(max_dd_pct / 30, 3)

            margin = 20
            kq15 = round(margin * 15 * (max_dd_pct/100), 3)
            kq20 = round(margin * 20 * (max_dd_pct/100), 3)
            kq30 = round(margin * 30 * (max_dd_pct/100), 3)

            # pretty trend label
            trend_label = "🟢 Uptrend (Bull)" if trend_type == "bullish" else "⚪ Neutral"

            text = (
                f"🔎 <b>{coin}</b>\n"
                f"💵 Giá hiện tại: <b>{price:.6f}</b>\n"
                f"💧 Vol24h: ~{int(volq):,} USDT\n"
                f"🛡️ Support (D1): {sup:.6f} | Resistance: {res:.6f}\n"
                f"📉 Max Drawdown cần: {max_dd_pct:.2f}%\n"
                f"📊 Trend Score: <b>{trend_score:.0f}</b> | {trend_label}\n"
                f"📈 24h growth: {growth_24h:.2f}%\n\n"
                f"➡️ <b>Bước giá gợi ý</b> (margin×{margin}):\n"
                f"• 15 orders: {step15}% | Ký quỹ ≈ {kq15}\n"
                f"• 20 orders: {step20}% | Ký quỹ ≈ {kq20}\n"
                f"• 30 orders: {step30}% | Ký quỹ ≈ {kq30}\n"
                f"🔗 Funding: {funding if 'funding' in locals() else 'N/A'}\n"
                "────────────────────────"
            )
            lines.append(text)

        except Exception as e:
            logger.exception(f"DCA research error {coin}: {e}")
            continue

    out = "\n\n".join(lines) if lines else "❌ Không tìm thấy coin phù hợp."
    chat_id = update.effective_chat.id
    await safe_send(context.bot, chat_id=chat_id, text=out, parse_mode="HTML", disable_web_page_preview=True)
async def dca_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ /dca <COIN>
    Prints full DCA and Grid suggestions for a coin
    """
    args = context.args or []
    if not args:
        await update.message.reply_text("Usage: /dca COIN (e.g. /dca BTC-USDT)")
        return
    coin = args[0].upper()
    # fetch price  SR D1
    df1h = get_ohlc_okx(coin, bar="1H", limit=200)
    df1d = get_ohlc_okx(coin, bar="1D", limit=200)
    price = None
    if not df1h.empty:
        price = float(df1h.iloc[-1]["close"])
    elif coin in MARKET_MAP:
        price = MARKET_MAP[coin].get("current_price")

    sup_d1, res_d1 = compute_support_resistance_from_df(df1d, window=90)
    if sup_d1 is None or res_d1 is None:
        sup_d1, res_d1 = compute_support_resistance_from_df(df1h, window=200)

    if not price:
        await update.message.reply_text(f"Cannot find price for {coin}")
        return

    cfg15 = suggest_dca_future(price, 15, support=sup_d1)
    cfg20 = suggest_dca_future(price, 20, support=sup_d1)
    cfg30 = suggest_dca_future(price, 30, support=sup_d1)
    grid_cfg = suggest_grid_future(price, support=sup_d1, resistance=res_d1, grids=10)

    parts = [
        f"⚙️ DCA & Grid suggestions for {coin}",
        f"Price: {price:.8f}",
        f"TP: {cfg15['tp_pct']}% | Leverage: x{cfg15['leverage']}",
        "",
        "DCA 15 orders (order,price,step%,money×):",
        "\n".join([f"{s['order']}: {s['price']} | step%={s['step_pct']} | money×={s['money_x']}" for s in cfg15['steps']]),
        "",
        "DCA 20 orders sample (first 10):",
        "\n".join([f"{s['order']}: {s['price']} | step%={s['step_pct']} | money×={s['money_x']}" for s in cfg20['steps'][:10]]),
        "",
        "DCA 30 orders sample (first 10):",
        "\n".join([f"{s['order']}: {s['price']} | step%={s['step_pct']} | money×={s['money_x']}" for s in cfg30['steps'][:10]]),
        "",
        "Grid (10 grids):",
        ", ".join(map(str, grid_cfg['grid_levels'])),
        "",
        f"Support(D1): {sup_d1} | Resistance(D1): {res_d1}",
    ]
    text = "\n".join(parts)
    await update.message.reply_text(text)

async def scan_alerts(context: ContextTypes.DEFAULT_TYPE):
    try:
        for coin in COINS_LIST:
            df = get_ohlc_okx(coin, bar="1H", limit=200)
            if df.empty:
                continue


            price = float(df.iloc[-1]["close"])
            score, details = multi_tf_score(coin, mode="long")


            # 🔎 Kiểm tra thanh khoản
            ok, liq_text = check_liquidity_strength(df)


            if abs(score) >= 3 and ok and can_alert(coin):
                msg = (
                    f"🚨 Alert {coin}\n"
                    f"💰 Giá: {price}\n"
                    f"{liq_text}\n"
                    f"📊 Score(15m/1H/4H/1D): "
                    f"{details['15m']['score']:.0f}/"
                    f"{details['1H']['score']:.0f}/"
                    f"{details['4H']['score']:.0f}/"
                    f"{details['1D']['score']:.0f}"
                )
                for chat_id in ALERT_CHAT_IDS:
                    await safe_send(context.bot, chat_id,text=msg)
    except Exception as e:
        logger.exception(f"scan_alerts error: {e}")

async def send_flow_alerts(context, coin: str, sig: dict):
    """
    Gửi cảnh báo Pump/Dump thật với phân tích tiếp diễn
    sig: dict từ detect_flow_multi_tf
    """
    tf = sig.get("tf") or "?"
    d = sig.get("details", {})
    last_vol = d.get("last_vol", 0)
    mean_vol = d.get("mean_prev_vol", 0)
    pct = d.get("price_change_pct", 0.0)
    inflow = sig.get("inflow")
    outflow = sig.get("outflow")

    # Lấy dữ liệu 1H để phân tích kỹ thuật
    df = get_ohlc_okx(coin, bar="1H", limit=200)
    score_long, inds = compute_trend_score(df, mode="long")
    score_short, inds_short = compute_trend_score(df, mode="short")

    # Các chỉ báo chính
    ema12, ema26 = inds.get("ema12"), inds.get("ema26")
    macd, macd_sig = inds.get("macd"), inds.get("macd_signal")
    rsi = inds.get("rsi")

    # Kết luận pump/dump tiếp diễn
    continuation = "❓ Chưa rõ xu hướng tiếp diễn."
    if inflow:
        if score_long >= 50 and ema12 > ema26:
            if macd and macd_sig and macd > macd_sig and (rsi is None or rsi < 75):
                continuation = "🚀 Khả năng cao tiếp tục PUMP mạnh (EMA & MACD đồng thuận, RSI chưa quá mua)."
            else:
                continuation = "⚡ Pump mạnh nhưng chỉ báo chưa đồng thuận hoàn toàn."
        else:
            continuation = "⚠️ Pump nhưng xu hướng chưa chắc chắn (cẩn thận trap)."

    elif outflow:
        if score_short >= 50 and ema12 < ema26:
            if macd and macd_sig and macd < macd_sig and (rsi is None or rsi > 25):
                continuation = "⚠️ Khả năng cao tiếp tục DUMP mạnh (EMA & MACD đồng thuận, RSI chưa quá bán)."
            else:
                continuation = "⚡ Dump mạnh nhưng chỉ báo chưa đồng thuận hoàn toàn."
        else:
            continuation = "⚠️ Dump nhưng xu hướng chưa chắc chắn (cẩn thận trap)."

    # Xây tin nhắn gửi đi
    if inflow:
        msg = (
            f"🔥 [15m] INFLOW đột biến: {coin}\n"
            f"Vol: {last_vol:.0f} | MeanPrev: {mean_vol:.0f}\n"
            f"Δ: {pct:.2f}% | Strength: x{d.get('inflow_strength') or 0:.2f}\n\n"
            f"{continuation}"
        )
    else:
        msg = (
            f"⚠️ [15m] OUTFLOW đột biến: {coin}\n"
            f"Vol: {last_vol:.0f} | MeanPrev: {mean_vol:.0f}\n"
            f"Δ: {pct:.2f}% | Strength: x{d.get('outflow_strength') or 0:.2f}\n\n"
            f"{continuation}"
        )

    # Gửi tới các chat đã bật Alerts
    for chat in list(ALERT_CHAT_IDS):
        try:
            await safe_send(context.bot,chat_id=chat, text=msg)
        except Exception as e:
            logger.exception("Failed to send flow alert")

async def reset_webhook(app: Application):
    bot: Bot = app.bot
    # Xóa webhook cũ
    await bot.delete_webhook()
    logger.info("❌ Old webhook deleted")
    # Đặt webhook mới
    await bot.set_webhook(url=WEBHOOK_URL)
    logger.info(f"✅ New webhook set: {WEBHOOK_URL}")

# ================== HANDLERS ==================
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"📩 Received /start from user {update.effective_user.id if update.effective_user else 'unknown'}")
    await update.message.reply_text("👋 Crypto Research Bot", reply_markup=main_menu(update.effective_user.id))

async def research_run(update_obj, context: ContextTypes.DEFAULT_TYPE, mode: str = "long"):
    """Run research scanning TOP_COINS; update_obj may be callback_query or message object (for flexibility)."""
    try:
        # prepare UI: if callback_query, edit message; if message, send new message
        if hasattr(update_obj, "edit_message_text"):
            waiting_msg = await update_obj.edit_message_text(f"⏳ Running research ({mode})...")
        else:
            waiting_msg = await update_obj.reply_text(f"⏳ Running research ({mode})...")

        results = []
        candidates = filter_research_coins(mode=mode, top_n=25)
        for coin in candidates:
            df = get_ohlc_okx(coin, bar="1H", limit=200)
            if df.empty or len(df) < 30:
                logger.warning(f"Dataframe rỗng cho {coin} (1H). Bỏ qua.")
                continue
            score, details = compute_trend_score(df, mode=mode)
            results.append(f"{coin} | score {score} | RSI {details.get('rsi')} | trend {details.get('trend')}")

        text = "📊 Research results:\\n\\n" + ("\\n".join(results) if results else "❌ Không có dữ liệu để phân tích.")
        # reply or edit back to menu
        if hasattr(update_obj, "edit_message_text"):
            await update_obj.edit_message_text(text, reply_markup=main_menu(update_obj))
        else:
            await update_obj.reply_text(text, reply_markup=main_menu(update_obj))
    except Exception as e:
        logger.exception(f"research_run error: {e}")
        if hasattr(update_obj, "edit_message_text"):
            await update_obj.edit_message_text("❌ Research gặp lỗi.", reply_markup=main_menu(update_obj))
        else:
            await update_obj.reply_text("❌ Research gặp lỗi.", reply_markup=main_menu(update_obj))

async def research_callback_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # helper to call research_run with callback_query.message as target for editing
    query = update.callback_query
    await query.answer()
    await research_run(query, context, mode="long" if query.data == "research_long" else "short")

async def research_long_short_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # if called by message (text), emulate long by default
    if isinstance(update, Update) and update.callback_query:
        await research_callback_wrapper(update, context)
        return
    else:
        await research_run(update.message, context, mode="long")

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"📩 Received callback: {update.callback_query.data}")
    query = update.callback_query
    data = query.data
    chat_id = update.effective_chat.id   # lấy chat id

    if data.startswith("research:"):
        _, symbol, mode = data.split(":")
        await research_handler(update, context, symbol=symbol, mode=mode)
        return

    if data == "main":
        await query.edit_message_text("🏠 Menu", reply_markup=main_menu(update.effective_user.id))
        return

    elif data.startswith("topcoins:"):
        page = int(data.split(":")[1])
        await safe_edit(query.message, text="🔥 Top Coins theo thanh khoản:", reply_markup=coins_page_markup(page))

    elif data.startswith("coin:"):
        coin = data.split(":")[1]
        price = MARKET_MAP.get(coin, {}).get("current_price")
        volq = MARKET_MAP.get(coin, {}).get("vol_quote_24h", 0)
        txt = f"🔎 {coin}\\nGiá: {price} USDT\\nThanh khoản 24h: ~{volq:,.0f} USDT"
        await safe_send(context.bot, chat_id=chat_id, text=txt, reply_markup=coin_actions_markup(coin))

    elif data.startswith("chart:"):
        coin = data.split(":")[1]
        df = get_ohlc_okx(coin, bar="1D", limit=200)
        buf = create_price_chart(df, coin)
        await context.bot.send_photo(chat_id=chat_id, photo=buf, caption=f"📊 {coin} - 1D")

    elif data.startswith("ind:"):
        coin = data.split(":")[1]
        df = get_ohlc_okx(coin, bar="1H", limit=200)
        _, inds = compute_trend_score(df, mode="long")
        if not inds:
            await safe_send(context.bot,chat_id=chat_id, text="Không đủ dữ liệu.", reply_markup=coin_actions_markup(coin))
            return
        text = (f"📋 {coin} (1H):\n"
                f"- Close: {inds.get('latest_close')}\n"
                f"- RSI: {inds.get('rsi')}\n"
                f"- EMA12/26: {inds.get('ema12')}/{inds.get('ema26')}\n"
                f"- MACD/MACDs: {inds.get('macd')}/{inds.get('macd_signal')}\n"
                f"- ADX: {inds.get('adx')}\n"
                f"- Signal: {inds.get('signal')}\n")
        await safe_send(context.bot,chat_id=chat_id, text=text, reply_markup=coin_actions_markup(coin))

    elif data.startswith("ai:"):
        coin = data.split(":")[1]
        avg, details = multi_tf_score(coin, mode="long")
        volq = MARKET_MAP.get(coin, {}).get("vol_quote_24h", 0)
        ai_text = ai_analysis(coin, details, volq, "long")
        await safe_send(context.bot,chat_id=chat_id, text=ai_text, reply_markup=coin_actions_markup(coin))

    elif data == "back_coins":
        await safe_send(context.bot,chat_id=chat_id, text="📊 Top Coins (select):", reply_markup=coins_page_markup(0))

    elif data == "toggle_alert":
        user_id = update.effective_user.id
        alerts[user_id] = not alerts.get(user_id, False)
        await safe_edit(
            update.callback_query.message,
            text="🏠 Menu",
            reply_markup=main_menu(user_id)
        )

    if data in ("research_long", "research_short"):
        await research_callback_wrapper(update, context)
        return
        return
		
    if data == "research_btn":
        await query.edit_message_text("🔎 Chọn chế độ Research:", reply_markup=research_choice_markup())
        return", reply_markup=research_choice_markup())
        return
		
    elif data == "news_market_menu":
        news_list = get_news_today(limit=10)
        text = "📰 Tin tức hôm nay:\n\n"  "\n\n".join(news_list)
        await query.message.reply_text(text)

    elif data.startswith("news_menu:"):
        coin = data.split(":")[1]
        await safe_edit(update.callback_query.message, "📰 Chọn loại tin tức:", reply_markup=news_menu_markup(coin))

    elif data.startswith("news_market:"):
        coin = data.split(":")[1]
        news_list = fetch_all_news()
        news_text = "📰 Tin tức thị trường:\n\n"  "\n\n".join(news_list)
        await safe_send(context.bot,chat_id=chat_id, text=news_text, reply_markup=news_menu_markup(coin))

    elif data.startswith("news_coin:"):
        coin = data.split(":")[1]
        news_list = get_news_coin(coin)
        news_text = f"💡 Tin tức về {coin}:\n\n"  "\n\n".join(news_list)
        await safe_send(context.bot,chat_id=chat_id, text=news_text, reply_markup=news_menu_markup(coin))

    if data == "research_long":
        await research_handler(update, context, mode="long")

    elif data == "research_short":
        await research_handler(update, context, mode="short")
    
    if data == "bot_dca_btn":
        # show dca options; for demo we only have bull option
        kb = [[InlineKeyboardButton("Bull (only)", callback_data="bot_dca_bull")],
              [InlineKeyboardButton("🔙 Back", callback_data="main")]]
        await query.edit_message_text("Chọn chế độ lọc Bot DCA:", reply_markup=InlineKeyboardMarkup(kb))
        return

    if data == "bot_dca_bull":
        await research_dca_handler(update, context)
        return
        return

    elif data.startswith("dca:"):
        coin = data.split(":")[1]
        # recompute quickly
        df = get_ohlc_okx(coin, bar="1H", limit=200)
        price = float(df.iloc[-1]["close"]) if not df.empty else MARKET_MAP.get(coin,{}).get("current_price",0)
        res, sup = compute_support_resistance(df, window=90)
        d15 = dca_levels(price, 15, 0.15)
        d20 = dca_levels(price, 20, 0.18)
        d30 = dca_levels(price, 30, 0.22)
        grid10 = grid_levels(price, support=sup, resistance=res, grids=10)
        text = (
            f"📊  {coin} \n"
            f"💵 Giá hiện tại:  {price:.6f} USDT \n"
            f"🛡️ Hỗ trợ gần nhất: {sup:.6f}\n\n"
            f"⚙️  Chiến lược DCA \n"
            f"├ 15 lệnh: {', '.join(f'{x:.6f}' for x in d15)}\n"
            f"├ 20 lệnh: {', '.join(f'{x:.6f}' for x in d20)}\n"
            f"└ 30 lệnh: {', '.join(f'{x:.6f}' for x in d30)}\n\n"
            f"📐  Chiến lược Grid (10) \n"
            f"{' | '.join(f'{x:.6f}' for x in grid10)}\n"
        )
        await safe_send(context.bot, chat_id=chat_id, text=text, parse_mode="HTML")

    elif data.startswith("chart:"):
        coin = data.split(":", 1)[1]
        await chart_handler(update, context, coin)

    elif data.startswith("ind:"):
        coin = data.split(":", 1)[1]
        await indicators_handler(update, context, coin)

    elif data.startswith("ai:"):
        coin = data.split(":", 1)[1]
        await ai_analysis(update, context, coin)

    elif data == "news_market_menu" or data.startswith("news_market"):
        await news_market_handler(update, context)
        return

    if data == "news_more":
        batch, has_more = get_news_batch(15)
        if not batch:
            await query.answer("Hết tin rồi ✅")
            return
        text = "📰 Tin tức tiếp theo:\n\n" + "\n\n".join(batch)
        buttons = []
        if has_more:
            buttons.append([InlineKeyboardButton("📩 Xem thêm tin tức", callback_data="news_more")])
        await query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons), disable_web_page_preview=True)

    if data.startswith("news:"):
        await news_page_handler(update, context)
        return
        return

    # fallback
    await query.edit_message_text("❓ Unknown action", reply_markup=main_menu(update.effective_user.id))

import datetime as dt
async def background_price_checker(context: ContextTypes.DEFAULT_TYPE):
    global last_sent
    utcnow = dt.datetime.now(dt.timezone.utc)
    if not last_sent or (utcnow - last_sent) >= FLOW_IMMEDIATE_COOLDOWN:
        last_sent = utcnow
    """
    Enhanced background job:
     - refresh markets stub
     - hourly market news broadcast (deduped)
     - detect multi-TF flow signals and send immediate alerts to toggled chats (prioritize m3/m15)
     - existing price move alerts kept
    """
    try:
        await refresh_markets_stub()
        utcnow = dt.datetime.now(dt.timezone.utc)

        # Hourly market news broadcast (only once per NEWS_HOURLY_COOLDOWN)
        global LAST_NEWS_HOUR, LAST_NEWS_IDS
        if LAST_NEWS_HOUR is None or (utcnow - LAST_NEWS_HOUR) >= NEWS_HOURLY_COOLDOWN:
            try:
                articles = fetch_all_news(limit=10) or []
            except Exception:
                articles = []
            new_articles = []
            for a in articles:
                uid = a  # fallback: use full string as ID (titlelink). If you have an URL field, use that instead.
                if uid not in LAST_NEWS_IDS:
                    LAST_NEWS_IDS.add(uid)
                    new_articles.append(a)
            if new_articles:
                text = "📰 Tin tức thị trường (mới):\n\n"  "\n\n".join(new_articles)
                for chat in list(ALERT_CHAT_IDS):
                    try:
                        await safe_send(context.bot,chat_id=chat, text=text)
                    except Exception:
                        logger.exception("Failed to send hourly market news")
            LAST_NEWS_HOUR = utcnow

        for cid, info in list(MARKET_MAP.items()):
            price = info.get("current_price")
            volq = info.get("vol_quote_24h", 0)
            if not price or volq < MIN_QUOTE_VOL:
                continue

            # existing percent change alert (simple)
            old = PRICE_CACHE.get(cid)
            PRICE_CACHE[cid] = price
            if old is not None and old != 0:
                change = ((price - old) / old) * 100.0
                if abs(change) >= ALERT_THRESHOLD:
                    last = LAST_ALERT.get(cid)
                    if not last or (utcnow - last) >= dt.timedelta(minutes=8):
                        LAST_ALERT[cid] = utcnow
                        # compute short timeframe score
                        df15 = get_ohlc_okx(cid, bar="15m", limit=200)
                        s15, _ = compute_trend_score(df15, mode="long" if change > 0 else "short")
                        msg = (
                            f"🚨 Price Move: {cid} {change:.2f}%\n"
                            f"Price: {price:.8f} | 15m score: {s15:.1f}\n"
                            f"Vol24h ≈ {volq:,.0f}"
                        )
                        for chat in list(ALERT_CHAT_IDS):
                            try:
                                await safe_send(context.bot,chat_id=chat, text=msg)
                            except Exception:
                                logger.exception("Failed to send price change alert")

            # Multi-TF flow detection (immediate alerts to toggled chats)
            sig = await detect_flow_multi_tf(cid)
            if sig and (sig.get("inflow") or sig.get("outflow")):
                tf = sig.get("tf") or "?"
                if tf != "15m":
                    continue  # bỏ qua nếu không phải 15m

                typ = "inflow" if sig.get("inflow") else "outflow"
                key = (cid, tf, typ)   # lúc này tf chắc chắn đã có giá trị

            if not last_sent or (utcnow - last_sent) >= FLOW_IMMEDIATE_COOLDOWN:
                LAST_FLOW_ALERTS[key] = utcnow
                d = sig.get("details", {})
                if typ == "inflow":
                    msg = (
                        f"🔥 [15m] INFLOW đột biến: {cid}\n"
                        f"Vol: {d.get('last_vol'):.0f} | MeanPrev: {d.get('mean_prev_vol'):.0f}\n"
                        f"Δ: {d.get('price_change_pct'):.2f}% | Strength: x{d.get('inflow_strength') or 0:.2f}"
                    )
                else:
                    msg = (
                        f"⚠️ [15m] OUTFLOW đột biến: {cid}\n"
                        f"Vol: {d.get('last_vol'):.0f} | MeanPrev: {d.get('mean_prev_vol'):.0f}\n"
                        f"Δ: {d.get('price_change_pct'):.2f}% | Strength: x{d.get('outflow_strength') or 0:.2f}"
                    )
                for chat in list(ALERT_CHAT_IDS):
                    try:
                        await safe_send(context.bot,chat_id=chat, text=msg)
                    except Exception:
                        logger.exception("Failed to send 15m flow alert")
    except Exception:
        logger.exception("Error in background_price_checker")

async def research_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, mode="long"):
    logger.info(f"📩 Received /research with args: {context.args}")
    chat_id = update.effective_chat.id
    await safe_send(context.bot,chat_id=chat_id, text=f"🔎 Đang quét coins ({mode.upper()})...")


    refresh_markets(MAX_SCAN)
    liquid_syms = [c for c in COINS_LIST if MARKET_MAP.get(c,{}).get("vol_quote_24h",0) >= MIN_QUOTE_VOL]
    liquid_syms = sorted(liquid_syms, key=lambda c: MARKET_MAP[c]["vol_quote_24h"], reverse=True)[:80]

    results = []
    for cid in liquid_syms:
        try:
            avg, details = multi_tf_score(cid, mode=mode)
            # Sửa KeyError: Kiểm tra nếu tf tồn tại trước khi truy cập
            per_tf_ok = all((details.get(tf, {"score": 0})["score"] >= (55 if tf!="1D" else 45)) for tf in ["15m","1H","4H","1D"])
            if not per_tf_ok:
                continue

        # --- D1 Support/Resistance cho coin này ---
            df_d1 = get_ohlc_okx(cid, bar="1D", limit=90)
            sup_d1, res_d1 = compute_support_resistance_from_df(df_d1, window=90)

            df1h = get_ohlc_okx(cid, bar="1H", limit=200)
            pct = percent_change_over_period(df1h, lookback=24) or 0.0
            res, sup = compute_support_resistance(df1h, window=90)
            price = float(df1h.iloc[-1]["close"]) if not df1h.empty else MARKET_MAP.get(cid,{}).get("current_price", 0)

            entry = suggest_entry(details.get("1H",{}).get("inds",{}), price, sup, res, mode=mode)

        # --- DCA & Grid ---
            cfg15 = suggest_dca_future(price, 15, support=sup_d1, resistance=res_d1, direction=mode)
            cfg20 = suggest_dca_future(price, 20, support=sup_d1, resistance=res_d1, direction=mode)
            cfg30 = suggest_dca_future(price, 30, support=sup_d1, resistance=res_d1, direction=mode)
            grid10 = grid_levels(price, support=sup, resistance=res, grids=10)

            results.append({
                "coin": cid,
                "avg_score": round(avg,1),
                "s15": round(details.get("15m", {"score": 0})["score"],1),
                "s1h": round(details.get("1H", {"score": 0})["score"],1),
                "s4h": round(details.get("4H", {"score": 0})["score"],1),
                "s1d": round(details.get("1D", {"score": 0})["score"],1),
                "price": round(price, 8),
                "pct_24h": round(pct, 2),
                "entry": entry,
                "resistance": round(res, 8) if res else None,
                "support": round(sup, 8) if sup else None,
                "volq": MARKET_MAP.get(cid,{}).get("vol_quote_24h",0),
                "dca_15": cfg15,
                "dca_20": cfg20,
                "dca_30": cfg30,
                "grid_10": grid10
            })

        except Exception:
            logger.exception(f"research error for {cid}")
            continue
    results = sorted(results, key=lambda x: (x["avg_score"], x["volq"], abs(x["pct_24h"])), reverse=True)[:25]
    if not results:
        await safe_send(context.bot,
            chat_id=chat_id,
            text="❌ Không tìm thấy coin có xu hướng rõ ràng và thanh khoản đủ.",
            reply_markup=research_choice_markup()
	)
        return


    lines = [f"📊 KẾT QUẢ RESEARCH ({mode.upper()}) — Ưu tiên Thanh khoản & Xu hướng rõ ràng\n━━━━━━━━━━━━━━━━━━━━━"]
    for r in results:
                lines.append(
            f"\n {r['coin']}  | Score(avg):  {r['avg_score']} \n"
            f"🧭 15m/1H/4H/1D:  {r['s15']}/{r['s1h']}/{r['s4h']}/{r['s1d']} \n"
            f"💧 Vol24h≈  {r['volq']:,.0f} USDT \n"
            f"💰 Giá:  {r['price']}  | 24h:  {r['pct_24h']}% \n"
            f"🎯 Entry gợi ý: {r['entry']}\n"
            f"🛑 Kháng cự:  {r['resistance']}  | 🛡️ Hỗ trợ:  {r['support']} \n"
        )
    reply = "\n".join(lines)


    await safe_send(context.bot,
        chat_id=chat_id,
        text=reply,
        parse_mode="HTML",
        reply_markup=research_choice_markup()
    )

async def research_dca_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        # show processing
        await query.edit_message_text("⏳ Scanning for Bot DCA candidates... (demo top coins)")
        results = []
        candidates = filter_dca_candidates(TOP_COINS)
        for coin in candidates:
            df1d = get_ohlc_okx(coin, bar="1D", limit=200)
            if df1d.empty or len(df1d) < 5:
                logger.warning(f"No 1D data for {coin}, skip")
                continue
            price = float(df1d.iloc[-1]["close"])
            # simple filter: 24h growth > 2%
            try:
                prev = float(df1d.iloc[-2]["close"])
                growth_24h = ((price - prev) / prev) * 100 if prev != 0 else 0.0
            except Exception:
                growth_24h = 0.0
            if growth_24h < 2.0:
                continue
            sup = float(df1d["low"].tail(90).min()) if len(df1d) >= 90 else float(df1d["low"].min())
            cfg15 = suggest_dca_future(price, 15, support=sup)
            results.append(f"{coin} | price {price:.6f} | 24h {growth_24h:.2f}% | max_dd {cfg15.get('max_dd_pct','-')}%")
        text = dca_result_to_text(results)
        await query.edit_message_text(text, reply_markup=main_menu(query))
    except Exception as e:
        logger.exception(f"research_dca_handler error: {e}")
        await query.edit_message_text("❌ Bot DCA gặp lỗi.", reply_markup=main_menu(query))

# News handlers: pagination via callback data news:{page}
async def news_page_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        data = query.data or ""
        page = 0
        if data.startswith("news:"):
            try:
                page = int(data.split(":")[1])
            except Exception:
                page = 0
        items, has_more, total_pages = get_news_page(page, NEWS_PAGE_SIZE)
        text = f"📰 Tin tức (Trang {page+1}/{total_pages})\\n\\n" + "\\n\\n".join(items)
        buttons = []
        row = []
        if page > 0:
            row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"news:{page-1}"))
        if has_more:
            row.append(InlineKeyboardButton("Next ➡️", callback_data=f"news:{page+1}"))
        if row:
            buttons.append(row)
        buttons.append([InlineKeyboardButton("🔙 Back", callback_data="main")])
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(buttons))
    except Exception as e:
        logger.exception(f"news_page_handler error: {e}")
        await query.edit_message_text("❌ Lỗi khi lấy tin tức.", reply_markup=main_menu(query))
		
async def text_coin_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    logger.info(f"📩 Received message: {text}")

    # ===== MENU CHÍNH =====
    if text == "🔍 Research":
        # mở menu chọn Long/Short
        await update.message.reply_text(
            "🔎 Chọn chế độ Research:",
            reply_markup=research_choice_markup()
        )
        return

    elif text == "🤖 Bot DCA":
        # mở menu Bot DCA
        await update.message.reply_text(
            "Chọn chế độ lọc Bot DCA: (chỉ Bull)",
            reply_markup=bot_dca_menu()
        )
        return

    elif text == "📊 Top Coins":
        # show top coins
        await update.message.reply_text(
            "🔥 Top Coins theo thanh khoản:",
            reply_markup=coins_page_markup(0)
        )
        return

    elif text == "📰 Tin tức":
        # show tin tức hôm nay
        news_list = get_news_today(limit=10)
        news_text = "📰 Tin tức hôm nay:\n\n" + "\n\n".join(news_list)
        await update.message.reply_text(news_text)
        return

    elif text == "❌ Alerts":
        user_id = update.effective_user.id
        alerts[user_id] = not alerts.get(user_id, False)
        await update.message.reply_text(
            f"🔔 Alerts {'bật' if alerts[user_id] else 'tắt'}"
        )
        return

    # ===== NGƯỜI DÙNG NHẬP COIN =====
    waiting_msg = await update.message.reply_text("⏳ Đang xử lý...")
    try:
        final_text = await analyze_coin(update, context, text)

        for chunk in split_message(final_text):
            await safe_edit(waiting_msg, chunk, parse_mode=None)

    except Exception as e:
        logger.warning(f"Lỗi khi phân tích {text}: {e}")
        await safe_edit(waiting_msg, f"❌ Lỗi khi phân tích {text}")


async def deepcoin_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"📩 Received /deepcoin")
    chat_id = update.effective_chat.id
    if not context.args:
        await safe_send(context.bot, chat_id, "Ví dụ: /deepcoin BTC")
        return
    coin = context.args[0].upper()
    if not coin.endswith("-USDT") and not coin.endswith("-USD"):
        coin = coin + "-USDT"
    waiting_msg = await safe_send(context.bot, chat_id, f"⏳ Đang phân tích sâu cho {coin}...")

    try:
        avg, details = multi_tf_score(coin, mode="long")
        volq = MARKET_MAP.get(coin, {}).get("vol_quote_24h", 0)
        df = get_ohlc_okx(coin, bar="1H", limit=200)
        price = float(df.iloc[-1]["close"]) if not df.empty else MARKET_MAP.get(coin,{}).get("current_price")
        res, sup = compute_support_resistance(df, window=90)
        ai_text = ai_analysis(coin, details, volq, "long")
        news_list = get_news_coin(coin)
        news_text = "📰 Tin tức gần đây:\n"  "\n\n".join(news_list)
        tech_text = (
            f"📊 Tóm tắt kỹ thuật:\n"
            f"- Giá: {price}\n- Kháng cự: {res}\n- Hỗ trợ: {sup}\n"
            f"- Score(15m/1H/4H/1D): {details.get('15m', {'score': 0})['score']:.0f}/{details.get('1H', {'score': 0})['score']:.0f}/{details.get('4H', {'score': 0})['score']:.0f}/{details.get('1D', {'score': 0})['score']:.0f}\n"
        )
        ai_news_text = ai_news_analysis(coin, news_list)

        final_text = news_text + "\n\n" + ai_news_text + "\n\n" + tech_text + "\n\n" + ai_text
        await waiting_msg.edit_text(final_text)
    except Exception as e:
        logger.exception(f"deepcoin_handler error: {e}")
        await waiting_msg.edit_text(f"❌ Lỗi khi phân tích {coin}")


async def refresh_markets_stub():
    """
    Placeholder to populate MARKET_MAP. Replace with your real refresh implementation that fills:
    MARKET_MAP[coin] = {"current_price": float, "vol_quote_24h": float}
    """
    # Example static for testing
    MARKET_MAP.setdefault("BTC-USDT", {})["current_price"] = MARKET_MAP.get("BTC-USDT", {}).get("current_price", 30000.0)
    MARKET_MAP.setdefault("BTC-USDT", {})["vol_quote_24h"] = MARKET_MAP.get("BTC-USDT", {}).get("vol_quote_24h", 500000000.0)
    MARKET_MAP.setdefault("ETH-USDT", {})["current_price"] = MARKET_MAP.get("ETH-USDT", {}).get("current_price", 1800.0)
    MARKET_MAP.setdefault("ETH-USDT", {})["vol_quote_24h"] = MARKET_MAP.get("ETH-USDT", {}).get("vol_quote_24h", 200000000.0)

async def debug_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"🔥 Raw update: {update.to_dict()}")

async def top_coins_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fetch top coins by liquidity (from OKX) and show UI with Home button."""
    refresh_markets(limit=100)
    text = "🔥 Top Coins theo thanh khoản 24h (OKX):"
    await update.message.reply_text(text, reply_markup=coins_page_markup(0))

# ================== HANDLERS: CHART, INDICATORS, AI, NEWS ==================

async def chart_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, coin: str):
    try:
        df = get_ohlc_okx(coin, bar="1H", limit=200)
        if df.empty:
            await update.callback_query.message.reply_text(f"⚠️ Không có dữ liệu chart cho {coin}")
            return
        # đơn giản demo: gửi giá cuối
        price = df.iloc[-1]["close"]
        await update.callback_query.message.reply_text(f"📈 Chart {coin} (demo): giá cuối {price}")
        # TODO: bạn có thể viết create_price_chart vẽ matplotlib chart và gửi bằng send_photo
    except Exception as e:
        logger.exception(f"chart_handler error for {coin}: {e}")
        await update.callback_query.message.reply_text(f"❌ Lỗi khi tạo chart {coin}")


async def indicators_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, coin: str):
    try:
        df = get_ohlc_okx(coin, bar="1H", limit=200)
        if df.empty:
            await update.callback_query.message.reply_text(f"⚠️ Không có dữ liệu indicators cho {coin}")
            return
        inds = _indicators(df)
        text = f"📋 Indicators {coin}:\n{json.dumps(inds, indent=2, ensure_ascii=False)}"
        await update.callback_query.message.reply_text(text)
    except Exception as e:
        logger.exception(f"indicators_handler error for {coin}: {e}")
        await update.callback_query.message.reply_text(f"❌ Lỗi khi tính indicators cho {coin}")


async def ai_analysis(update: Update, context: ContextTypes.DEFAULT_TYPE, coin: str):
    try:
        prompt = f"Phân tích xu hướng giá, rủi ro và cơ hội giao dịch của {coin} trong ngắn hạn và dài hạn."
        summary = ai_summarize(prompt)
        await update.callback_query.message.reply_text(f"🧠 AI Analysis {coin}:\n{summary}")
    except Exception as e:
        logger.exception(f"ai_analysis error for {coin}: {e}")
        await update.callback_query.message.reply_text(f"❌ Lỗi AI analysis cho {coin}")


async def news_market_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global LAST_NEWS_IDS
    try:
        news_list = fetch_all_news(limit=20)  # lấy nhiều rồi lọc
        sent = 0
        out = []
        for n in news_list:
            # dùng title làm id
            nid = n.split("\n")[0]
            if nid in LAST_NEWS_IDS:
                continue
            LAST_NEWS_IDS.add(nid)
            out.append(n)
            sent += 1
            if sent >= 15:  # gửi tối đa 15 tin
                break
        if not out:
            await update.callback_query.message.reply_text("ℹ️ Không có tin mới.")
        else:
            await update.callback_query.message.reply_text("📰 Tin tức thị trường:\n\n" + "\n\n".join(out[:15]), disable_web_page_preview=True)
    except Exception as e:
        logger.exception(f"news_market_handler error: {e}")
        await update.callback_query.message.reply_text("❌ Lỗi khi lấy tin tức thị trường.")

async def analyze_coin(update: Update, context: ContextTypes.DEFAULT_TYPE, symbol: str):
    try:
        results = []
        for bar in ["15m", "1H", "4H", "1D"]:
            df = get_ohlc_okx(f"{symbol.upper()}-USDT", bar=bar, limit=200)
            if df is None or df.empty:
                results.append(f"⚠️ {bar}: Không đủ dữ liệu")
                continue

            score, inds = compute_trend_score(df, mode="long")
            results.append(
                f"{bar}: Score {score}, RSI {inds.get('rsi')}, EMA12 {inds.get('ema12')}, EMA26 {inds.get('ema26')}"
            )

        return "\n".join(results)

    except Exception as e:
        logger.exception(f"analyze_coin error for {symbol}: {e}")
        return f"❌ Lỗi khi phân tích {symbol}"

async def news_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    batch, has_more = get_news_batch(15)

    if not batch:
        await update.message.reply_text("❌ Không tìm thấy tin tức mới.")
        return

    text = "📰 Tin tức thị trường gần đây:\n\n" + "\n\n".join(batch)
    buttons = []
    if has_more:
        buttons.append([InlineKeyboardButton("📩 Xem thêm tin tức", callback_data="news_more")])

    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons), disable_web_page_preview=True)


# ---------- Main startup ----------


def compute_pct_change_24h(symbol: str) -> float | None:
    """Compute approximate 24h percent change using 1D candles (last two closes)."""
    try:
        df = get_ohlc_okx(symbol, bar="1D", limit=3)
        if df.empty or len(df) < 2:
            return None
        last = float(df.iloc[-1]["close"])
        prev = float(df.iloc[-2]["close"])
        return ((last - prev) / prev) * 100.0 if prev != 0 else None
    except Exception:
        return None

def get_top_coins(limit: int = 20) -> list:
    """Return top coins sorted by 24h percent change (desc). If percent change unavailable, fallback to vol."""
    lst = []
    for sym, info in MARKET_MAP.items():
        pct = info.get('pct_change_24h')
        if pct is None:
            pct = compute_pct_change_24h(sym) or 0.0
        lst.append((sym, pct, info.get('vol_quote_24h', 0)))
    lst.sort(key=lambda x: (x[1], x[2]), reverse=True)
    return [x[0] for x in lst[:limit]]


def filter_research_coins(mode: str = "long", top_n: int = 15) -> list:
    """Return list of top_n symbols that have high liquidity and clear trend for given mode.
    Mode: 'long' or 'short'
    """
    candidates = []
    # ensure MARKET_MAP populated
    for symbol, info in sorted(MARKET_MAP.items(), key=lambda kv: kv[1].get('vol_quote_24h', 0), reverse=True):
        try:
            volq = info.get('vol_quote_24h', 0)
            if volq < MIN_QUOTE_VOL:
                continue
            df = get_ohlc_okx(symbol, bar='1H', limit=200)
            if df.empty or len(df) < 30:
                continue
            score, details = compute_trend_score(df, mode=mode)
            # accept if score strongly supports mode
            if mode == 'long' and score >= 40:
                candidates.append((symbol, score, volq))
            elif mode == 'short' and score <= -40:
                candidates.append((symbol, score, volq))
        except Exception:
            continue
    candidates.sort(key=lambda x: (x[2], x[1]), reverse=True)
    return [c[0] for c in candidates[:top_n]]
def build_application() -> Application:
    app = Application.builder().token(TOKEN).build()

    # register handlers
    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(CallbackQueryHandler(news_page_handler, pattern=r"^news:"))
    app.add_handler(CommandHandler("refresh_news", lambda u,c: (ensure_news_buffer(), u.message.reply_text(f"Refreshed: {len(NEWS_BUFFER)} items"))))

    # set bot commands after startup
    async def _post_init(application: Application):
        try:
            cmds = [
                BotCommand("start", "Bắt đầu"),
                BotCommand("refresh_news", "Refill news buffer"),
            ]
            await application.bot.set_my_commands(cmds)
            logger.info("✅ Set bot commands")
        except Exception as e:
            logger.warning(f"set_my_commands failed: {e}")

    app.post_init = _post_init
    return app

def main():
    if not TOKEN:
        logger.error("TELEGRAM_TOKEN not set. Set env var and restart.")
        return

    app = build_application()

    # start optional healthcheck thread
    threading.Thread(target=start_healthcheck_server, args=(8081,), daemon=True).start()

    # start webhook (this will be the foreground process - Railway should keep container alive)
    logger.info("🚀 Starting bot in webhook mode...")
    # ensure webhook url/path configured
    if not PTB_WEBHOOK_URL or not PTB_WEBHOOK_PATH:
        logger.error("PTB_WEBHOOK_URL or PTB_WEBHOOK_PATH is not set. Set env vars accordingly.")
        return

    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=PTB_WEBHOOK_PATH,
        webhook_url=PTB_WEBHOOK_URL,
        drop_pending_updates=True,
    )



def suggest_grid_future(price: float, support: float = None, resistance: float = None, grids: int = 10):
    """Wrapper returning structured grid suggestion used by other code.
    Returns dict with keys grid_levels, grid_step_pct, support, resistance
    """
    levels = grid_levels(price, support=support, resistance=resistance, grids=grids)
    step_pct = 0.0
    if len(levels) >= 2 and levels[0] > 0:
        step_pct = (levels[1] - levels[0]) / levels[0] * 100.0
    return {
        'grid_levels': levels,
        'grid_step_pct': round(step_pct, 4),
        'support': round(levels[0], 8) if levels else None,
        'resistance': round(levels[-1], 8) if levels else None
    }
if __name__ == "__main__":
    main()
