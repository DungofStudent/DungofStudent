
# crypto_research_bot_optimized.py
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


from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
from flask import Flask, request
from urllib.parse import urlencode
import asyncio
from urllib.parse import urlencode, urljoin

from telegram import Bot
from telegram.ext import Application

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, MessageHandler, ContextTypes, filters
)

CRYPTOPANIC_KEY = "e7e42ec66da05ffb971daa4a81ab716ed3dbcee6"
logger = logging.getLogger(__name__)

OKX_BASE = "https://www.okx.com/api/v5"
PROXY = None 

#=================== Hàm AI tóm tắt tin tức bằng Groq LLM==============
from groq import Groq
def ai_summarize(prompt: str) -> str:
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        return "⚠️ Chưa có GROQ_API_KEY."

    try:
        client = Groq(api_key=api_key)
        resp = client.chat.completions.create(
            model="llama3-8b-8192",   # dùng model hợp lệ
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
# ================== GLOBAL STATE ==================
COINS_LIST = []
MARKET_MAP = {}   # key: "BTC-USDT", value: dict(info...)
PAGE_SIZE = 10
PRICE_CACHE = {}
LAST_ALERT = {}
ALERT_CHAT_IDS = set()
ALERT_THRESHOLD = 4.0  # % change between checks to alert
MIN_QUOTE_VOL = 10_000_000  # USDT, 24h quote volume filter (liquidity floor)
MAX_SCAN = 200  # max instruments to scan from OKX

TOKEN = os.getenv("TELEGRAM_TOKEN")  # Đặt trong Render → Environment Variables
PORT = int(os.getenv("PORT", 8080))
WEBHOOK_PATH = f"/webhook/{TOKEN}"
RENDER_URL = os.getenv("RENDER_EXTERNAL_URL")  # ví dụ: https://bot-okxx.onrender.com
WEBHOOK_URL = f"{RENDER_URL}{WEBHOOK_PATH}"
# Flask app
flask_app = Flask(__name__)
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
# ================== Flask routes =====================
#@flask_app.route("/")
#def home():
#    return "✅ Bot is running with Flask + Polling!"
	
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

def compute_trend_score(df: pd.DataFrame) -> tuple[int, str]:
    """
    Trả về (score, trend_type)
    trend_type = 'bullish' | 'bearish'
    """
    if df is None or len(df) < 50:
        return 0, "neutral"

    df = df.copy()
    df["ema20"] = df["close"].ewm(span=20).mean()
    df["ema50"] = df["close"].ewm(span=50).mean()

    # RSI
    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = -delta.where(delta < 0, 0).rolling(14).mean()
    rs = gain / (loss + 1e-9)
    df["rsi"] = 100 - (100 / (1 + rs))

    last = df.iloc[-1]
    score = 0

    if last["ema20"] > last["ema50"]:
        score += 30
    else:
        score -= 30

    if last["rsi"] > 55:
        score += 30
    elif last["rsi"] < 45:
        score -= 30

    if last["close"] > last["ema50"]:
        score += 40
    else:
        score -= 40

    trend_type = "bullish" if score >= 60 else "bearish" if score <= -60 else "neutral"
    return score, trend_type



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

async def text_coin_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"📩 Received message: {update.message.text}")
    waiting_msg = await update.message.reply_text("⏳ Đang xử lý...")

    # Chia nhỏ text
    for chunk in split_message(final_text):
        await waiting_msg.edit_text(chunk, parse_mode=None)   # ✅ hợp lệ vì nằm trong async

async def error_handler(update, context):
    logger.error("Update %s gây lỗi %s", update, context.error)
app.add_error_handler(error_handler)

# ================== OKX HELPERS ==================
def refresh_markets(limit: int = 60):
    """
    Refresh markets: fetch instruments + tickers, filter USDT-SWAP,
    pick top by quote volume and populate global MARKET_MAP and COINS_LIST.
    """
    try:
        inst_data = fetch_instruments_okx()
        tickers = fetch_tickers_okx()
        tick_map = {t.get("instId"): t for t in tickers}

        out = {}
        for item in inst_data:
            inst_id = item.get("instId", "")
            # want USDT-SWAP only
            if not inst_id.endswith("USDT-SWAP"):
                continue
            base = item.get("uly")  # e.g. "BTC-USDT"
            if not base or not base.endswith("USDT"):
                continue
            coin_id = base  # "BTC-USDT"

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

        liquid = sorted(out.values(), key=lambda x: x.get("vol_quote_24h", 0.0), reverse=True)
        liquid = liquid[:limit]

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
                # OKX sometimes returns {"code":"1",...}
                logger.debug(f"okx public non-zero code: {j}")
            return j
        except requests.exceptions.HTTPError as e:
            logger.exception(f"OKX HTTPError: {url} {params} {e}")
            # if final attempt, break and return {}
            time.sleep(1)
        except Exception as e:
            logger.exception(f"OKX request error: {url} {params} {e}")
            time.sleep(1)
    return {}



def get_ohlc_okx(inst_id: str, bar: str = "1H", limit: int = 200):
    endpoint = "/api/v5/market/candles"
    params = {"instId": inst_id, "bar": bar, "limit": limit}
    # try public
    j = okx_get_json(OKX_BASE.rstrip("/") + endpoint, params=params, headers={"User-Agent": "Mozilla/5.0"})
    data = j.get("data", []) if j else []
    if not data:
        logger.debug(f"Public candles empty for {inst_id} -> fallback signed")
        j = okx_get_json_signed(endpoint, params=params, method="GET")
        data = j.get("data", []) if j else []
    # chuẩn hóa thành DataFrame được xử lý ở nơi khác trong cơ sở mã của bạn; ở đây trả về danh sách thô
    return data
		
def detect_flow_signals(coin: str):
    """
    Return dict with:
      { 'inflow': bool, 'inflow_strength': float, 'outflow': bool, 'outflow_strength': float, 'details': {...} }
    Logic:
      - lấy 1H candles limit=25 (last hour + 24 previous 1H)
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

# Lấy danh sách tickers SWAP
def fetch_tickers_okx():
    url = f"{OKX_BASE.rstrip('/')}/market/tickers"
    params = {"instType": "SWAP"}
    j = okx_get_json(url, params=params, headers={"User-Agent": "Mozilla/5.0"})
    if not j or not j.get("data"):
        logger.warning("Public tickers API rỗng → fallback signed")
        # fallback to signed (pass endpoint path)
        j = okx_get_json_signed("/market/tickers" if "/api/v5" in OKX_BASE else "/api/v5/market/tickers", params={"instType": "SWAP"}, method="GET")
    return j.get("data", []) if j else []

# Lấy danh sách instruments SWAP
def fetch_instruments_okx():
    url = f"{OKX_BASE.rstrip('/')}/public/instruments"
    params = {"instType": "SWAP"}
    j = okx_get_json(url, params=params, headers={"User-Agent": "Mozilla/5.0"})
    if not j or not j.get("data"):
        logger.warning("Public instruments API rỗng → fallback signed")
        j = okx_get_json_signed("/public/instruments" if "/api/v5" in OKX_BASE else "/api/v5/public/instruments", params={"instType": "SWAP"}, method="GET")
    return j.get("data", []) if j else []

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
        # Bạn có thể thêm signature + timestamp nếu gọi private endpoints
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
    timestamp = dt.datetime.utcnow().isoformat("T", "milliseconds") + "Z"

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
            ts = dt.datetime.utcnow().isoformat(timespec="milliseconds") + "Z"
            # prehash: timestamp + method + requestPath + body  (OKX expects full path incl. query if present)
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

def start_healthcheck_server():
    import http.server, socketserver

    port = 8080  # cổng phụ cho healthcheck
    class HealthHandler(http.server.SimpleHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/":
                self.send_response(200)
                self.send_header("Content-type", "text/plain")
                self.end_headers()
                self.wfile.write(b"Bot OKX is alive")
            else:
                self.send_error(404)

    with socketserver.TCPServer(("", port), HealthHandler) as httpd:
        logger.info(f"✅ Healthcheck server listening on {port}")
        httpd.serve_forever()

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

# ================== NEWS API ==================
LAST_NEWS_CACHE = []
LAST_NEWS_FETCH = None
NEWS_CACHE_TTL = dt.timedelta(minutes=35) 

def get_news_general(limit: int = 5):
    global LAST_NEWS_CACHE, LAST_NEWS_FETCH
    now = dt.datetime.now()

    # Nếu cache còn hạn thì trả về cache
    if LAST_NEWS_FETCH and (now - LAST_NEWS_FETCH) < NEWS_CACHE_TTL:
        return LAST_NEWS_CACHE[:limit]

    # Thử lấy từ CryptoPanic trước
    try:
        url = "https://cryptopanic.com/api/v1/posts/"
        params = {"auth_token": CRYPTOPANIC_KEY, "filter": "hot"}
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        j = r.json()

        # Nếu trả về quota limit thì bỏ qua và fallback
        if "error" in j or "message" in j and "limit" in j["message"].lower():
            raise RuntimeError("CryptoPanic quota exceeded")

        articles = j.get("results", [])
        out = []
        for a in articles[:limit]:
            title = a.get("title")
            link = a.get("url")
            if title and link:
                out.append(f"- {title}\n🔗 {link}")
        if out:
            LAST_NEWS_CACHE = out
            LAST_NEWS_FETCH = now
            return out
    except Exception:
        logger.warning("CryptoPanic lỗi/quota full → fallback CoinStats")

    # fallback CoinStats
    try:
        url = "https://api.coinstats.app/public/v1/news"
        r = requests.get(url, params={"skip": 0, "limit": limit}, timeout=15)
        r.raise_for_status()
        j = r.json()
        articles = j.get("news", [])
        out = []
        for a in articles:
            title = a.get("title", "")
            link = a.get("link", "")
            if title:
                out.append(f"- {title}\n🔗 {link}")
        if out:
            LAST_NEWS_CACHE = out
            LAST_NEWS_FETCH = now
            return out[:limit]
    except Exception as e:
        logger.exception("CoinStats fallback error")

    # nếu không lấy được gì
    return ["Không lấy được tin tức thị trường."]


def get_news_coin(coin: str, limit: int = 5):
    sym = coin.upper().replace("-USDT", "").replace("-USD", "")
    
    # Thử CryptoPanic
    try:
        url = "https://cryptopanic.com/api/v1/posts/"
        params = {"auth_token": CRYPTOPANIC_KEY, "currencies": sym}
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        j = r.json()
        articles = j.get("results", [])
        out = []
        for a in articles[:limit]:
            title = a.get("title")
            link = a.get("url")
            if title and link:
                out.append(f"- {title}\n🔗 {link}")
        if out:
            return out
    except Exception as e:
        logger.warning(f"CryptoPanic coin news error: {e}, dùng fallback CoinStats")
    
    # fallback CoinStats
    try:
        url = "https://api.coinstats.app/public/v1/news"
        r = requests.get(url, params={"skip": 0, "limit": 20}, timeout=15)
        r.raise_for_status()
        j = r.json()
        articles = j.get("news", [])
        out = []
        for a in articles:
            title = a.get("title", "")
            link = a.get("link", "")
            if sym and title and sym in title.upper():
                out.append(f"- {title}\n🔗 {link}")
        return out[:limit] if out else [f"Không có tin tức mới cho {sym}."]
    except Exception as e:
        logger.exception("CoinStats fallback error")
        return [f"Không lấy được tin tức cho {coin}."]

def get_news_today(limit: int = 10):
    """
    Lấy tin tức thị trường trong ngày từ CoinStats
    """
    try:
        url = "https://api.coinstats.app/public/v1/news"
        r = requests.get(url, params={"skip": 0, "limit": 50}, timeout=15)
        r.raise_for_status()
        j = r.json()
        articles = j.get("news", [])
        today = dt.datetime.utcnow().date()
        out = []
        for a in articles:
            title = a.get("title", "")
            link = a.get("link", "")
            pub_ts = a.get("publishedAt")  # timestamp UTC
            if title and link and pub_ts:
                pub_date = datetime.utcfromtimestamp(pub_ts).date()
                if pub_date == today:
                    out.append(f"- {title}\n🔗 {link}")
            if len(out) >= limit:
                break
        return out if out else ["Không có tin tức hôm nay."]
    except Exception as e:
        logger.exception("CoinStats get_news_today error")
        return ["Không lấy được tin tức hôm nay."]

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

def compute_trend_score(df: pd.DataFrame, mode: str = "long"):
    """
    Composite score (0..100) for 'clarity' of trend.
    - EMA alignment + slope
    - MACD vs signal + histogram sign
    - RSI position (bullish: 50-70; bearish: 30-50)
    - ADX strength (>20)
    """
    if df is None:
        return 0.0, {}
    if df.empty or len(df) < 50:
        return 0.0, {}
    inds = _indicators(df)
    if not inds:
        return 0.0, {}

    d = df.copy().set_index("ts")
    c = d["close"]
    ema12 = d["close"].ewm(span=12).mean()
    ema26 = d["close"].ewm(span=26).mean()
    # slope estimates (per bar)
    slope12 = (ema12.iloc[-1] - ema12.iloc[-5]) / 5.0
    slope26 = (ema26.iloc[-1] - ema26.iloc[-5]) / 5.0

    score = 0.0
    # EMA alignment
    if inds.get("ema12") is not None and inds.get("ema26") is not None:
        if mode == "long" and inds["ema12"] > inds["ema26"]:
            score += 25
        if mode == "short" and inds["ema12"] < inds["ema26"]:
            score += 25
    # slopes
    if mode == "long" and slope12 > 0 and slope26 > 0:
        score += 20
    if mode == "short" and slope12 < 0 and slope26 < 0:
        score += 20
    # MACD
    macd = inds.get("macd"); sig = inds.get("macd_signal"); hist = inds.get("macd_hist")
    if macd is not None and sig is not None and hist is not None:
        if mode == "long" and macd > sig and hist > 0:
            score += 25
        if mode == "short" and macd < sig and hist < 0:
            score += 25
    # RSI
    rsi = inds.get("rsi")
    if rsi is not None:
        if mode == "long" and 50 <= rsi <= 70:
            score += 15
        if mode == "short" and 30 <= rsi <= 50:
            score += 15
    # ADX
    adx = inds.get("adx")
    if adx is not None and adx >= 20:
        score += 15

    score = max(0.0, min(100.0, score))
    return score, inds

def multi_tf_score(coin: str, bars=("15m","1H","4H","1D"), mode="long"):
    """
    Compute average score across multiple timeframes. Require alignment (all > threshold).
    Returns (avg_score, details) where details has per-tf score and indicators.
    """
    details = {}
    scores = []
    for b in bars:
        df = get_ohlc_okx(coin, b, limit=200 if b != "1D" else 400)
        s, inds = compute_trend_score(df, mode=mode)
        details[b] = {"score": s, "inds": inds}
        scores.append(s if s is not None else 0.0)
    avg = sum(scores)/len(scores) if scores else 0.0
    return avg, details

# ================== CHART ==================
def create_price_chart(df, coin, title_suffix="(OKX)"):
    buf = BytesIO()
    if df.empty:
        return buf
    plt.figure(figsize=(8,4))
    plt.plot(df["ts"], df["close"], label=f"{coin} close")
    plt.title(f"{coin} Price {title_suffix}")
    plt.xlabel("Date")
    plt.ylabel("Price")
    plt.legend()
    plt.tight_layout()
    plt.savefig(buf, format="png")
    buf.seek(0)
    plt.close()
    return buf

# ================== AI TEXT ==================
def ai_analysis(coin: str, tf_details: dict, vol_quote_24h: float, mode: str):
    """
    Build a friendly Vietnamese analysis string from indicators and trend scores.
    """
    lines = [f"🤖 Nhận định AI cho {coin} ({mode.upper()}):"]
    try:
        s15 = tf_details.get("15m",{}).get("score",0)
        s1h = tf_details.get("1H",{}).get("score",0)
        s4h = tf_details.get("4H",{}).get("score",0)
        s1d = tf_details.get("1D",{}).get("score",0)
        lines.append(f"- Điểm xu hướng: 15m={s15:.0f} | 1H={s1h:.0f} | 4H={s4h:.0f} | 1D={s1d:.0f}")
        lines.append(f"- Thanh khoản 24h ước tính: ~{vol_quote_24h:,.0f} USDT")
        key_tf = "1H"
        inds = tf_details.get(key_tf,{}).get("inds",{})
        rsi = inds.get("rsi"); adx = inds.get("adx"); ema12 = inds.get("ema12"); ema26 = inds.get("ema26")
        macd = inds.get("macd"); sig = inds.get("macd_signal")
        if rsi is not None:
            if rsi > 70: lines.append(f"- RSI(1H) {rsi:.1f} → có dấu hiệu quá mua, cẩn trọng chốt lời.")
            elif rsi < 30: lines.append(f"- RSI(1H) {rsi:.1f} → quá bán, dễ có hồi kỹ thuật.")
            else: lines.append(f"- RSI(1H) {rsi:.1f} → vùng trung tính/hỗ trợ xu hướng hiện tại.")
        if adx is not None:
            lines.append(f"- ADX(1H) {adx:.1f} → {'xu hướng mạnh' if adx>=20 else 'xu hướng yếu/chưa rõ'}.")
        if macd is not None and sig is not None:
            lines.append(f"- MACD(1H) {'>' if macd>sig else '<'} Signal → {'đồng thuận' if (mode=='long' and macd>sig) or (mode=='short' and macd<sig) else 'chưa đồng thuận'}.")
        if ema12 is not None and ema26 is not None:
            lines.append(f"- EMA12 {'>' if ema12>ema26 else '<'} EMA26 → {'thuận xu hướng' if (mode=='long' and ema12>ema26) or (mode=='short' and ema12<ema26) else 'ngược xu hướng'}.")
        lines.append("⚠️ Đây không phải lời khuyên đầu tư. Hãy đặt dừng lỗ và quản trị rủi ro.")
    except Exception:
        lines.append("Không đủ dữ liệu để phân tích chi tiết.")
    return "\n".join(lines)

def ai_news_analysis(coin: str, news_list: list) -> str:

    if not news_list:
        return "🧠 Không có tin tức để AI phân tích."
    
    # Ví dụ đơn giản: kết hợp các tin thành prompt
    prompt = f"Phân tích các tin tức gần đây về {coin}:\n" + "\n".join(news_list)
    
    # Gọi hàm AI của bạn (giả sử ai_text = ai_summarize(prompt))
    try:
        ai_text = ai_summarize(prompt)  # đây là hàm AI của bạn
        return f"🧠 Phân tích AI từ tin tức:\n{ai_text}"
    except Exception as e:
        logger.exception(f"AI news analysis error: {e}")
        return "🧠 Lỗi khi phân tích tin tức bằng AI."


# ================== UI (Telegram) ==================
def main_menu(user_id: int) -> InlineKeyboardMarkup:
    state = "ON ✅" if alerts.get(user_id, False) else "OFF ❌"
    keyboard = [
        [InlineKeyboardButton("📊 Top Coins", callback_data="topcoins:0")],
        [InlineKeyboardButton("🔍 Research (Scanner)", callback_data="research_btn")],
        [InlineKeyboardButton("🤖 Bot DCA", callback_data="bot_dca_btn")],
        [InlineKeyboardButton("📰 Tin tức thị trường", callback_data="news_market_menu")],
        [InlineKeyboardButton(f"⚡ Toggle Alerts: {state}", callback_data="toggle_alert")]
    ]
    return InlineKeyboardMarkup(keyboard)

def research_choice_markup():
    keyboard = [
        [InlineKeyboardButton("📈 Long (Xu hướng tăng)", callback_data="research_long")],
        [InlineKeyboardButton("📉 Short (Xu hướng giảm)", callback_data="research_short")],
        [InlineKeyboardButton("🏠 Main Menu", callback_data="main")]
    ]
    return InlineKeyboardMarkup(keyboard)

def coins_page_markup(page:int):
    start = page * PAGE_SIZE
    end = start + PAGE_SIZE
    items = COINS_LIST[start:end]
    keyboard = [[InlineKeyboardButton(c, callback_data=f"coin:{c}")] for c in items]
    nav = []
    if page > 0: nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"topcoins:{page-1}"))
    if end < len(COINS_LIST): nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"topcoins:{page+1}"))
    if nav: keyboard.append(nav)
    keyboard.append([InlineKeyboardButton("🏠 Main Menu", callback_data="main")])
    return InlineKeyboardMarkup(keyboard)

def coin_actions_markup(coin_id):
    keyboard = [
        [InlineKeyboardButton("📈 Chart (1D)", callback_data=f"chart:{coin_id}")],
        [InlineKeyboardButton("📋 Indicators (1H)", callback_data=f"ind:{coin_id}")],
        [InlineKeyboardButton("🤖 AI Research", callback_data=f"ai:{coin_id}")],
        [InlineKeyboardButton("📰 Tin tức", callback_data=f"news_menu:{coin_id}")],
        [InlineKeyboardButton("⬅️ Back", callback_data="back_coins")]
    ]
    return InlineKeyboardMarkup(keyboard)

def news_menu_markup(coin_id):
    keyboard = [
        [InlineKeyboardButton("📰 Diễn biến thị trường", callback_data=f"news_market:{coin_id}")],
        [InlineKeyboardButton("💡 Diễn biến coin", callback_data=f"news_coin:{coin_id}")],
        [InlineKeyboardButton("⬅️ Back", callback_data=f"coin:{coin_id}")]
    ]
    return InlineKeyboardMarkup(keyboard)



# ================== HELPERS ==================
def percent_change_over_period(df: pd.DataFrame, lookback: int = 24):
    if df.empty or len(df) <= lookback:
        return None
    current = float(df.iloc[-1]['close'])
    prev = float(df.iloc[-1 - lookback]['close'])
    if prev == 0:
        return None
    return ((current - prev) / prev) * 100.0

def compute_support_resistance(df: pd.DataFrame, window: int = 50):
    if df.empty:
        return (None, None)
    d = df.copy().reset_index(drop=True)
    tail = d.iloc[-window:] if len(d) >= window else d
    resistance = float(tail['high'].max())
    strong_support = float(tail['low'].min())
    return resistance, strong_support

def suggest_entry(indicators: dict, price: float, support: float, resistance: float, mode="long"):
    try:
        ema12 = indicators.get("ema12")
        ema26 = indicators.get("ema26")
        if ema12 and ema26 and support and resistance:
            if mode == "long" and ema12 > ema26:
                return round(max(support, price*0.995), 8)
            if mode == "short" and ema12 < ema26:
                return round(min(resistance, price*1.005), 8)
    except Exception:
        pass
    return round(price, 8)

def dca_levels(price: float, num_orders: int = 15, total_range_pct: float = 0.15):
    """
    Suggest DCA levels: linear steps from current price down to price*(1 - total_range_pct).
    Returns list of floats (levels) length = num_orders ordered from nearest to farthest (descending).
    """
    if price is None or price <= 0:
        return []
    bottom = price * (1 - total_range_pct)
    steps = []
    for i in range(1, num_orders + 1):
        level = price - (i / num_orders) * (price - bottom)
        steps.append(round(level, 8))
    return steps

def grid_levels(price: float, support: float = None, resistance: float = None, grids: int = 10):
    """
    Suggest grid levels for futures grid bot.
    If support/resistance provided, generate grids between them; otherwise use +/-5% around price.
    Returns list of grid prices (ascending).
    """
    if price is None or price <= 0:
        return []
    if support and resistance and resistance > support:
        low = support
        high = resistance
    else:
        # default 5% each side
        low = price * 0.95
        high = price * 1.05
    levels = []
    for i in range(grids + 1):
        lvl = low + (i / grids) * (high - low)
        levels.append(round(lvl, 8))
    return levels

# ================== BOT DCA & GRID FUTURE SUGGESTIONS ==================
from typing import Optional

def suggest_dca_future(price: float, num_orders: int, support: Optional[float] = None,
                       resistance: Optional[float] = None, direction: str = "long"):
    ...
    if not price or price <= 0:
        return {}

    leverage = 2   # mặc định x2
    tp_pct = 0.37

    # Tính % drawdown tới hỗ trợ / kháng cự cứng D1
    max_dd_pct = 0.0
    if support and support < price:
        max_dd_pct = ((price - support) / price) * 100.0
    elif resistance and resistance < price:
        max_dd_pct = ((price - resistance) / price) * 100.0
    else:
        max_dd_pct = 15.0  # fallback giả định

    avg_step_pct = max_dd_pct / num_orders if num_orders > 0 else 0.0

    steps = []
    for i in range(num_orders):
        entry_price = price * (1 - avg_step_pct/100 * (i+1))
        steps.append({
            "order": i+1,
            "price": round(entry_price, 6),
            "step_pct": round(avg_step_pct, 4)
        })

    return {
        "type": f"DCA Future ({num_orders} safety orders)",
        "price_now": round(price, 6),
        "tp_pct": tp_pct,
        "leverage": leverage,
        "avg_step_pct": round(avg_step_pct, 4),
        "max_drawdown_pct": round(max_dd_pct, 2),
        "steps": steps
    }


def suggest_grid_future(price: float, support: Optional[float] = None, resistance: Optional[float] = None, grids: int = 10):
    leverage = 20
    tp_pct = 0.37

    if not support or not resistance or resistance <= support:
        support = price * 0.95
        resistance = price * 1.05

    step_pct = ((resistance - support) / support) / grids * 100

    levels = []
    for i in range(grids + 1):
        lvl = support + (i / grids) * (resistance - support)
        levels.append(round(lvl, 6))

    return {
        "type": f"Grid Future ({grids} grids)",
        "price_now": price,
        "tp_pct": tp_pct,
        "leverage": leverage,
        "grid_levels": levels,
        "grid_step_pct": round(step_pct, 3),
        "support": support,
        "resistance": resistance
    }



# === Bot commands & handlers ===
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start_handler(update, context)

async def research_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /research [coin1 coin2 ...] - quick research for given coins or defaults in MARKET_MAP
    This function will:
      - compute simple scores for multiple timeframes
      - compute support/resistance D1
      - produce DCA (15/20/30) + Grid suggestion (10)
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
                f"<b>{coin}</b> | Score(avg): <b>{avg_score}</b>\n"
                f"15m/1H/4H/1D: <code>{int(s15)}/{int(s1h)}/{int(s4h)}/{int(s1d)}</code>\n"
                f"Price: <code>{price:.8f}</code> | 1DΔ: <code>{pct_24h:.2f}%</code>\n"
                f"Support(D1): <code>{sup_d1}</code> | Resistance(D1): <code>{res_d1}</code>\n"
                f"🤖 DCA Future (TP={cfg15['tp_pct']}%, Lev=x{cfg15['leverage']}):\n"
                f" • Base step%: {cfg15['base_step_pct']}% | Money×: {cfg15['money_multiplier']} | Step×: {cfg15['step_multiplier']}\n"
                f" • MaxDrawdownNeeded ≈ <code>{cfg15['max_drawdown_pct']}%</code>\n"
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

async def research_dca_bot(update: Update, context: ContextTypes.DEFAULT_TYPE, mode: str = "all"):
    """
    Quét các coin có thanh khoản cao + xu hướng rõ ràng để gợi ý bot DCA.
    mode = all | bull | bear
    """
    lines = []
    for coin, info in MARKET_MAP.items():
        try:
            price = info.get("current_price")
            volq = info.get("vol_quote_24h", 0)
            if not price or volq < 10_000_000:
                continue

            df1d = get_ohlc_okx(coin, bar="1D", limit=200)
            if df1d.empty:
                continue

            trend_score, trend_type = compute_trend_score(df1d)

            # lọc theo mode
            if mode == "bull" and trend_type != "bullish":
                continue
            if mode == "bear" and trend_type != "bearish":
                continue
            if mode == "all" and abs(trend_score) < 60:
                continue

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

            text = (
                f"🔎 <b>{coin}</b>\n"
                f"Giá hiện tại: <code>{price:.4f}</code>\n"
                f"Support gần nhất: <code>{sup:.4f}</code>\n"
                f"Max Drawdown: <code>{max_dd_pct:.2f}%</code>\n"
                f"Trend Score: <b>{trend_score}</b> ({trend_type})\n\n"
                f"➡️ Bước giá gợi ý:\n"
                f"- 15 lệnh: {step15}% | Ký quỹ ≈ {kq15}\n"
                f"- 20 lệnh: {step20}% | Ký quỹ ≈ {kq20}\n"
                f"- 30 lệnh: {step30}% | Ký quỹ ≈ {kq30}\n"
                f"(margin x20, step×=0.97, money×=1.05)\n"
                "——————————————"
            )
            lines.append(text)

        except Exception as e:
            logger.error(f"DCA research error {coin}: {e}")
            continue

    out = "\n\n".join(lines) if lines else "❌ Không tìm thấy coin phù hợp."
    chat_id = update.effective_chat.id
    await safe_send(context.bot, chat_id=chat_id, text=out, parse_mode="HTML", disable_web_page_preview=True)


async def dca_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /dca <COIN>
    Prints full DCA and Grid suggestions for a coin
    """
    args = context.args or []
    if not args:
        await update.message.reply_text("Usage: /dca COIN (e.g. /dca BTC-USDT)")
        return
    coin = args[0].upper()
    # fetch price + SR D1
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
    logger.info(f"📩 Received /start from user {update.effective_user.id}")
    refresh_markets(MAX_SCAN)
    user_id = update.effective_user.id
    await update.message.reply_text(
        "👋 Crypto Research Bot (OKX • Liquidity & Trend)",
        reply_markup=main_menu(user_id)
    )


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"📩 Received callback: {update.callback_query.data}")
    query = update.callback_query
    data = query.data
    chat_id = update.effective_chat.id   # lấy chat id

    if data.startswith("research:"):
        _, symbol, mode = data.split(":")
        await research_handler(update, context, symbol=symbol, mode=mode)

    elif data == "main":
        user_id = update.effective_user.id
        await safe_edit(
            update.callback_query.message,
            text="🏠 Menu",
            reply_markup=main_menu(user_id)
        )

    elif data.startswith("topcoins:"):
        page = int(data.split(":")[1])
        # sort by liquid first
        liquid_sorted = sorted(COINS_LIST, key=lambda c: MARKET_MAP.get(c,{}).get("vol_quote_24h",0), reverse=True)
        # rebuild page list
        start = page*PAGE_SIZE
        end = start+PAGE_SIZE
        subset = liquid_sorted[start:end]
        # temporarily override COINS_LIST view for page
        text = "📊 Top Coins theo thanh khoản (24h):\n"
        for c in subset:
            v = MARKET_MAP.get(c,{}).get("vol_quote_24h",0)
            text += f"- {c}: ~{v:,.0f} USDT\n"
        await safe_edit(update.callback_query.message, text=text, reply_markup=coins_page_markup(page))

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
        _, inds = compute_trend_score(df, mode="long")  # returns score + inds
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

    elif data == "research_btn":
        await safe_edit(update.callback_query.message, text="🔎 Chọn chế độ Research:", reply_markup=research_choice_markup())
		
    elif data == "news_market_menu":
        news_list = get_news_today(limit=10)
        text = "📰 Tin tức hôm nay:\n\n" + "\n\n".join(news_list)
        await query.message.reply_text(text)

    elif data.startswith("news_menu:"):
        coin = data.split(":")[1]
        await safe_edit(update.callback_query.message, "📰 Chọn loại tin tức:", reply_markup=news_menu_markup(coin))

    elif data.startswith("news_market:"):
        coin = data.split(":")[1]
        news_list = get_news_general()
        news_text = "📰 Tin tức thị trường:\n\n" + "\n\n".join(news_list)
        await safe_send(context.bot,chat_id=chat_id, text=news_text, reply_markup=news_menu_markup(coin))

    elif data.startswith("news_coin:"):
        coin = data.split(":")[1]
        news_list = get_news_coin(coin)
        news_text = f"💡 Tin tức về {coin}:\n\n" + "\n\n".join(news_list)
        await safe_send(context.bot,chat_id=chat_id, text=news_text, reply_markup=news_menu_markup(coin))

    if data == "research_long":
        await research_handler(update, context, mode="long")

    elif data == "research_short":
        await research_handler(update, context, mode="short")

    elif data == "bot_dca_btn":
        keyboard = [
            [InlineKeyboardButton("📈 Xu hướng Tăng", callback_data="bot_dca_bull")],
            [InlineKeyboardButton("📉 Xu hướng Giảm", callback_data="bot_dca_bear")],
            [InlineKeyboardButton("🔎 Tất cả", callback_data="bot_dca_all")],
            [InlineKeyboardButton("⬅️ Quay lại", callback_data="main_menu")]
        ]
        await update.callback_query.message.edit_text(
            "Chọn chế độ lọc Bot DCA:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif data == "bot_dca_bull":
        await research_dca_bot(update, context, mode="bull")
    elif data == "bot_dca_bear":
        await research_dca_bot(update, context, mode="bear")
    elif data == "bot_dca_all":
        await research_dca_bot(update, context, mode="all")


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
            f"⚙️ DCA & Grid suggestions for {coin}\n"
            f"Price: {price}\n\n"
            f"DCA (15 orders):\n{', '.join(map(str,d15))}\n\n"
            f"DCA (20 orders):\n{', '.join(map(str,d20))}\n\n"
            f"DCA (30 orders):\n{', '.join(map(str,d30))}\n\n"
            f"Grid (10):\n{', '.join(map(str,grid10))}\n"
        )
        await safe_send(context.bot,chat_id=chat_id, text=text)

import datetime as dt
async def background_price_checker(context: ContextTypes.DEFAULT_TYPE):
    global last_sent
    utcnow = dt.datetime.now(dt.timezone.utc)


# Khi gán last_sent cũng phải dùng aware datetime
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
                articles = get_news_general(limit=10) or []
            except Exception:
                articles = []
            new_articles = []
            for a in articles:
                uid = a  # fallback: use full string as ID (title+link). If you have an URL field, use that instead.
                if uid not in LAST_NEWS_IDS:
                    LAST_NEWS_IDS.add(uid)
                    new_articles.append(a)
            if new_articles:
                text = "📰 Tin tức thị trường (mới):\n\n" + "\n\n".join(new_articles)
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
            # Multi-TF flow detection (immediate alerts to toggled chats)
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
            per_tf_ok = all((details[tf]["score"] >= (55 if tf!="1D" else 45)) for tf in ["15m","1H","4H","1D"])
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
                "s15": round(details["15m"]["score"],1),
                "s1h": round(details["1H"]["score"],1),
                "s4h": round(details["4H"]["score"],1),
                "s1d": round(details["1D"]["score"],1),
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
            f"\n<b>{r['coin']}</b> | Score(avg): <b>{r['avg_score']}</b>\n"
            f"🧭 15m/1H/4H/1D: <code>{r['s15']}/{r['s1h']}/{r['s4h']}/{r['s1d']}</code>\n"
            f"💧 Vol24h≈ <code>{r['volq']:,.0f} USDT</code>\n"
            f"💰 Giá: <code>{r['price']}</code> | 24h: <code>{r['pct_24h']}%</code>\n"
            f"🎯 Entry gợi ý: {r['entry']}\n"
            f"🛑 Kháng cự: <code>{r['resistance']}</code> | 🛡️ Hỗ trợ: <code>{r['support']}</code>\n"
        )
    reply = "\n".join(lines)


    await safe_send(context.bot,
        chat_id=chat_id,
        text=reply,
        parse_mode="HTML",
        reply_markup=research_choice_markup()
    )


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
        news_text = "📰 Tin tức gần đây:\n" + "\n\n".join(news_list)
        tech_text = (
            f"📊 Tóm tắt kỹ thuật:\n"
            f"- Giá: {price}\n- Kháng cự: {res}\n- Hỗ trợ: {sup}\n"
            f"- Score(15m/1H/4H/1D): {details['15m']['score']:.0f}/{details['1H']['score']:.0f}/{details['4H']['score']:.0f}/{details['1D']['score']:.0f}\n"
        )
        ai_news_text = ai_news_analysis(coin, news_list)

        final_text = news_text + "\n\n" + ai_news_text + "\n\n" + tech_text + "\n\n" + ai_text
        await waiting_msg.edit_text(final_text)
    except Exception as e:
        logger.exception(f"deepcoin_handler error: {e}")
        await waiting_msg.edit_text(f"❌ Lỗi khi phân tích {coin}")

async def text_coin_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    text = update.message.text.strip().upper()
    if not text.endswith("-USDT") and not text.endswith("-USD"):
        text = text + "-USDT"
    coin = text
    waiting_msg = await safe_send(context.bot, chat_id=chat_id, text=f"⏳ Đang phân tích sâu cho {coin}...")

    try:
        avg, details = multi_tf_score(coin, mode="long")
        df = get_ohlc_okx(coin, bar="1H", limit=200)
        price = float(df.iloc[-1]["close"]) if not df.empty else MARKET_MAP.get(coin,{}).get("current_price", 0)
        res, sup = compute_support_resistance(df, window=90)
        entry = suggest_entry(details.get("1H",{}).get("inds",{}), price, sup, res, mode="long")
        text_out = (
            f"📊 <b>{coin}</b>\n"
            f"💰 Giá hiện tại: <code>{round(price, 8)}</code>\n"
            f"🧭 Score(15m/1H/4H/1D): <code>{details['15m']['score']:.0f}/{details['1H']['score']:.0f}/{details['4H']['score']:.0f}/{details['1D']['score']:.0f}</code>\n"
            f"🎯 Entry gợi ý: <code>{entry}</code>\n"
            f"🛑 Resistance: <code>{round(res, 8) if res else 'N/A'}</code>\n"
            f"🛡️ Support: <code>{round(sup, 8) if sup else 'N/A'}</code>\n"
        )
        volq = MARKET_MAP.get(coin, {}).get("vol_quote_24h", 0)
        ai_text = ai_analysis(coin, details, volq, "long")
        news_list = get_news_coin(coin)
        news_text = "📰 Tin tức liên quan:\n" + "\n\n".join(news_list)
        ai_news_text = ai_news_analysis(coin, news_list)
        final_text = text_out + "\n\n" + news_text + "\n\n" + ai_news_text + "\n\n" + ai_text
        if waiting_msg:
            await waiting_msg.edit_text(final_text, parse_mode=None)
        else:
            await safe_send(context.bot, chat_id=chat_id, text=final_text, parse_mode=None)
    except Exception as e:
        logger.exception(f"text_coin_handler error: {e}")
        err_text = f"❌ Lỗi khi phân tích {coin}"
        if waiting_msg:
            await waiting_msg.edit_text(err_text)
        else:
            await safe_send(context.bot, chat_id=chat_id, text=err_text)


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

# ================== MAIN ==================
def main():
    app = Application.builder().token(TOKEN).build()

    # Handlers
    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(CommandHandler("research", research_handler))
    app.add_handler(CommandHandler("deepcoin", deepcoin_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_coin_handler))
    app.add_handler(CallbackQueryHandler(callback_handler))

    # Reset webhook trước khi run
    app.post_init(reset_webhook)

    logger.info(f"🔗 Setting webhook to: {WEBHOOK_URL}")

    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=TOKEN,           # chỉ token
        webhook_url=WEBHOOK_URL,  # full URL có /webhook/<TOKEN>
    )
	
if __name__ == "__main__":
    logger.info("🚀 Starting bot in webhook mode...")
    threading.Thread(target=start_healthcheck_server, daemon=True).start()
    main()

