
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

import os
import requests
import logging
import pandas as pd
import pandas_ta as ta
import matplotlib.pyplot as plt
import logging
import threading
from io import BytesIO
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
from flask import Flask
from groq import Groq

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, MessageHandler, ContextTypes, filters
)

CRYPTOPANIC_KEY = "e7e42ec66da05ffb971daa4a81ab716ed3dbcee6"
logger = logging.getLogger(__name__)

# ================== ENV & LOG ==================
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

if not TELEGRAM_TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN not found in .env")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("crypto_bot_opt")

# ================== Fake web for background worker=========================
app = Flask(__name__)

@app.route("/healthz")
def health():
    return "ok", 200

def run_flask():
    app.run(host="0.0.0.0", port=10000)

# Chạy Flask song song với bot Telegram
threading.Thread(target=run_flask, daemon=True).start()

#=================== Hàm AI tóm tắt tin tức bằng Groq LLM============
import os
from groq import Groq

def ai_summarize(prompt: str) -> str:
    """
    Tóm tắt tin tức crypto bằng Groq LLM.
    Cần set biến môi trường GROQ_API_KEY trong Railway.
    """
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        return "⚠️ Chưa cấu hình GROQ_API_KEY, không thể gọi AI."

    try:
        client = Groq(api_key=api_key)
        resp = client.chat.completions.create(
            model="llama-3.1-8b-instruct",
            messages=[
                {"role": "system", "content": "Bạn là trợ lý AI, hãy tóm tắt ngắn gọn tin tức crypto."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=300,
            temperature=0.6
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        return f"❌ Lỗi Groq API: {e}"


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

# Flow detection globals
LAST_HOURLY_INFLOW_ALERT = {}   # key: coin -> datetime of last hourly inflow alert
LAST_IMMEDIATE_OUTFLOW_ALERT = {}  # key: coin -> datetime of last immediate outflow alert (cooldown short)
HOURLY_INFLOW_COOLDOWN = timedelta(hours=1)
IMMEDIATE_OUTFLOW_COOLDOWN = timedelta(minutes=10)

# thresholds (tuneable)
INFLOW_VOL_MULTIPLIER = 3.0   # nếu vol(1H) >= mean(prev 24 x 1H) * 3 -> inflow mạnh
OUTFLOW_VOL_MULTIPLIER = 2.5  # vol spike
OUTFLOW_PRICE_DROP_PCT = -2.0 # trong 1h giảm <= -2% kèm vol spike -> outflow cảnh báo

LAST_NEWS_IDS = set()             # store unique identifiers (urls or titles) already sent
LAST_NEWS_HOUR = None             # last time hourly market news was broadcast (UTC)
NEWS_HOURLY_COOLDOWN = timedelta(hours=1)

# Flow alert dedupe: key = (coin, timeframe, type) -> datetime
LAST_FLOW_ALERTS = {}

# Timeframes priority for immediate alerts (prefer short timeframes)
FLOW_TFS = ["3m", "15m", "1H", "4H"]
FLOW_IMMEDIATE_COOLDOWN = timedelta(minutes=10)  # per (coin,tf,type)

# API Key cho CryptoPanic (nếu có), nếu không có thì để trống -> bot sẽ fallback CoinStats
CRYPTOPANIC_KEY = "e7e42ec66da05ffb971daa4a81ab716ed3dbcee6"
logger = logging.getLogger(__name__)

LAST_ALERT_TIME = {}

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

def compute_trend_score(df: pd.DataFrame, mode: str = "long") -> (float, dict):
    """
    Placeholder score: returns a naive strength based on slope of closes.
    Replace with real indicator computing from your previous file.
    """
    if df is None or df.empty:
        return 0.0, {"signal": None}
    closes = df["close"].astype(float)
    if len(closes) < 3:
        return 0.0, {"signal": None}
    # simple slope percentage over last part
    start = closes.iloc[0]
    end = closes.iloc[-1]
    pct = ((end - start) / start) * 100 if start != 0 else 0.0
    score = max(0.0, min(100.0, pct * 5 + 50))  # make 0-100
    signal = "buy" if pct > 0 else "sell"
    return score, {"signal": signal}


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


# ================== OKX HELPERS ==================
def okx_get_json(url: str, params: dict | None = None, timeout: int = 15):
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; Bot/1.0; +https://github.com/DungofStudent)"
        }
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        j = r.json()
        if j.get("code") not in (None, "0"):
            logger.warning(f"OKX non-zero code: {j}")
        return j
    except Exception as e:
        logger.exception(f"OKX request error: {url} {params} {e}")
        return {}

def refresh_markets(limit: int = MAX_SCAN):
    """
    Populate MARKET_MAP with SWAP USDT instruments, prioritizing by 24h quote volume.
    We fetch both instrument meta and tickers to get current price + 24h volumes.
    """
    try:
        # 1) Instruments (SWAP)
        url_inst = "https://www.okx.com/api/v5/public/instruments"
        inst_j = okx_get_json(url_inst, {"instType": "SWAP"})
        data = inst_j.get("data", []) if inst_j else []
        # 2) Tickers (SWAP)
        url_tickers = "https://www.okx.com/api/v5/market/tickers"
        tick_j = okx_get_json(url_tickers, {"instType": "SWAP"})
        tickers = tick_j.get("data", []) if tick_j else []

        # Map tickers by instId for quick join
        tick_map = {t.get("instId"): t for t in tickers}

        out = {}
        for item in data:
            inst_id = item.get("instId", "")
            # We want *USDT-SWAP* only
            if not inst_id.endswith("USDT-SWAP"):
                continue

            base = item.get("uly")  # underlying (e.g., BTC-USDT)
            if not base or not base.endswith("USDT"):
                continue
            coin_id = base  # e.g., BTC-USDT (without -SWAP)

            t = tick_map.get(inst_id, {})
            # OKX fields: last, bidPx, askPx, vol24h, volCcy24h, high24h, low24h, sodUtc8
            last = t.get("last")
            vol_quote = t.get("volCcy24h")  # quote currency volume (USDT)
            vol_base = t.get("vol24h")      # base volume (contracts)
            try:
                last = float(last) if last is not None else None
            except:
                last = None
            try:
                vol_quote = float(vol_quote) if vol_quote is not None else 0.0
            except:
                vol_quote = 0.0
            try:
                vol_base = float(vol_base) if vol_base is not None else 0.0
            except:
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

        # keep only liquid instruments and take top `limit` by vol_quote_24h
        liquid = sorted(out.values(), key=lambda x: x.get("vol_quote_24h", 0.0), reverse=True)
        liquid = [x for x in liquid if x.get("vol_quote_24h", 0.0) >= 0]  # keep all; volume filter later
        liquid = liquid[:limit]
        # finalize maps
        global MARKET_MAP, COINS_LIST
        MARKET_MAP = {f"{x['base']}-USDT": x for x in liquid}
        COINS_LIST = list(MARKET_MAP.keys())
        logger.info(f"Refreshed markets: {len(COINS_LIST)} USDT SWAP coins (top by 24h quote vol)")

    except Exception:
        logger.exception("refresh_markets error")

def get_ohlc_okx(instId="BTC-USDT", bar="1H", limit=200):
    """
    Return OHLC df with columns: ts, open, high, low, close, vol, volCcy
    (we keep vol/volCcy so downstream can compute flow signals)
    """
    try:
        url = "https://www.okx.com/api/v5/market/candles"
        params = {"instId": instId, "bar": bar, "limit": limit}
        j = okx_get_json(url, params)
        data = j.get("data", []) if j else []
        if not data:
            return pd.DataFrame()
        # OKX returns list of lists: [ts, open, high, low, close, vol, ...]
        # We'll try to parse flexible shape
        df = pd.DataFrame(data)
        # Normalize columns by length: common OKX structure has at least 7 cols
        # We'll map known positions: 0=ts,1=open,2=high,3=low,4=close,5=vol
        df = df.rename(columns={
            0: "ts", 1: "open", 2: "high", 3: "low", 4: "close", 5: "vol"
        })
        # Ensure numeric
        for c in ["open","high","low","close","vol"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        # parse ts (OKX may return ms or epoch string)
        try:
            df["ts"] = pd.to_datetime(df["ts"].astype(float), unit="ms", utc=True)
        except Exception:
            try:
                df["ts"] = pd.to_datetime(df["ts"], utc=True)
            except:
                pass
        df = df.sort_values("ts").reset_index(drop=True)
        return df[["ts","open","high","low","close","vol"]]
    except Exception as e:
        logger.error(f"Error get_ohlc_okx {instId}: {e}")
        return pd.DataFrame()

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

# ================== FLOW DETECTION ==================
async def detect_flow_signals_async(symbol: str, df: pd.DataFrame):
    if len(df) < 2:
        return None

    coin = symbol.upper()
    now = datetime.now(timezone.utc)

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
    Check FLOW_TFS (3m,15m,1H,4H) in priority order. Return:
      { 'inflow': bool, 'outflow': bool, 'tf': tf_string, 'details': {...} }
    Details contains last_vol, mean_prev_vol, price_now, price_change_pct, inflow_strength, outflow_strength
    """
    try:
        for tf in FLOW_TFS:
            # limit smaller for short timeframes
            lim = 25 if tf in ("3m", "15m") else 50
            df = get_ohlc_okx(symbol, bar=tf, limit=lim)
            if df.empty or len(df) < 6:
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
        return {"inflow": False, "outflow": False, "tf": None, "details": {}}
    except Exception:
        logger.exception(f"detect_flow_multi_tf error for {symbol}")
        return {"inflow": False, "outflow": False, "tf": None, "details": {}}

# ================== NEWS API ==================
LAST_NEWS_CACHE = []
LAST_NEWS_FETCH = None
NEWS_CACHE_TTL = timedelta(minutes=10) 

def get_news_general(limit: int = 5):
    global LAST_NEWS_CACHE, LAST_NEWS_FETCH
    now = datetime.now()
    
    # nếu cache còn hạn thì trả về cache
    if LAST_NEWS_FETCH and (now - LAST_NEWS_FETCH) < NEWS_CACHE_TTL:
        return LAST_NEWS_CACHE[:limit]
    
    # Thử lấy từ CryptoPanic trước
    try:
        url = "https://cryptopanic.com/api/v1/posts/"
        params = {"auth_token": CRYPTOPANIC_KEY, "filter": "hot"}
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
            LAST_NEWS_CACHE = out
            LAST_NEWS_FETCH = now
            return out
    except Exception as e:
        logger.warning(f"CryptoPanic error: {e}, dùng fallback CoinStats")
    
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
        today = datetime.utcnow().date()
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
def main_menu():
    keyboard = [
        [InlineKeyboardButton("📊 Top Coins", callback_data="topcoins:0")],
        [InlineKeyboardButton("🔍 Research (Scanner)", callback_data="research_btn")],
        [InlineKeyboardButton("📰 Tin tức thị trường", callback_data="news_market_menu")],
        [InlineKeyboardButton("⚡ Toggle Alerts", callback_data="toggle_alerts")]
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
    await update.message.reply_text("Hello! Use /research to scan coins and /dca <COIN> to get DCA/Grid suggestions.")

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
            df15 = await get_ohlc_okx(coin, bar="15m", limit=200)
            df1h = await get_ohlc_okx(coin, bar="1H", limit=200)
            df4h = await get_ohlc_okx(coin, bar="4H", limit=200)
            df1d = await get_ohlc_okx(coin, bar="1D", limit=200)

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
                f"🔲 Grid: {len(grid_cfg['grid_levels'])-1} grids | step% ≈ {grid_cfg['grid_step_pct']}% | Range: {grid_cfg['support']} ↔ {grid_cfg['resistance']}\n"
            )
            lines.append(line)
        except Exception:
            logger.exception(f"research_command error for {coin}")

    out = "\n\n".join(lines) if lines else "No results"
    await update.message.reply_text(out, parse_mode="HTML", disable_web_page_preview=True)

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
    df1h = await get_ohlc_okx(coin, bar="1H", limit=200)
    df1d = await get_ohlc_okx(coin, bar="1D", limit=200)
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
                    await context.bot.send_message(chat_id=chat_id, text=msg)
    except Exception as e:
        logger.exception(f"scan_alerts error: {e}")

# ================== HANDLERS ==================
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    refresh_markets(MAX_SCAN)
    await update.message.reply_text("👋 Crypto Research Bot (OKX • Liquidity & Trend)", reply_markup=main_menu())

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data.startswith("research:"):
        _, symbol, mode = data.split(":")
        await research_handler(update, context, symbol=symbol, mode=mode)

    chat_id = update.effective_chat.id

    if data == "main":
        await query.edit_message_text("🏠 Menu", reply_markup=main_menu())

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
        await query.edit_message_text(text, reply_markup=coins_page_markup(page))

    elif data.startswith("coin:"):
        coin = data.split(":")[1]
        price = MARKET_MAP.get(coin, {}).get("current_price")
        volq = MARKET_MAP.get(coin, {}).get("vol_quote_24h", 0)
        txt = f"🔎 {coin}\nGiá: {price} USDT\nThanh khoản 24h: ~{volq:,.0f} USDT"
        await context.bot.send_message(chat_id=chat_id, text=txt, reply_markup=coin_actions_markup(coin))

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
            await context.bot.send_message(chat_id=chat_id, text="Không đủ dữ liệu.", reply_markup=coin_actions_markup(coin))
            return
        text = (f"📋 {coin} (1H):\n"
                f"- Close: {inds.get('latest_close')}\n"
                f"- RSI: {inds.get('rsi')}\n"
                f"- EMA12/26: {inds.get('ema12')}/{inds.get('ema26')}\n"
                f"- MACD/MACDs: {inds.get('macd')}/{inds.get('macd_signal')}\n"
                f"- ADX: {inds.get('adx')}\n"
                f"- Signal: {inds.get('signal')}\n")
        await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=coin_actions_markup(coin))

    elif data.startswith("ai:"):
        coin = data.split(":")[1]
        avg, details = multi_tf_score(coin, mode="long")
        volq = MARKET_MAP.get(coin, {}).get("vol_quote_24h", 0)
        ai_text = ai_analysis(coin, details, volq, "long")
        await context.bot.send_message(chat_id=chat_id, text=ai_text, reply_markup=coin_actions_markup(coin))

    elif data == "back_coins":
        await context.bot.send_message(chat_id=chat_id, text="📊 Top Coins (select):", reply_markup=coins_page_markup(0))

    elif data == "toggle_alerts":
        if chat_id in ALERT_CHAT_IDS:
            ALERT_CHAT_IDS.remove(chat_id)
            await context.bot.send_message(chat_id=chat_id, text="⚡ Alerts: OFF", reply_markup=main_menu())
        else:
            ALERT_CHAT_IDS.add(chat_id)
            await context.bot.send_message(chat_id=chat_id, text="⚡ Alerts: ON", reply_markup=main_menu())

    elif data == "research_btn":
        await query.edit_message_text("🔎 Chọn chế độ Research:", reply_markup=research_choice_markup())

    elif data == "news_market_menu":
        news_list = get_news_today(limit=10)
        text = "📰 Tin tức hôm nay:\n\n" + "\n\n".join(news_list)
        await query.message.reply_text(text)

    elif data.startswith("news_menu:"):
        coin = data.split(":")[1]
        await query.edit_message_text("📰 Chọn loại tin tức:", reply_markup=news_menu_markup(coin))

    elif data.startswith("news_market:"):
        coin = data.split(":")[1]
        news_list = get_news_general()
        news_text = "📰 Tin tức thị trường:\n\n" + "\n\n".join(news_list)
        await context.bot.send_message(chat_id=chat_id, text=news_text, reply_markup=news_menu_markup(coin))

    elif data.startswith("news_coin:"):
        coin = data.split(":")[1]
        news_list = get_news_coin(coin)
        news_text = f"💡 Tin tức về {coin}:\n\n" + "\n\n".join(news_list)
        await context.bot.send_message(chat_id=chat_id, text=news_text, reply_markup=news_menu_markup(coin))

    if data == "research_long":
        await research_handler(update, context, mode="long")

    elif data == "research_short":
        await research_handler(update, context, mode="short")

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
        await context.bot.send_message(chat_id=chat_id, text=text)


async def background_price_checker(context: ContextTypes.DEFAULT_TYPE):
    """
    Enhanced background job:
     - refresh markets stub
     - hourly market news broadcast (deduped)
     - detect multi-TF flow signals and send immediate alerts to toggled chats (prioritize m3/m15)
     - existing price move alerts kept
    """
    try:
        await refresh_markets_stub()
        utcnow = datetime.now(timezone.utc)

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
                        await context.bot.send_message(chat_id=chat, text=text)
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
                    if not last or (utcnow - last) >= timedelta(minutes=8):
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
                                await context.bot.send_message(chat_id=chat, text=msg)
                            except Exception:
                                logger.exception("Failed to send price change alert")

            # Multi-TF flow detection (immediate alerts to toggled chats)
            # Multi-TF flow detection (immediate alerts to toggled chats)
            sig = await detect_flow_multi_tf(cid)
            if sig and (sig.get("inflow") or sig.get("outflow")):
                tf = sig.get("tf") or "?"
                if tf != "15m":
                    continue  # chỉ gửi alert nếu tf là 15m
                typ = "inflow" if sig.get("inflow") else "outflow"
                key = (cid, tf, typ)
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
                            await context.bot.send_message(chat_id=chat, text=msg)
                        except Exception:
                            logger.exception("Failed to send 15m flow alert")

    except Exception:
        logger.exception("Error in background_price_checker")

async def research_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, mode="long"):
    chat_id = update.effective_chat.id
    await context.bot.send_message(chat_id=chat_id, text=f"🔎 Đang quét coins ({mode.upper()})...")


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
        await context.bot.send_message(
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
            f"🎯 Entry gợi ý: <code>{r['entry']}</code>\n"
            f"🛑 Kháng cự: <code>{r['resistance']}</code> | 🛡️ Hỗ trợ: <code>{r['support']}</code>\n"
            f"\n🤖 <b>DCA Future suggestions</b> (TP={cfg15['tp_pct']}% | Lev=x{cfg15['leverage']})\n"
            f"• 15 orders: step ≈ {cfg15['avg_step_pct']}% | sức chống chịu ≈ {cfg15['max_drawdown_pct']}%\n"
            f"• 20 orders: step ≈ {cfg20['avg_step_pct']}% | sức chống chịu ≈ {cfg20['max_drawdown_pct']}%\n"
            f"• 30 orders: step ≈ {cfg30['avg_step_pct']}% | sức chống chịu ≈ {cfg30['max_drawdown_pct']}%\n"
            f"🔲 Grid (first 5/last 1 of 10): <code>{','.join(map(str, r['grid_10'][:5]))}...{r['grid_10'][-1]}</code>\n"
        )
    reply = "\n".join(lines)


    await context.bot.send_message(
        chat_id=chat_id,
        text=reply,
        parse_mode="HTML",
        reply_markup=research_choice_markup()
    )


async def deepcoin_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        await context.bot.send_message(chat_id, "Ví dụ: /deepcoin BTC")
        return
    coin = context.args[0].upper()
    if not coin.endswith("-USDT") and not coin.endswith("-USD"):
        coin = coin + "-USDT"
    waiting_msg = await context.bot.send_message(chat_id, f"⏳ Đang phân tích sâu cho {coin}...")

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
    waiting_msg = await context.bot.send_message(chat_id, f"⏳ Đang phân tích sâu cho {coin}...")

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
        await waiting_msg.edit_text(final_text, parse_mode=None)
    except Exception as e:
        logger.exception(f"text_coin_handler error: {e}")
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

# ================== MAIN ==================
def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(CommandHandler("research", research_handler))
    app.add_handler(CommandHandler("deepcoin", deepcoin_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_coin_handler))
    app.add_handler(CallbackQueryHandler(callback_handler))
    logger.info("Bot polling...")

    # background job every 60s
    app.job_queue.run_repeating(background_price_checker, interval=60, first=5)

    app.run_polling()

if __name__ == "__main__":
    main()
