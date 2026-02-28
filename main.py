import os
import asyncio
import aiohttp
import sqlite3
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Tuple, Dict

import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from apscheduler.schedulers.background import BackgroundScheduler

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
TD_API_KEY = os.getenv("TD_API_KEY", "")

AUTO_INTERVAL_MINUTES = int(os.getenv("AUTO_INTERVAL_MINUTES", "15"))
MONITOR_INTERVAL_MINUTES = int(os.getenv("MONITOR_INTERVAL_MINUTES", "5"))
HEARTBEAT_INTERVAL_MINUTES = int(os.getenv("HEARTBEAT_INTERVAL_MINUTES", "180"))
MIN_SIGNALS_PER_DAY = int(os.getenv("MIN_SIGNALS_PER_DAY", "3"))

# ✅ авто-анализ можно оставить в сессию, а /now и кнопка будут работать всегда
SESSION_START_UTC = int(os.getenv("SESSION_START_UTC", "7"))
SESSION_END_UTC = int(os.getenv("SESSION_END_UTC", "21"))

NEWS_BLACKOUT_UTC = os.getenv("NEWS_BLACKOUT_UTC", "").strip()

INSTRUMENTS = ["EUR/USD", "XAU/USD"]

TF_4H = "4h"
TF_1H = "1h"
TF_15 = "15min"
TF_5 = "5min"
TF_MONITOR = "5min"

N_4H = 350
N_1H = 650
N_15 = 900
N_5 = 1200
N_MONITOR = 450

DB_PATH = "bot.db"

MODE_PARAMS: Dict[str, dict] = {
    "STRICT": {
        "score_base": 9.3, "rr": 2.10, "sl_atr_mult": 1.90,
        "cooldown_min": 150, "breakout_n": 8, "impulse_k": 1.05,
        "vol_thr_1h": 0.00050, "rsi_long_min": 56, "rsi_short_max": 44,
        "pullback_dist_atr": 0.45, "cont_dist_atr": 0.35, "range_n": 10,
    },
    "BALANCED": {
        "score_base": 8.3, "rr": 1.85, "sl_atr_mult": 1.65,
        "cooldown_min": 120, "breakout_n": 5, "impulse_k": 0.85,
        "vol_thr_1h": 0.00038, "rsi_long_min": 52, "rsi_short_max": 50,
        "pullback_dist_atr": 0.60, "cont_dist_atr": 0.55, "range_n": 8,
    },
    "FLOW": {
        "score_base": 7.6, "rr": 1.60, "sl_atr_mult": 1.45,
        "cooldown_min": 55, "breakout_n": 2, "impulse_k": 0.45,
        "vol_thr_1h": 0.00018, "rsi_long_min": 46, "rsi_short_max": 56,
        "pullback_dist_atr": 1.20, "cont_dist_atr": 1.10, "range_n": 6,
    },
}

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def in_session_window(now_utc: datetime) -> bool:
    return SESSION_START_UTC <= now_utc.hour < SESSION_END_UTC

def parse_blackouts(s: str) -> List[Tuple[datetime, datetime]]:
    out = []
    if not s:
        return out
    parts = [p.strip() for p in s.split(",") if p.strip()]
    for p in parts:
        if "/" not in p:
            continue
        a, b = p.split("/", 1)
        try:
            start = datetime.fromisoformat(a.replace("Z", "+00:00")).astimezone(timezone.utc)
            end = datetime.fromisoformat(b.replace("Z", "+00:00")).astimezone(timezone.utc)
            if end > start:
                out.append((start, end))
        except Exception:
            pass
    return out

BLACKOUTS = parse_blackouts(NEWS_BLACKOUT_UTC)

def in_blackout(now_utc: datetime) -> bool:
    for a, b in BLACKOUTS:
        if a <= now_utc <= b:
            return True
    return False

def sha16(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]

def digits_for(symbol: str) -> int:
    return 5 if "EUR" in symbol else 2

def price_sanity_ok(symbol: str, price: float) -> bool:
    if symbol == "EUR/USD":
        return 0.8 <= price <= 1.6
    if symbol == "XAU/USD":
        return 1000 <= price <= 10000
    return True

# =========================
# DB
# =========================
def db_init():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS signals (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      created_utc TEXT NOT NULL,
      symbol TEXT NOT NULL,
      direction TEXT NOT NULL,
      entry REAL NOT NULL,
      sl REAL NOT NULL,
      tp REAL NOT NULL,
      score REAL NOT NULL,
      mode TEXT NOT NULL,
      setup TEXT NOT NULL,
      status TEXT NOT NULL,
      closed_utc TEXT,
      hash16 TEXT NOT NULL
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS meta (
      k TEXT PRIMARY KEY,
      v TEXT
    )
    """)
    con.commit()
    con.close()

def db_add_signal(sig: dict):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
      INSERT INTO signals(created_utc, symbol, direction, entry, sl, tp, score, mode, setup, status, hash16)
      VALUES(?,?,?,?,?,?,?,?,?,?,?)
    """, (
        sig["created_utc"], sig["symbol"], sig["direction"], sig["entry"], sig["sl"], sig["tp"],
        sig["score"], sig["mode"], sig["setup"], "OPEN", sig["hash16"]
    ))
    con.commit()
    con.close()

def db_last_signal(symbol: str) -> Optional[dict]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
      SELECT id, created_utc, symbol, direction, entry, sl, tp, score, mode, setup, status, hash16
      FROM signals WHERE symbol=? ORDER BY id DESC LIMIT 1
    """, (symbol,))
    r = cur.fetchone()
    con.close()
    if not r:
        return None
    keys = ["id","created_utc","symbol","direction","entry","sl","tp","score","mode","setup","status","hash16"]
    return dict(zip(keys, r))

def db_open_signals() -> List[dict]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
      SELECT id, created_utc, symbol, direction, entry, sl, tp, score, mode, setup, status, hash16
      FROM signals WHERE status='OPEN' ORDER BY id ASC
    """)
    rows = cur.fetchall()
    con.close()
    keys = ["id","created_utc","symbol","direction","entry","sl","tp","score","mode","setup","status","hash16"]
    return [dict(zip(keys, r)) for r in rows]

def db_mark_closed(row_id: int, status: str, closed_utc: str):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("UPDATE signals SET status=?, closed_utc=? WHERE id=?", (status, closed_utc, row_id))
    con.commit()
    con.close()

def db_count_today() -> int:
    now = utc_now()
    day_start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM signals WHERE created_utc >= ?", (day_start.isoformat(),))
    c = cur.fetchone()[0]
    con.close()
    return int(c)

def db_stats(days: int = 7) -> dict:
    now = utc_now()
    since = now - timedelta(days=days)
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
      SELECT status, COUNT(*) FROM signals
      WHERE created_utc >= ?
      GROUP BY status
    """, (since.isoformat(),))
    rows = cur.fetchall()
    con.close()
    out = {"OPEN":0,"TP":0,"SL":0,"UNKNOWN":0,"TOTAL":0}
    for s, c in rows:
        out[s] = int(c)
        out["TOTAL"] += int(c)
    closed = out["TP"] + out["SL"] + out["UNKNOWN"]
    out["CLOSED"] = closed
    out["WINRATE"] = (out["TP"] / closed * 100.0) if closed > 0 else 0.0
    return out

# =========================
# Market data
# =========================
@dataclass
class Candle:
    t: datetime
    o: float
    h: float
    l: float
    c: float

async def fetch_td(session: aiohttp.ClientSession, symbol: str, interval: str, outputsize: int) -> List[Candle]:
    url = "https://api.twelvedata.com/time_series"
    params = {"symbol": symbol, "interval": interval, "outputsize": str(outputsize), "apikey": TD_API_KEY, "format": "JSON"}
    async with session.get(url, params=params, timeout=30) as r:
        data = await r.json()
    if isinstance(data, dict) and data.get("status") == "error":
        raise RuntimeError(f"TwelveData error: {data.get('message')}")
    values = data.get("values")
    if not values:
        raise RuntimeError(f"No candles for {symbol} {interval}")
    candles: List[Candle] = []
    for v in reversed(values):
        t = datetime.fromisoformat(v["datetime"]).replace(tzinfo=timezone.utc)
        candles.append(Candle(t=t, o=float(v["open"]), h=float(v["high"]), l=float(v["low"]), c=float(v["close"])))
    return candles

# =========================
# Indicators
# =========================
def ema_list(closes: List[float], period: int) -> List[float]:
    out = [float("nan")] * len(closes)
    if len(closes) < period:
        return out
    k = 2.0 / (period + 1)
    sma = sum(closes[:period]) / period
    out[period-1] = sma
    prev = sma
    for i in range(period, len(closes)):
        prev = closes[i] * k + prev * (1 - k)
        out[i] = prev
    return out

def rsi_list(closes: List[float], period: int = 14) -> List[float]:
    out = [float("nan")] * len(closes)
    if len(closes) < period + 1:
        return out
    gains, losses = [], []
    for i in range(1, period+1):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    def calc(g, l):
        if l == 0:
            return 100.0
        rs = g / l
        return 100.0 - (100.0 / (1.0 + rs))

    out[period] = calc(avg_gain, avg_loss)
    for i in range(period+1, len(closes)):
        d = closes[i] - closes[i-1]
        gain = max(d, 0.0)
        loss = max(-d, 0.0)
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        out[i] = calc(avg_gain, avg_loss)
    return out

def atr_list(candles: List[Candle], period: int = 14) -> List[float]:
    out = [float("nan")] * len(candles)
    if len(candles) < period + 1:
        return out
    trs = []
    for i in range(1, period+1):
        c = candles[i]
        prev_close = candles[i-1].c
        tr = max(c.h - c.l, abs(c.h - prev_close), abs(c.l - prev_close))
        trs.append(tr)
    atr = sum(trs) / period
    out[period] = atr
    for i in range(period+1, len(candles)):
        c = candles[i]
        prev_close = candles[i-1].c
        tr = max(c.h - c.l, abs(c.h - prev_close), abs(c.l - prev_close))
        atr = (atr * (period - 1) + tr) / period
        out[i] = atr
    return out

def calc_pack(candles: List[Candle]) -> dict:
    closes = [c.c for c in candles]
    return {
        "closes": closes,
        "ema20": ema_list(closes, 20),
        "ema50": ema_list(closes, 50),
        "ema200": ema_list(closes, 200),
        "rsi14": rsi_list(closes, 14),
        "atr14": atr_list(candles, 14),
    }

# =========================
# Strategy
# =========================
def bias_4h(c4: List[Candle]) -> Optional[str]:
    if len(c4) < 220:
        return None
    p = calc_pack(c4)
    price = p["closes"][-1]
    ema50 = p["ema50"][-1]
    ema200 = p["ema200"][-1]
    ema50_prev = p["ema50"][-2]
    if any(x != x for x in [ema50, ema200, ema50_prev]):
        return None
    if abs(ema50 - ema200) / max(1e-12, price) < 0.00045:
        if price > ema200 and ema50 >= ema50_prev:
            return "LONG"
        if price < ema200 and ema50 <= ema50_prev:
            return "SHORT"
        return None
    if ema50 > ema200 and price > ema50 and ema50 > ema50_prev:
        return "LONG"
    if ema50 < ema200 and price < ema50 and ema50 < ema50_prev:
        return "SHORT"
    if price > ema200 and ema50 >= ema50_prev:
        return "LONG"
    if price < ema200 and ema50 <= ema50_prev:
        return "SHORT"
    return None

def vol_ok(atr_val: float, price: float, thr: float) -> bool:
    return (atr_val == atr_val) and atr_val > 0 and (atr_val / max(1e-12, price)) > thr

def setup_1h(symbol: str, c1: List[Candle], direction: str, mode: str) -> bool:
    prm = MODE_PARAMS[mode]
    p = calc_pack(c1)
    price = p["closes"][-1]
    ema20 = p["ema20"][-1]
    ema50 = p["ema50"][-1]
    rsi = p["rsi14"][-1]
    atr = p["atr14"][-1]
    if any(x != x for x in [ema20, ema50, rsi, atr]):
        return False
    thr = prm["vol_thr_1h"]
    if symbol == "XAU/USD":
        thr *= 0.55
    if not vol_ok(atr, price, thr):
        return False
    if mode == "FLOW" and symbol == "XAU/USD":
        ema_ok_long = price > ema20
        ema_ok_short = price < ema20
    else:
        ema_ok_long = price > ema50
        ema_ok_short = price < ema50
    if direction == "LONG":
        return ema_ok_long and rsi >= prm["rsi_long_min"] and rsi <= (78 if symbol == "XAU/USD" else 74)
    else:
        return ema_ok_short and rsi >= 26 and rsi <= prm["rsi_short_max"]

def ema_confirm_5m(c5: List[Candle], direction: str) -> bool:
    p = calc_pack(c5)
    price = p["closes"][-1]
    ema50 = p["ema50"][-1]
    ema50_prev = p["ema50"][-2]
    if any(x != x for x in [ema50, ema50_prev]):
        return False
    slope_up = ema50 > ema50_prev
    slope_down = ema50 < ema50_prev
    return (price > ema50 and slope_up) if direction == "LONG" else (price < ema50 and slope_down)

def impulse_5m(c5: List[Candle], direction: str, mode: str) -> bool:
    prm = MODE_PARAMS[mode]
    p = calc_pack(c5)
    atr5 = p["atr14"][-1]
    if atr5 != atr5 or atr5 <= 0:
        return False
    last = c5[-1]
    rng = last.h - last.l
    dir_ok = (last.c > last.o) if direction == "LONG" else (last.c < last.o)
    return dir_ok and (rng >= atr5 * prm["impulse_k"])

def breakout_15m(c15: List[Candle], direction: str, mode: str) -> bool:
    prm = MODE_PARAMS[mode]
    n = prm["breakout_n"]
    if len(c15) < n + 2:
        return False
    prev = c15[-(n+1):-1]
    last = c15[-1]
    prev_high = max(c.h for c in prev)
    prev_low = min(c.l for c in prev)
    rng = max(1e-12, last.h - last.l)
    if direction == "LONG":
        return (last.c > prev_high) or (last.h > prev_high and last.c >= (last.h - 0.25 * rng))
    else:
        return (last.c < prev_low) or (last.l < prev_low and last.c <= (last.l + 0.25 * rng))

def pullback_15m(c15: List[Candle], direction: str, mode: str) -> bool:
    prm = MODE_PARAMS[mode]
    p = calc_pack(c15)
    price = p["closes"][-1]
    ema20 = p["ema20"][-1]
    ema50 = p["ema50"][-1]
    atr = p["atr14"][-1]
    if any(x != x for x in [ema20, ema50, atr]) or atr <= 0:
        return False
    dist = prm["pullback_dist_atr"] * atr
    near_ema20 = abs(price - ema20) <= dist
    near_ema50 = abs(price - ema50) <= dist
    last = c15[-1]
    body = abs(last.c - last.o)
    rng = max(1e-12, (last.h - last.l))
    thr = 0.25 if mode == "FLOW" else 0.35
    body_ok = (body / rng) >= thr
    if direction == "LONG":
        return (near_ema20 or near_ema50) and (last.c > last.o) and body_ok
    else:
        return (near_ema20 or near_ema50) and (last.c < last.o) and body_ok

def continuation_15m(c15: List[Candle], direction: str, mode: str) -> bool:
    prm = MODE_PARAMS[mode]
    p = calc_pack(c15)
    ema20 = p["ema20"][-1]
    atr = p["atr14"][-1]
    if any(x != x for x in [ema20, atr]) or atr <= 0:
        return False
    last = c15[-1]
    dist = prm["cont_dist_atr"] * atr
    if direction == "LONG":
        touched = last.l <= (ema20 + dist)
        closed_up = (last.c > last.o) and (last.c >= ema20 - 0.15 * atr)
        return touched and closed_up
    else:
        touched = last.h >= (ema20 - dist)
        closed_down = (last.c < last.o) and (last.c <= ema20 + 0.15 * atr)
        return touched and closed_down

def range_break_15m(c15: List[Candle], direction: str, mode: str) -> bool:
    prm = MODE_PARAMS[mode]
    n = prm["range_n"]
    if len(c15) < n + 2:
        return False
    prev = c15[-(n+1):-1]
    last = c15[-1]
    prev_max_close = max(c.c for c in prev)
    prev_min_close = min(c.c for c in prev)
    return (last.c > prev_max_close) if direction == "LONG" else (last.c < prev_min_close)

def swing_high(candles: List[Candle], lookback: int = 90) -> Optional[float]:
    return max(c.h for c in candles[-lookback:]) if len(candles) >= lookback else None

def swing_low(candles: List[Candle], lookback: int = 90) -> Optional[float]:
    return min(c.l for c in candles[-lookback:]) if len(candles) >= lookback else None

def build_signal(symbol: str, direction: str, c5: List[Candle], c15: List[Candle], mode: str, setup: str) -> Optional[dict]:
    prm = MODE_PARAMS[mode]
    entry = c5[-1].c
    if not price_sanity_ok(symbol, entry):
        return None
    created = utc_now().isoformat()
    p15 = calc_pack(c15)
    atr15 = p15["atr14"][-1]
    if atr15 != atr15 or atr15 <= 0:
        return None
    rr = prm["rr"]
    sl_dist = atr15 * prm["sl_atr_mult"]

    sh = swing_high(c15, 90)
    slw = swing_low(c15, 90)

    if direction == "LONG":
        sl = entry - sl_dist
        tp = entry + sl_dist * rr
        if sh is not None and sh < tp:
            near_mult = 0.85 if mode != "FLOW" else 0.60
            if (sh - entry) < (sl_dist * near_mult):
                return None
    else:
        sl = entry + sl_dist
        tp = entry - sl_dist * rr
        if slw is not None and slw > tp:
            near_mult = 0.85 if mode != "FLOW" else 0.60
            if (entry - slw) < (sl_dist * near_mult):
                return None

    d = digits_for(symbol)
    entry_r = round(entry, d)
    sl_r = round(sl, d)
    tp_r = round(tp, d)

    base = prm["score_base"]
    score = base + (0.45 if setup == "BREAKOUT" else 0.25 if setup == "CONTINUATION" else 0.20 if setup == "RANGE_BREAK" else 0.15)
    h = sha16(f"{symbol}|{direction}|{entry_r}|{sl_r}|{tp_r}|{mode}|{setup}")

    return {
        "created_utc": created, "symbol": symbol, "direction": direction,
        "entry": entry_r, "sl": sl_r, "tp": tp_r,
        "score": round(score, 1), "mode": mode, "setup": setup, "hash16": h,
        "cooldown_min": prm["cooldown_min"],
    }

def format_signal(sig: dict) -> str:
    ts = datetime.fromisoformat(sig["created_utc"]).strftime("%Y-%m-%d %H:%M UTC")
    rr = abs(sig["tp"] - sig["entry"]) / max(1e-12, abs(sig["entry"] - sig["sl"]))
    sl_dist = abs(sig["entry"] - sig["sl"])
    return (
        f"📌 СИГНАЛ ({ts})\n"
        f"Инструмент: {sig['symbol']}\n"
        f"Сетап: {sig['setup']}\n"
        f"Направление: {sig['direction']} (MARKET)\n"
        f"Entry: {sig['entry']}\n"
        f"TP: {sig['tp']}\n"
        f"SL: {sig['sl']}\n"
        f"Оценка (0–10): {sig['score']}  |  Режим: {sig['mode']}\n"
        f"R:R ≈ {rr:.2f}  |  SL distance: {sl_dist:.5f}\n\n"
        f"Управление сделкой:\n"
        f"• На +1R → SL в безубыток\n"
        f"• На +1.5R → закрыть 50%\n"
        f"• Остаток трейлить по EMA20 (15m)\n\n"
        f"Риск: 0.5–1% на сделку.\n"
        f"⚠️ Это сигнал по правилам, не гарантия."
    )

def repeated(symbol: str, hash16: str) -> bool:
    last = db_last_signal(symbol)
    return bool(last and last["hash16"] == hash16)

def cooldown_ok(symbol: str, cooldown_minutes: int) -> bool:
    last = db_last_signal(symbol)
    if not last:
        return True
    try:
        t = datetime.fromisoformat(last["created_utc"])
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        return (utc_now() - t) >= timedelta(minutes=cooldown_minutes)
    except Exception:
        return True

def choose_mode() -> str:
    c = db_count_today()
    if c >= MIN_SIGNALS_PER_DAY:
        return "STRICT"
    if c == 0:
        return "FLOW"
    return "BALANCED"

# ✅ force=True отключает фильтр сессии/blackout
async def analyze_symbol(session: aiohttp.ClientSession, symbol: str, mode: str, force: bool=False) -> Tuple[Optional[dict], List[str]]:
    reasons: List[str] = []
    now = utc_now()

    if (not force) and (not in_session_window(now)):
        reasons.append("⛔️ вне сессии")
        return None, reasons
    if (not force) and in_blackout(now):
        reasons.append("⛔️ blackout (новости)")
        return None, reasons

    c4 = await fetch_td(session, symbol, TF_4H, N_4H)
    direction = bias_4h(c4)
    if direction is None:
        reasons.append("❌ 4H: нет трендового bias")
        return None, reasons
    reasons.append(f"✅ 4H bias: {direction}")

    c1 = await fetch_td(session, symbol, TF_1H, N_1H)
    if not setup_1h(symbol, c1, direction, mode):
        reasons.append("❌ 1H: не прошёл фильтр (EMA/RSI/Vol)")
        return None, reasons
    reasons.append("✅ 1H: setup ok")

    c15 = await fetch_td(session, symbol, TF_15, N_15)
    c5 = await fetch_td(session, symbol, TF_5, N_5)

    if not ema_confirm_5m(c5, direction):
        reasons.append("❌ 5m: EMA50 confirm не прошёл")
        return None, reasons
    reasons.append("✅ 5m: EMA confirm ok")

    candidates: List[dict] = []

    if breakout_15m(c15, direction, mode):
        if impulse_5m(c5, direction, mode):
            sig = build_signal(symbol, direction, c5, c15, mode, "BREAKOUT")
            if sig: candidates.append(sig); reasons.append("✅ BREAKOUT: ok")
            else: reasons.append("❌ BREAKOUT: SL/TP blocked")
        else:
            reasons.append("❌ BREAKOUT: нет импульса 5m")
    else:
        reasons.append("❌ BREAKOUT: нет пробоя диапазона 15m")

    if pullback_15m(c15, direction, mode):
        last5 = c5[-1]
        dir_candle = (last5.c > last5.o) if direction == "LONG" else (last5.c < last5.o)
        if dir_candle:
            sig = build_signal(symbol, direction, c5, c15, mode, "PULLBACK")
            if sig: candidates.append(sig); reasons.append("✅ PULLBACK: ok")
            else: reasons.append("❌ PULLBACK: SL/TP blocked")
        else:
            reasons.append("❌ PULLBACK: 5m свеча не в сторону тренда")
    else:
        reasons.append("❌ PULLBACK: цена не у EMA/нет разворота 15m")

    if continuation_15m(c15, direction, mode):
        sig = build_signal(symbol, direction, c5, c15, mode, "CONTINUATION")
        if sig: candidates.append(sig); reasons.append("✅ CONTINUATION: ok")
        else: reasons.append("❌ CONTINUATION: SL/TP blocked")
    else:
        reasons.append("❌ CONTINUATION: нет касания EMA20/нет возврата")

    if range_break_15m(c15, direction, mode):
        sig = build_signal(symbol, direction, c5, c15, mode, "RANGE_BREAK")
        if sig: candidates.append(sig); reasons.append("✅ RANGE_BREAK: ok")
        else: reasons.append("❌ RANGE_BREAK: SL/TP blocked")
    else:
        reasons.append("❌ RANGE_BREAK: нет пробоя close-диапазона")

    if not candidates:
        return None, reasons
    best = max(candidates, key=lambda x: x["score"])
    return best, reasons

async def get_final_signal_for_symbol(session: aiohttp.ClientSession, symbol: str, mode: str, force: bool=False) -> Tuple[Optional[dict], str, List[str]]:
    sig, reasons = await analyze_symbol(session, symbol, mode, force=force)
    if not sig:
        return None, "no_candidate", reasons

    rep = repeated(symbol, sig["hash16"])
    cd = sig["cooldown_min"]
    if mode == "FLOW" and symbol == "XAU/USD":
        cd = min(cd, 40)

    if rep:
        return None, "repeat", reasons
    if not cooldown_ok(symbol, cd):
        return None, f"cooldown({cd}m)", reasons

    return sig, "ok", reasons

async def run_force_all() -> List[str]:
    mode = choose_mode()
    out: List[str] = []
    async with aiohttp.ClientSession() as session:
        for sym in INSTRUMENTS:
            sig, status, reasons = await get_final_signal_for_symbol(session, sym, mode, force=True)  # ✅ force=True
            if sig:
                db_add_signal(sig)
                out.append(format_signal(sig))
            else:
                # ✅ покажем реальную причину (если это была сессия — ты сразу увидишь)
                short = " | ".join(reasons[:3]) if reasons else ""
                out.append(f"ℹ️ {sym}: кандидата нет или блок ({status}). {short}")
    return out

async def run_analysis_auto() -> str:
    """Авто-анализ: соблюдает сессию/blackout"""
    mode = choose_mode()
    try:
        async with aiohttp.ClientSession() as session:
            best = None
            for sym in INSTRUMENTS:
                sig, status, _ = await get_final_signal_for_symbol(session, sym, mode, force=False)
                if sig and (best is None or sig["score"] > best["score"]):
                    best = sig
            if not best:
                return ""
            db_add_signal(best)
            return format_signal(best)
    except Exception:
        return ""

async def run_diag() -> str:
    mode = choose_mode()
    out = [f"🧩 DIAG ({utc_now().strftime('%Y-%m-%d %H:%M UTC')}) | mode={mode}"]
    try:
        async with aiohttp.ClientSession() as session:
            for sym in INSTRUMENTS:
                sig, reasons = await analyze_symbol(session, sym, mode, force=False)
                out.append(f"\n— {sym} —")
                out.extend(reasons[:24])
                if sig:
                    rep = repeated(sym, sig["hash16"])
                    cd = sig["cooldown_min"]
                    if mode == "FLOW" and sym == "XAU/USD":
                        cd = min(cd, 40)
                    cd_ok = cooldown_ok(sym, cd)
                    out.append(f"👉 Кандидат: {sig['setup']} {sig['direction']} score={sig['score']}")
                    out.append(f"   block_check: repeat={rep} | cooldown_ok={cd_ok} (cd={cd}m)")
    except Exception as e:
        out.append(f"\n⚠️ DIAG error: {e}")
    return "\n".join(out)

# =========================
# Monitor TP/SL (не трогаем)
# =========================
def candle_hits(sig: dict, candles: List[Candle]) -> Optional[str]:
    tp = float(sig["tp"]); sl = float(sig["sl"]); direction = sig["direction"]
    created = datetime.fromisoformat(sig["created_utc"])
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    for c in candles:
        if c.t < created:
            continue
        if direction == "LONG":
            hit_tp = c.h >= tp; hit_sl = c.l <= sl
        else:
            hit_tp = c.l <= tp; hit_sl = c.h >= sl
        if hit_tp and hit_sl: return "UNKNOWN"
        if hit_tp: return "TP"
        if hit_sl: return "SL"
    return None

async def monitor_open_signals():
    opens = db_open_signals()
    if not opens:
        return
    try:
        async with aiohttp.ClientSession() as session:
            for sig in opens:
                candles = await fetch_td(session, sig["symbol"], TF_MONITOR, N_MONITOR)
                res = candle_hits(sig, candles)
                if res:
                    db_mark_closed(sig["id"], res, utc_now().isoformat())
    except Exception:
        pass

# =========================
# Telegram
# =========================
bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None)

def owner_guard(user_id: int) -> bool:
    return user_id == OWNER_ID

def kb_main():
    kb = InlineKeyboardMarkup()
    kb.row(InlineKeyboardButton("📊 Сигнал", callback_data="force_signal"))
    kb.row(
        InlineKeyboardButton("📈 Сегодня", callback_data="today"),
        InlineKeyboardButton("📊 Статистика", callback_data="stats"),
    )
    return kb

@bot.message_handler(commands=["start"])
def on_start(msg):
    if not owner_guard(msg.from_user.id):
        bot.reply_to(msg, "⛔️ Доступ запрещён.")
        return
    bot.send_message(
        msg.chat.id,
        "✅ Бот запущен.\n"
        "Важно: /now и кнопка «Сигнал» работают 24/5 (игнорируют сессию).\n"
        "Авто-анализ — по расписанию и по твоей сессии UTC.\n"
        "Команды: /diag /now\n",
        reply_markup=kb_main()
    )

@bot.message_handler(commands=["diag"])
def on_diag(msg):
    if not owner_guard(msg.from_user.id):
        bot.reply_to(msg, "⛔️")
        return
    bot.send_message(msg.chat.id, "Собираю диагностику...")
    text = asyncio.run(run_diag())
    chunk = 3500
    for i in range(0, len(text), chunk):
        bot.send_message(msg.chat.id, text[i:i+chunk])

@bot.message_handler(commands=["now"])
def on_now(msg):
    if not owner_guard(msg.from_user.id):
        bot.reply_to(msg, "⛔️")
        return
    bot.send_message(msg.chat.id, "Ищу сигналы сейчас...")
    res = asyncio.run(run_force_all())
    for t in res:
        bot.send_message(msg.chat.id, t, reply_markup=kb_main())

@bot.callback_query_handler(func=lambda c: True)
def on_callback(call):
    if not owner_guard(call.from_user.id):
        bot.answer_callback_query(call.id, "⛔️", show_alert=True)
        return
    if call.data == "force_signal":
        bot.answer_callback_query(call.id, "Анализирую...")
        res = asyncio.run(run_force_all())
        for t in res:
            bot.send_message(call.message.chat.id, t, reply_markup=kb_main())
    elif call.data == "today":
        bot.answer_callback_query(call.id)
        c = db_count_today()
        bot.send_message(call.message.chat.id, f"📈 Сегодня сигналов: {c}/{MIN_SIGNALS_PER_DAY}", reply_markup=kb_main())
    elif call.data == "stats":
        bot.answer_callback_query(call.id)
        s = db_stats(days=7)
        bot.send_message(
            call.message.chat.id,
            "📊 Статистика за 7 дней:\n"
            f"Всего: {s['TOTAL']}\n"
            f"Закрыто: {s['CLOSED']} | TP: {s['TP']} | SL: {s['SL']} | UNKNOWN: {s['UNKNOWN']}\n"
            f"Winrate (по закрытым): {s['WINRATE']:.1f}%",
            reply_markup=kb_main()
        )

# =========================
# Jobs
# =========================
def job_analysis():
    try:
        text = asyncio.run(run_analysis_auto())
        if text:
            bot.send_message(OWNER_ID, text, reply_markup=kb_main())
    except Exception:
        pass

def job_monitor():
    try:
        asyncio.run(monitor_open_signals())
    except Exception:
        pass

def job_heartbeat():
    try:
        now = utc_now()
        mode = choose_mode()
        c = db_count_today()
        msg = (
            f"🟢 Статус: бот активен\n"
            f"UTC: {now.strftime('%Y-%m-%d %H:%M')}\n"
            f"Режим: {mode}\n"
            f"Сегодня сигналов: {c}/{MIN_SIGNALS_PER_DAY}\n"
            f"Auto interval: {AUTO_INTERVAL_MINUTES} min"
        )
        bot.send_message(OWNER_ID, msg, reply_markup=kb_main())
    except Exception:
        pass

def main():
    if not BOT_TOKEN or OWNER_ID == 0 or not TD_API_KEY:
        raise RuntimeError("Set BOT_TOKEN, OWNER_ID, TD_API_KEY env vars.")
    db_init()

    sched = BackgroundScheduler(timezone="UTC")
    sched.add_job(job_analysis, "interval", minutes=AUTO_INTERVAL_MINUTES, id="analysis", replace_existing=True)
    sched.add_job(job_monitor, "interval", minutes=MONITOR_INTERVAL_MINUTES, id="monitor", replace_existing=True)
    sched.add_job(job_heartbeat, "interval", minutes=HEARTBEAT_INTERVAL_MINUTES, id="heartbeat", replace_existing=True)
    sched.start()

    bot.infinity_polling(timeout=30, long_polling_timeout=30)

if __name__ == "__main__":
    main()
