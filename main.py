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

# ==========================================================
# ENV
# ==========================================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
TD_API_KEY = os.getenv("TD_API_KEY", "")

# Частота авто-анализа (минуты) — для breakout логики лучше 15
AUTO_INTERVAL_MINUTES = int(os.getenv("AUTO_INTERVAL_MINUTES", "15"))

# Мониторинг открытых сигналов (для статистики) — каждые 5 минут ок
MONITOR_INTERVAL_MINUTES = int(os.getenv("MONITOR_INTERVAL_MINUTES", "5"))

# Heartbeat: чтобы бот не "молчал", отправляет статус раз в N минут
HEARTBEAT_INTERVAL_MINUTES = int(os.getenv("HEARTBEAT_INTERVAL_MINUTES", "180"))  # 3 часа

# Цель по сигналам
MIN_SIGNALS_PER_DAY = int(os.getenv("MIN_SIGNALS_PER_DAY", "3"))

# Сессия (UTC)
SESSION_START_UTC = int(os.getenv("SESSION_START_UTC", "7"))
SESSION_END_UTC = int(os.getenv("SESSION_END_UTC", "21"))

# Черный список времени (новости) в UTC, формат:
# 2026-02-27T12:25:00Z/2026-02-27T13:05:00Z,2026-02-28T...
NEWS_BLACKOUT_UTC = os.getenv("NEWS_BLACKOUT_UTC", "").strip()

# Инструменты — строго
INSTRUMENTS = ["EUR/USD", "XAU/USD"]

# Таймфреймы
TF_4H = "4h"
TF_1H = "1h"
TF_15 = "15min"
TF_5 = "5min"
TF_MONITOR = "5min"

# Количество свечей (запас)
N_4H = 350
N_1H = 650
N_15 = 900
N_5 = 1200
N_MONITOR = 450

DB_PATH = "bot.db"

# ==========================================================
# Режимы — бот сам понижает строгость, чтобы добирать сигналы
# ==========================================================
MODES = ["FLOW", "BALANCED", "STRICT"]  # от мягкого к строгому

MODE_PARAMS: Dict[str, dict] = {
    "STRICT": {
        "score": 9.5,
        "rr": 2.10,
        "sl_atr_mult": 1.90,
        "cooldown_min": 150,
        "breakout_n": 8,     # пробой диапазона 8 свечей 15m
        "impulse_k": 1.10,   # range 5m >= ATR5*1.10
        "vol_thr_1h": 0.00050,
        "rsi_long_min": 56,
        "rsi_short_max": 44,
    },
    "BALANCED": {
        "score": 8.5,
        "rr": 1.85,
        "sl_atr_mult": 1.65,
        "cooldown_min": 120,
        "breakout_n": 5,
        "impulse_k": 0.90,
        "vol_thr_1h": 0.00038,
        "rsi_long_min": 52,
        "rsi_short_max": 50,
    },
    "FLOW": {
        "score": 7.8,
        "rr": 1.60,
        "sl_atr_mult": 1.50,
        "cooldown_min": 75,   # ниже — чтобы добирать до 3/день
        "breakout_n": 3,
        "impulse_k": 0.65,
        "vol_thr_1h": 0.00030,
        "rsi_long_min": 49,
        "rsi_short_max": 53,
    },
}

# ==========================================================
# Helpers
# ==========================================================
def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def in_session_window(now_utc: datetime) -> bool:
    return SESSION_START_UTC <= now_utc.hour < SESSION_END_UTC

def parse_blackouts(s: str) -> List[Tuple[datetime, datetime]]:
    out: List[Tuple[datetime, datetime]] = []
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

def price_sanity_ok(symbol: str, price: float) -> bool:
    # широкие диапазоны
    if symbol == "EUR/USD":
        return 0.8 <= price <= 1.6
    if symbol == "XAU/USD":
        return 1000 <= price <= 10000
    return True

def digits_for(symbol: str) -> int:
    return 5 if "EUR" in symbol else 2

# ==========================================================
# DB
# ==========================================================
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
    cur.execute("CREATE INDEX IF NOT EXISTS idx_signals_created ON signals(created_utc)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_signals_status ON signals(status)")
    con.commit()
    con.close()

def meta_get(key: str) -> Optional[str]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT v FROM meta WHERE k=?", (key,))
    r = cur.fetchone()
    con.close()
    return r[0] if r else None

def meta_set(key: str, val: str):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (key, val))
    con.commit()
    con.close()

def db_add_signal(sig: dict):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
      INSERT INTO signals(created_utc, symbol, direction, entry, sl, tp, score, mode, status, hash16)
      VALUES(?,?,?,?,?,?,?,?,?,?)
    """, (
        sig["created_utc"], sig["symbol"], sig["direction"], sig["entry"], sig["sl"], sig["tp"],
        sig["score"], sig["mode"], "OPEN", sig["hash16"]
    ))
    con.commit()
    con.close()

def db_last_signal(symbol: str) -> Optional[dict]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
      SELECT id, created_utc, symbol, direction, entry, sl, tp, score, mode, status, hash16
      FROM signals WHERE symbol=? ORDER BY id DESC LIMIT 1
    """, (symbol,))
    r = cur.fetchone()
    con.close()
    if not r:
        return None
    keys = ["id","created_utc","symbol","direction","entry","sl","tp","score","mode","status","hash16"]
    return dict(zip(keys, r))

def db_open_signals() -> List[dict]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
      SELECT id, created_utc, symbol, direction, entry, sl, tp, score, mode, status, hash16
      FROM signals WHERE status='OPEN' ORDER BY id ASC
    """)
    rows = cur.fetchall()
    con.close()
    keys = ["id","created_utc","symbol","direction","entry","sl","tp","score","mode","status","hash16"]
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

# ==========================================================
# Market data (TwelveData)
# ==========================================================
@dataclass
class Candle:
    t: datetime
    o: float
    h: float
    l: float
    c: float

async def fetch_td(session: aiohttp.ClientSession, symbol: str, interval: str, outputsize: int) -> List[Candle]:
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": str(outputsize),
        "apikey": TD_API_KEY,
        "format": "JSON",
    }
    async with session.get(url, params=params, timeout=30) as r:
        data = await r.json()
    if isinstance(data, dict) and data.get("status") == "error":
        raise RuntimeError(f"TwelveData error: {data.get('message')}")
    values = data.get("values")
    if not values:
        raise RuntimeError(f"No candles for {symbol} {interval}")
    candles: List[Candle] = []
    for v in reversed(values):  # oldest -> newest
        t = datetime.fromisoformat(v["datetime"]).replace(tzinfo=timezone.utc)
        candles.append(Candle(
            t=t, o=float(v["open"]), h=float(v["high"]), l=float(v["low"]), c=float(v["close"])
        ))
    return candles

# ==========================================================
# Indicators (pure python)
# ==========================================================
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
        "ema50": ema_list(closes, 50),
        "ema200": ema_list(closes, 200),
        "rsi14": rsi_list(closes, 14),
        "atr14": atr_list(candles, 14),
    }

# ==========================================================
# Strategy
# ==========================================================
def bias_4h(c4: List[Candle]) -> Optional[str]:
    """
    Тренд по 4H: EMA50 vs EMA200 + наклон EMA50 + цена относительно EMA50.
    """
    if len(c4) < 220:
        return None
    p = calc_pack(c4)
    price = p["closes"][-1]
    ema50 = p["ema50"][-1]
    ema200 = p["ema200"][-1]
    ema50_prev = p["ema50"][-2]
    if any(x != x for x in [ema50, ema200, ema50_prev]):
        return None

    # слабый тренд / сжатие
    if abs(ema50 - ema200) / max(1e-12, price) < 0.001:
        return None

    if ema50 > ema200 and price > ema50 and ema50 > ema50_prev:
        return "LONG"
    if ema50 < ema200 and price < ema50 and ema50 < ema50_prev:
        return "SHORT"
    return None

def vol_ok(atr_val: float, price: float, threshold: float) -> bool:
    return (atr_val == atr_val) and atr_val > 0 and (atr_val / max(1e-12, price)) > threshold

def setup_1h(c1: List[Candle], direction: str, mode: str) -> bool:
    """
    1H фильтр: цена относительно EMA50, RSI, минимум волатильности по ATR.
    """
    p = calc_pack(c1)
    price = p["closes"][-1]
    ema50 = p["ema50"][-1]
    r = p["rsi14"][-1]
    a = p["atr14"][-1]
    if any(x != x for x in [ema50, r, a]):
        return False

    prm = MODE_PARAMS[mode]
    if not vol_ok(a, price, prm["vol_thr_1h"]):
        return False

    if direction == "LONG":
        return (price > ema50) and (r >= prm["rsi_long_min"]) and (r <= 72)
    else:
        return (price < ema50) and (r >= 28) and (r <= prm["rsi_short_max"])

def breakout_15m(c15: List[Candle], direction: str, mode: str) -> bool:
    """
    Триггер: пробой диапазона последних N 15m свечей (без текущей).
    """
    prm = MODE_PARAMS[mode]
    n = prm["breakout_n"]
    if len(c15) < n + 2:
        return False
    prev = c15[-(n+1):-1]
    last = c15[-1]
    prev_high = max(c.h for c in prev)
    prev_low = min(c.l for c in prev)
    return (last.c > prev_high) if direction == "LONG" else (last.c < prev_low)

def ema_confirm_5m(c5: List[Candle], direction: str) -> bool:
    """
    5m базовое подтверждение: цена над EMA50 (или под) + наклон EMA50.
    """
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
    """
    5m импульс: направленная свеча + range >= ATR5*k
    """
    prm = MODE_PARAMS[mode]
    p = calc_pack(c5)
    atr5 = p["atr14"][-1]
    if atr5 != atr5 or atr5 <= 0:
        return False
    last = c5[-1]
    rng = last.h - last.l
    dir_ok = (last.c > last.o) if direction == "LONG" else (last.c < last.o)
    return dir_ok and (rng >= atr5 * prm["impulse_k"])

def swing_high(candles: List[Candle], lookback: int = 90) -> Optional[float]:
    if len(candles) < lookback:
        return None
    return max(c.h for c in candles[-lookback:])

def swing_low(candles: List[Candle], lookback: int = 90) -> Optional[float]:
    if len(candles) < lookback:
        return None
    return min(c.l for c in candles[-lookback:])

def build_signal(symbol: str, direction: str, c5: List[Candle], c15: List[Candle], mode: str) -> Optional[dict]:
    prm = MODE_PARAMS[mode]
    entry = c5[-1].c
    if not price_sanity_ok(symbol, entry):
        return None

    created = utc_now().isoformat()

    # ATR(15m) для SL/TP
    p15 = calc_pack(c15)
    atr15 = p15["atr14"][-1]
    if atr15 != atr15 or atr15 <= 0:
        return None

    sl_dist = atr15 * prm["sl_atr_mult"]
    rr = prm["rr"]

    # Не входить в "стену" (свинг уровни)
    sh = swing_high(c15, 90)
    slw = swing_low(c15, 90)

    if direction == "LONG":
        sl = entry - sl_dist
        tp = entry + sl_dist * rr
        if sh is not None and sh < tp:
            # если ближайший high слишком близко — пропускаем
            near_mult = 0.85 if mode != "FLOW" else 0.55
            if (sh - entry) < (sl_dist * near_mult):
                return None
    else:
        sl = entry + sl_dist
        tp = entry - sl_dist * rr
        if slw is not None and slw > tp:
            near_mult = 0.85 if mode != "FLOW" else 0.55
            if (entry - slw) < (sl_dist * near_mult):
                return None

    d = digits_for(symbol)
    entry_r = round(entry, d)
    sl_r = round(sl, d)
    tp_r = round(tp, d)

    h = sha16(f"{symbol}|{direction}|{entry_r}|{sl_r}|{tp_r}|{mode}")

    return {
        "created_utc": created,
        "symbol": symbol,
        "direction": direction,
        "entry": entry_r,
        "sl": sl_r,
        "tp": tp_r,
        "score": prm["score"],
        "mode": mode,
        "hash16": h,
        "cooldown_min": prm["cooldown_min"],
    }

def format_signal(sig: dict) -> str:
    ts = datetime.fromisoformat(sig["created_utc"]).strftime("%Y-%m-%d %H:%M UTC")
    rr = abs(sig["tp"] - sig["entry"]) / max(1e-12, abs(sig["entry"] - sig["sl"]))
    sl_dist = abs(sig["entry"] - sig["sl"])
    return (
        f"📌 СИГНАЛ ({ts})\n"
        f"Инструмент: {sig['symbol']}\n"
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

# ==========================================================
# Mode selection (чтобы добирать до MIN_SIGNALS_PER_DAY)
# ==========================================================
def choose_mode() -> str:
    """
    Логика:
      - Если уже добрали MIN_SIGNALS_PER_DAY -> STRICT
      - Если 0 сигналов -> FLOW (макс добор)
      - Если 1 сигнал -> BALANCED
      - Если 2 сигнала -> BALANCED
    """
    c = db_count_today()
    if c >= MIN_SIGNALS_PER_DAY:
        return "STRICT"
    if c == 0:
        return "FLOW"
    return "BALANCED"

# ==========================================================
# Analysis
# ==========================================================
async def analyze_symbol(session: aiohttp.ClientSession, symbol: str, mode: str) -> Optional[dict]:
    now = utc_now()
    if not in_session_window(now):
        return None
    if in_blackout(now):
        return None

    # 4H bias
    c4 = await fetch_td(session, symbol, TF_4H, N_4H)
    direction = bias_4h(c4)
    if direction is None:
        return None

    # 1H setup
    c1 = await fetch_td(session, symbol, TF_1H, N_1H)
    if not setup_1h(c1, direction, mode):
        return None

    # 15m breakout
    c15 = await fetch_td(session, symbol, TF_15, N_15)
    if not breakout_15m(c15, direction, mode):
        return None

    # 5m confirm
    c5 = await fetch_td(session, symbol, TF_5, N_5)
    if not ema_confirm_5m(c5, direction):
        return None
    if not impulse_5m(c5, direction, mode):
        return None

    sig = build_signal(symbol, direction, c5, c15, mode)
    if not sig:
        return None

    if repeated(symbol, sig["hash16"]):
        return None

    if not cooldown_ok(symbol, sig["cooldown_min"]):
        return None

    return sig

async def run_analysis(send_only_signals: bool) -> str:
    mode = choose_mode()
    try:
        async with aiohttp.ClientSession() as session:
            best = None
            for sym in INSTRUMENTS:
                sig = await analyze_symbol(session, sym, mode)
                if sig:
                    best = sig if best is None or sig["score"] > best["score"] else best

            if not best:
                if send_only_signals:
                    return ""
                return (
                    f"🔎 Анализ выполнен ({utc_now().strftime('%Y-%m-%d %H:%M UTC')})\n"
                    f"Сильных сетапов нет. Режим: {mode}. Сегодня: {db_count_today()}/{MIN_SIGNALS_PER_DAY}"
                )

            db_add_signal(best)
            return format_signal(best)

    except Exception as e:
        if send_only_signals:
            return ""
        return f"⚠️ Ошибка анализа: {e}"

# ==========================================================
# Monitor TP/SL for OPEN signals
# ==========================================================
def candle_hits(sig: dict, candles: List[Candle]) -> Optional[str]:
    tp = float(sig["tp"])
    sl = float(sig["sl"])
    direction = sig["direction"]
    created = datetime.fromisoformat(sig["created_utc"])
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)

    for c in candles:
        if c.t < created:
            continue

        if direction == "LONG":
            hit_tp = c.h >= tp
            hit_sl = c.l <= sl
        else:
            hit_tp = c.l <= tp
            hit_sl = c.h >= sl

        if hit_tp and hit_sl:
            return "UNKNOWN"
        if hit_tp:
            return "TP"
        if hit_sl:
            return "SL"
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

# ==========================================================
# Telegram
# ==========================================================
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
        bot.reply_to(msg, "⛔️ Доступ запрещён. Этот бот доступен только владельцу.")
        return
    bot.send_message(
        msg.chat.id,
        "✅ Бот запущен.\n"
        "Логика: 4H (trend) → 1H (setup) → 15m (breakout) → 5m (EMA+impulse)\n"
        "Инструменты: EUR/USD и XAU/USD\n"
        f"Сессия (UTC): {SESSION_START_UTC}:00–{SESSION_END_UTC}:00\n"
        f"Авто-анализ: каждые {AUTO_INTERVAL_MINUTES} минут\n"
        f"Статус-отчёт: каждые {HEARTBEAT_INTERVAL_MINUTES} минут\n"
        f"Цель: минимум {MIN_SIGNALS_PER_DAY} сигнал(а) в день\n"
        "Нажми «📊 Сигнал», чтобы сделать анализ сейчас.",
        reply_markup=kb_main()
    )

@bot.callback_query_handler(func=lambda c: True)
def on_callback(call):
    if not owner_guard(call.from_user.id):
        bot.answer_callback_query(call.id, "⛔️", show_alert=True)
        return

    if call.data == "force_signal":
        bot.answer_callback_query(call.id, "Анализирую...")
        text = asyncio.run(run_analysis(send_only_signals=False))
        bot.send_message(call.message.chat.id, text, reply_markup=kb_main())

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

# ==========================================================
# Jobs
# ==========================================================
def job_analysis():
    # Авто-анализ: чтобы не спамить — отправляем только сигнал (если найден)
    try:
        text = asyncio.run(run_analysis(send_only_signals=True))
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
    # Статус раз в N часов, чтобы было видно, что бот живой
    try:
        last = meta_get("heartbeat_last")
        now = utc_now()

        # если уже отправляли недавно — не спамим (защита)
        if last:
            try:
                t = datetime.fromisoformat(last)
                if t.tzinfo is None:
                    t = t.replace(tzinfo=timezone.utc)
                if (now - t) < timedelta(minutes=max(10, HEARTBEAT_INTERVAL_MINUTES - 1)):
                    return
            except Exception:
                pass

        mode = choose_mode()
        c = db_count_today()
        msg = (
            f"🟢 Статус: бот активен\n"
            f"UTC: {now.strftime('%Y-%m-%d %H:%M')}\n"
            f"Режим: {mode}\n"
            f"Сегодня сигналов: {c}/{MIN_SIGNALS_PER_DAY}\n"
            f"Сессия (UTC): {SESSION_START_UTC}:00–{SESSION_END_UTC}:00\n"
            f"Auto interval: {AUTO_INTERVAL_MINUTES} min"
        )
        bot.send_message(OWNER_ID, msg, reply_markup=kb_main())
        meta_set("heartbeat_last", now.isoformat())
    except Exception:
        pass

# ==========================================================
# Main
# ==========================================================
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
