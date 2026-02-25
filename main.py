import os
import time
import asyncio
import aiohttp
import sqlite3
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Tuple

import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from apscheduler.schedulers.background import BackgroundScheduler

# =========================
# ENV
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
TD_API_KEY = os.getenv("TD_API_KEY", "")

AUTO_INTERVAL_MINUTES = int(os.getenv("AUTO_INTERVAL_MINUTES", "60"))
MONITOR_INTERVAL_MINUTES = int(os.getenv("MONITOR_INTERVAL_MINUTES", "5"))

STRICT_THRESHOLD = float(os.getenv("STRICT_THRESHOLD", "8.0"))
BALANCED_THRESHOLD = float(os.getenv("BALANCED_THRESHOLD", "7.0"))
MIN_SIGNALS_PER_DAY = int(os.getenv("MIN_SIGNALS_PER_DAY", "3"))

RR_STRICT = float(os.getenv("RR_STRICT", "2.0"))
RR_BALANCED = float(os.getenv("RR_BALANCED", "1.7"))

SL_ATR_MULT_STRICT = float(os.getenv("SL_ATR_MULT_STRICT", "1.8"))
SL_ATR_MULT_BALANCED = float(os.getenv("SL_ATR_MULT_BALANCED", "1.6"))

COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "180"))

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
N_1H = 600
N_15 = 900
N_5 = 1200
N_MONITOR = 400

DB_PATH = "bot.db"


# =========================
# Time helpers
# =========================
def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def in_session_window(now_utc: datetime) -> bool:
    h = now_utc.hour
    return SESSION_START_UTC <= h < SESSION_END_UTC

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
      status TEXT NOT NULL,
      closed_utc TEXT,
      hash16 TEXT NOT NULL
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_signals_created ON signals(created_utc)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_signals_status ON signals(status)")
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


# =========================
# Market data (TwelveData)
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
    for v in reversed(values):
        t = datetime.fromisoformat(v["datetime"]).replace(tzinfo=timezone.utc)
        candles.append(Candle(
            t=t, o=float(v["open"]), h=float(v["high"]), l=float(v["low"]), c=float(v["close"])
        ))
    return candles


# =========================
# Indicators (pure python)
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
        "ema50": ema_list(closes, 50),
        "ema200": ema_list(closes, 200),
        "rsi14": rsi_list(closes, 14),
        "atr14": atr_list(candles, 14),
    }

def bias_4h(c4: List[Candle]) -> Optional[str]:
    p = calc_pack(c4)
    i = len(c4) - 1
    if i < 201:
        return None
    price = p["closes"][i]
    ema50 = p["ema50"][i]
    ema200 = p["ema200"][i]
    ema50_prev = p["ema50"][i-1]
    if any(x != x for x in [ema50, ema200, ema50_prev]):
        return None
    if abs(ema50 - ema200) / price < 0.001:
        return None
    slope_up = ema50 > ema50_prev
    slope_down = ema50 < ema50_prev
    if ema50 > ema200 and price > ema50 and slope_up:
        return "LONG"
    if ema50 < ema200 and price < ema50 and slope_down:
        return "SHORT"
    return None

def vol_ok(atr_val: float, price: float, threshold: float) -> bool:
    return (atr_val == atr_val) and atr_val > 0 and (atr_val / price) > threshold

def setup_1h(c1: List[Candle], direction: str, strict: bool) -> bool:
    p = calc_pack(c1)
    price = p["closes"][-1]
    ema50 = p["ema50"][-1]
    r = p["rsi14"][-1]
    a = p["atr14"][-1]
    if any(x != x for x in [ema50, r, a]):
        return False
    vol_thr = 0.00045 if strict else 0.00035
    if not vol_ok(a, price, vol_thr):
        return False
    if direction == "LONG":
        r_ok = (52 <= r <= 70) if strict else (50 <= r <= 72)
        return (price > ema50) and r_ok
    else:
        r_ok = (30 <= r <= 48) if strict else (28 <= r <= 50)
        return (price < ema50) and r_ok

def trigger_15m(c15: List[Candle], direction: str, strict: bool) -> bool:
    p = calc_pack(c15)
    price = p["closes"][-1]
    ema50 = p["ema50"][-1]
    ema50_prev = p["ema50"][-2]
    r = p["rsi14"][-1]
    if any(x != x for x in [ema50, ema50_prev, r]):
        return False
    slope_up = ema50 > ema50_prev
    slope_down = ema50 < ema50_prev
    if direction == "LONG":
        r_ok = (45 <= r <= 68) if strict else (42 <= r <= 72)
        return (price > ema50) and slope_up and r_ok
    else:
        r_ok = (32 <= r <= 55) if strict else (28 <= r <= 58)
        return (price < ema50) and slope_down and r_ok

def confirm_5m(c5: List[Candle], direction: str, strict: bool) -> bool:
    p = calc_pack(c5)
    price = p["closes"][-1]
    ema50 = p["ema50"][-1]
    ema50_prev = p["ema50"][-2]
    r = p["rsi14"][-1]
    if any(x != x for x in [ema50, ema50_prev, r]):
        return False
    slope_up = ema50 > ema50_prev
    slope_down = ema50 < ema50_prev
    if direction == "LONG":
        r_ok = (52 <= r <= 70) if strict else (50 <= r <= 72)
        return (price > ema50) and slope_up and r_ok
    else:
        r_ok = (30 <= r <= 48) if strict else (28 <= r <= 50)
        return (price < ema50) and slope_down and r_ok

def swing_high(candles: List[Candle], lookback: int = 80) -> Optional[float]:
    if len(candles) < lookback:
        return None
    return max(c.h for c in candles[-lookback:])

def swing_low(candles: List[Candle], lookback: int = 80) -> Optional[float]:
    if len(candles) < lookback:
        return None
    return min(c.l for c in candles[-lookback:])

def build_signal(symbol: str, direction: str, c5: List[Candle], c15: List[Candle], strict: bool) -> Optional[dict]:
    entry = c5[-1].c
    created = utc_now().isoformat()

    p15 = calc_pack(c15)
    atr15 = p15["atr14"][-1]
    if atr15 != atr15 or atr15 <= 0:
        return None

    rr = RR_STRICT if strict else RR_BALANCED
    mult = SL_ATR_MULT_STRICT if strict else SL_ATR_MULT_BALANCED
    sl_dist = atr15 * mult

    sh = swing_high(c15, 80)
    slw = swing_low(c15, 80)

    if direction == "LONG":
        sl = entry - sl_dist
        tp = entry + sl_dist * rr
        if strict and sh is not None and sh < tp and (sh - entry) < (sl_dist * 0.9):
            return None
    else:
        sl = entry + sl_dist
        tp = entry - sl_dist * rr
        if strict and slw is not None and slw > tp and (entry - slw) < (sl_dist * 0.9):
            return None

    digits = 5 if "EUR" in symbol else 2
    entry_r = round(entry, digits)
    sl_r = round(sl, digits)
    tp_r = round(tp, digits)

    score = 10.0 if strict else 8.5
    mode = "STRICT" if strict else "BALANCED"
    h = sha16(f"{symbol}|{direction}|{entry_r}|{sl_r}|{tp_r}|{mode}")

    return {
        "created_utc": created,
        "symbol": symbol,
        "direction": direction,
        "entry": entry_r,
        "sl": sl_r,
        "tp": tp_r,
        "score": score,
        "mode": mode,
        "hash16": h,
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
        f"Управление сделкой (рекомендация):\n"
        f"• На +1R → SL в безубыток\n"
        f"• На +1.5R → закрыть 50%\n"
        f"• Остаток трейлить по EMA20 (15m)\n\n"
        f"Риск: 0.5–1% на сделку.\n"
        f"⚠️ Это сигнал по правилам, не гарантия."
    )

def cooldown_ok(symbol: str) -> bool:
    last = db_last_signal(symbol)
    if not last:
        return True
    try:
        t = datetime.fromisoformat(last["created_utc"])
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        return (utc_now() - t) >= timedelta(minutes=COOLDOWN_MINUTES)
    except Exception:
        return True

def repeated(symbol: str, hash16: str) -> bool:
    last = db_last_signal(symbol)
    return bool(last and last["hash16"] == hash16)


# =========================
# Core async tasks
# =========================
async def analyze_symbol(session: aiohttp.ClientSession, symbol: str, strict: bool) -> Optional[dict]:
    now = utc_now()
    if not in_session_window(now):
        return None
    if in_blackout(now):
        return None
    if not cooldown_ok(symbol):
        return None

    c4 = await fetch_td(session, symbol, TF_4H, N_4H)
    direction = bias_4h(c4)
    if direction is None:
        return None

    c1 = await fetch_td(session, symbol, TF_1H, N_1H)
    if not setup_1h(c1, direction, strict):
        return None

    c15 = await fetch_td(session, symbol, TF_15, N_15)
    if not trigger_15m(c15, direction, strict):
        return None

    c5 = await fetch_td(session, symbol, TF_5, N_5)
    if not confirm_5m(c5, direction, strict):
        return None

    sig = build_signal(symbol, direction, c5, c15, strict)
    if not sig:
        return None
    if repeated(symbol, sig["hash16"]):
        return None
    return sig

async def run_analysis(send_only_signals: bool) -> str:
    today_count = db_count_today()
    need_more = today_count < MIN_SIGNALS_PER_DAY
    strict = not need_more

    threshold = STRICT_THRESHOLD if strict else BALANCED_THRESHOLD
    _ = threshold  # score у нас фиксированный, threshold оставлен для будущей тонкой настройки

    async with aiohttp.ClientSession() as session:
        best = None
        for sym in INSTRUMENTS:
            sig = await analyze_symbol(session, sym, strict=strict)
            if sig:
                best = sig if best is None or sig["score"] > best["score"] else best

        if not best:
            if send_only_signals:
                return ""
            mode = "STRICT" if strict else "BALANCED"
            return f"🔎 Анализ выполнен ({utc_now().strftime('%Y-%m-%d %H:%M UTC')})\nСильных сетапов нет. Режим: {mode}. Сегодня: {today_count}/{MIN_SIGNALS_PER_DAY}"

        db_add_signal(best)
        return format_signal(best)

def candle_hits(sig: dict, candles: List[Candle]) -> Optional[str]:
    tp = float(sig["tp"])
    sl = float(sig["sl"])
    direction = sig["direction"]
    created = datetime.fromisoformat(sig["created_utc"])
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)

    relevant = [c for c in candles if c.t >= created]
    for c in relevant:
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
    async with aiohttp.ClientSession() as session:
        for sig in opens:
            candles = await fetch_td(session, sig["symbol"], TF_MONITOR, N_MONITOR)
            res = candle_hits(sig, candles)
            if res:
                db_mark_closed(sig["id"], res, utc_now().isoformat())


# =========================
# Telegram
# =========================
bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None)

def kb_main():
    kb = InlineKeyboardMarkup()
    kb.row(InlineKeyboardButton("📊 Сигнал", callback_data="force_signal"))
    kb.row(
        InlineKeyboardButton("📈 Сегодня", callback_data="today"),
        InlineKeyboardButton("📊 Статистика", callback_data="stats"),
    )
    return kb

def owner_guard(chat_id: int) -> bool:
    return chat_id == OWNER_ID

@bot.message_handler(commands=["start"])
def on_start(msg):
    if not owner_guard(msg.from_user.id):
        bot.reply_to(msg, "⛔️ Доступ запрещён. Этот бот доступен только владельцу.")
        return
    bot.send_message(
        msg.chat.id,
        "✅ Бот запущен.\n"
        "Фильтр: 4H → 1H → 15m → 5m по EUR/USD и XAU/USD.\n"
        f"Сессия (UTC): {SESSION_START_UTC}:00–{SESSION_END_UTC}:00\n"
        f"Цель: минимум {MIN_SIGNALS_PER_DAY} сигнал(а) в день (авто BALANCED при нехватке).\n"
        "Нажми «📊 Сигнал», чтобы сделать анализ сейчас.",
        reply_markup=kb_main()
    )

@bot.message_handler(commands=["last"])
def on_last(msg):
    if not owner_guard(msg.from_user.id):
        bot.reply_to(msg, "⛔️")
        return
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
      SELECT created_utc, symbol, direction, entry, sl, tp, score, mode, status
      FROM signals ORDER BY id DESC LIMIT 1
    """)
    r = cur.fetchone()
    con.close()
    if not r:
        bot.send_message(msg.chat.id, "Пока сигналов нет.", reply_markup=kb_main())
        return
    created, sym, d, e, sl, tp, sc, mode, st = r
    bot.send_message(
        msg.chat.id,
        f"Последний сигнал:\n{created}\n{sym} {d}\nEntry {e} | SL {sl} | TP {tp}\nScore {sc} | Mode {mode} | Status {st}",
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

# =========================
# Scheduler jobs (sync wrappers)
# =========================
def job_analysis():
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

def main():
    if not BOT_TOKEN or OWNER_ID == 0 or not TD_API_KEY:
        raise RuntimeError("Set BOT_TOKEN, OWNER_ID, TD_API_KEY env vars.")
    db_init()

    sched = BackgroundScheduler(timezone="UTC")
    sched.add_job(job_analysis, "interval", minutes=AUTO_INTERVAL_MINUTES, id="analysis", replace_existing=True)
    sched.add_job(job_monitor, "interval", minutes=MONITOR_INTERVAL_MINUTES, id="monitor", replace_existing=True)
    sched.start()

    bot.infinity_polling(timeout=30, long_polling_timeout=30)

if __name__ == "__main__":
    main()
