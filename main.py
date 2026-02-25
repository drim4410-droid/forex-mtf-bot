import os
import asyncio
import hashlib
from datetime import datetime, timezone, timedelta

import aiohttp
import pandas as pd
import numpy as np

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.filters import CommandStart
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# =========================
# ENV
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
TD_API_KEY = os.getenv("TD_API_KEY", "")

# Настройки стратегии (строже = меньше сигналов, выше качество)
SCORE_THRESHOLD = float(os.getenv("SCORE_THRESHOLD", "8.0"))  # 0..10 (строгий порог)
RR = float(os.getenv("RR", "2.0"))                            # TP = SL * RR
SL_ATR_MULT = float(os.getenv("SL_ATR_MULT", "1.8"))          # SL = ATR(15m) * mult

# Ограничения, чтобы бот не спамил
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "180"))  # 3 часа на инструмент
AUTO_INTERVAL_MINUTES = int(os.getenv("AUTO_INTERVAL_MINUTES", "60"))

INSTRUMENTS = ["EUR/USD", "XAU/USD"]  # TwelveData symbols

# Таймфреймы top-down
TF_4H = "4h"
TF_1H = "1h"
TF_15 = "15min"
TF_5 = "5min"

CANDLES_4H = 350
CANDLES_1H = 500
CANDLES_15 = 800
CANDLES_5 = 1200

# =========================
# Security
# =========================
def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID

def deny_text() -> str:
    return "⛔️ Доступ запрещён. Этот бот доступен только владельцу."

# =========================
# Indicators
# =========================
def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()
    rs = avg_gain / (avg_loss.replace(0, np.nan))
    return 100 - (100 / (1 + rs))

def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    prev_close = df["close"].shift(1)
    tr = pd.concat([
        (df["high"] - df["low"]),
        (df["high"] - prev_close).abs(),
        (df["low"] - prev_close).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1/period, adjust=False).mean()

def with_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ema50"] = ema(df["close"], 50)
    df["ema200"] = ema(df["close"], 200)
    df["rsi14"] = rsi(df["close"], 14)
    df["atr14"] = atr(df, 14)
    return df

# =========================
# TwelveData fetch
# =========================
async def fetch_td_candles(session: aiohttp.ClientSession, symbol: str, interval: str, outputsize: int) -> pd.DataFrame:
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

    # Ошибки TD приходят как {"status":"error","message":"..."}
    if isinstance(data, dict) and data.get("status") == "error":
        raise RuntimeError(f"TwelveData error: {data.get('message')}")

    values = data.get("values")
    if not values:
        raise RuntimeError(f"No candles for {symbol} {interval}")

    rows = []
    for v in values:
        # TD возвращает строки; time может быть "YYYY-MM-DD HH:MM:SS"
        rows.append({
            "time": pd.to_datetime(v["datetime"]),
            "open": float(v["open"]),
            "high": float(v["high"]),
            "low": float(v["low"]),
            "close": float(v["close"]),
            "volume": float(v.get("volume", 0) or 0),
        })

    df = pd.DataFrame(rows).sort_values("time").reset_index(drop=True)
    return df

# =========================
# Multi-timeframe logic
# =========================
def bias_4h(df4: pd.DataFrame):
    df4 = with_indicators(df4)
    last = df4.iloc[-1]
    prev = df4.iloc[-2]

    price = float(last["close"])
    ema50 = float(last["ema50"])
    ema200 = float(last["ema200"])
    slope_up = float(last["ema50"]) > float(prev["ema50"])
    slope_down = float(last["ema50"]) < float(prev["ema50"])

    # Фильтр "каша"
    if np.isnan(ema50) or np.isnan(ema200):
        return None

    if ema50 > ema200 and price > ema50 and slope_up:
        return "LONG"
    if ema50 < ema200 and price < ema50 and slope_down:
        return "SHORT"
    return None

def setup_1h(df1: pd.DataFrame, direction: str) -> bool:
    df1 = with_indicators(df1)
    last = df1.iloc[-1]
    price = float(last["close"])
    ema50 = float(last["ema50"])
    r = float(last["rsi14"]) if not np.isnan(float(last["rsi14"])) else 50.0
    a = float(last["atr14"]) if not np.isnan(float(last["atr14"])) else 0.0

    # фильтр боковика/тишины: ATR должен быть не слишком маленький относительно цены
    vol_ok = a > 0 and (a / price) > 0.0004

    if direction == "LONG":
        return (price > ema50) and (50 <= r <= 70) and vol_ok
    else:
        return (price < ema50) and (30 <= r <= 50) and vol_ok

def trigger_15m(df15: pd.DataFrame, direction: str) -> bool:
    df15 = with_indicators(df15)
    last = df15.iloc[-1]
    prev = df15.iloc[-2]

    price = float(last["close"])
    ema50 = float(last["ema50"])
    slope_up = float(last["ema50"]) > float(prev["ema50"])
    slope_down = float(last["ema50"]) < float(prev["ema50"])

    r = float(last["rsi14"]) if not np.isnan(float(last["rsi14"])) else 50.0

    if direction == "LONG":
        return (price > ema50) and slope_up and (45 <= r <= 70)
    else:
        return (price < ema50) and slope_down and (30 <= r <= 55)

def confirm_5m(df5: pd.DataFrame, direction: str) -> bool:
    df5 = with_indicators(df5)
    last = df5.iloc[-1]
    prev = df5.iloc[-2]

    price = float(last["close"])
    ema50 = float(last["ema50"])
    slope_up = float(last["ema50"]) > float(prev["ema50"])
    slope_down = float(last["ema50"]) < float(prev["ema50"])

    r = float(last["rsi14"]) if not np.isnan(float(last["rsi14"])) else 50.0

    if direction == "LONG":
        return (price > ema50) and slope_up and (50 <= r <= 72)
    else:
        return (price < ema50) and slope_down and (28 <= r <= 50)

def build_signal(symbol: str, direction: str, df5: pd.DataFrame, df15: pd.DataFrame):
    # Entry = последняя цена M5 (как "market now")
    df5i = with_indicators(df5)
    df15i = with_indicators(df15)

    entry = float(df5i.iloc[-1]["close"])
    t = df5i.iloc[-1]["time"].to_pydatetime().replace(tzinfo=timezone.utc)

    # SL/TP по ATR(15m)
    atr15 = float(df15i.iloc[-1]["atr14"])
    if np.isnan(atr15) or atr15 <= 0:
        return None

    sl_dist = atr15 * SL_ATR_MULT

    if direction == "LONG":
        sl = entry - sl_dist
        tp = entry + sl_dist * RR
    else:
        sl = entry + sl_dist
        tp = entry - sl_dist * RR

    # Округление (EURUSD 5 знаков, XAUUSD 2)
    digits = 5 if "EUR" in symbol else 2

    # Score 0..10: 4H(4) + 1H(2) + 15m(2) + 5m(2) = 10
    score = 10.0  # сюда попадаем только если все фильтры прошли
    return {
        "symbol": symbol,
        "direction": direction,
        "entry": round(entry, digits),
        "sl": round(sl, digits),
        "tp": round(tp, digits),
        "score": round(score, 1),
        "time": t,
    }

def format_signal(sig: dict) -> str:
    ts = sig["time"].strftime("%Y-%m-%d %H:%M UTC")
    return (
        f"📌 СИГНАЛ ({ts})\n"
        f"Инструмент: {sig['symbol']}\n"
        f"Направление: {sig['direction']} (MARKET)\n"
        f"Entry: {sig['entry']}\n"
        f"TP: {sig['tp']}\n"
        f"SL: {sig['sl']}\n"
        f"Оценка (0–10): {sig['score']}\n\n"
        f"Фильтры: 4H bias ✅  |  1H setup ✅  |  15m trigger ✅  |  5m confirm ✅\n"
        f"⚠️ Это оценка по правилам, не гарантия."
    )

# =========================
# Anti-spam state
# =========================
last_sent_at = {}      # symbol -> datetime
last_signal_hash = {}  # symbol -> hash str

def make_hash(sig: dict) -> str:
    raw = f"{sig['symbol']}|{sig['direction']}|{sig['entry']}|{sig['sl']}|{sig['tp']}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

def cooldown_ok(symbol: str) -> bool:
    now = datetime.now(timezone.utc)
    prev = last_sent_at.get(symbol)
    if not prev:
        return True
    return (now - prev) >= timedelta(minutes=COOLDOWN_MINUTES)

# =========================
# Telegram
# =========================
kb_main = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="📊 Сигнал", callback_data="force_signal")],
])

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

@dp.message(CommandStart())
async def start(m: Message):
    if not is_owner(m.from_user.id):
        await m.answer(deny_text())
        return
    await m.answer(
        "✅ Бот запущен.\n"
        "Авто-анализ каждый час (4H/1H/15m/5m) по EUR/USD и XAU/USD.\n"
        "Нажми «📊 Сигнал», чтобы сделать анализ прямо сейчас.",
        reply_markup=kb_main
    )

@dp.callback_query(F.data == "force_signal")
async def force_signal(cb: CallbackQuery):
    if not is_owner(cb.from_user.id):
        await cb.answer("⛔️", show_alert=True)
        return
    await cb.answer("Анализирую...", show_alert=False)
    text = await run_analysis(send_only_signals=False)
    await cb.message.answer(text, reply_markup=kb_main)

async def analyze_symbol(session: aiohttp.ClientSession, symbol: str):
    # 1) 4H bias
    df4 = await fetch_td_candles(session, symbol, TF_4H, CANDLES_4H)
    direction = bias_4h(df4)
    if direction is None:
        return None

    # 2) 1H setup
    df1 = await fetch_td_candles(session, symbol, TF_1H, CANDLES_1H)
    if not setup_1h(df1, direction):
        return None

    # 3) 15m trigger
    df15 = await fetch_td_candles(session, symbol, TF_15, CANDLES_15)
    if not trigger_15m(df15, direction):
        return None

    # 4) 5m confirm
    df5 = await fetch_td_candles(session, symbol, TF_5, CANDLES_5)
    if not confirm_5m(df5, direction):
        return None

    sig = build_signal(symbol, direction, df5, df15)
    if not sig:
        return None
    return sig

async def run_analysis(send_only_signals: bool = True) -> str:
    try:
        async with aiohttp.ClientSession() as session:
            best = None
            for sym in INSTRUMENTS:
                sig = await analyze_symbol(session, sym)
                if sig and sig["score"] >= SCORE_THRESHOLD:
                    if best is None or sig["score"] > best["score"]:
                        best = sig

            if not best:
                if send_only_signals:
                    return ""  # молчим в авто-режиме
                return f"🔎 Анализ выполнен ({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')})\nСильных сетапов нет (порог {SCORE_THRESHOLD}/10)."

            # анти-повтор + кулдаун
            h = make_hash(best)
            sym = best["symbol"]
            if (not cooldown_ok(sym)) and send_only_signals:
                return ""
            if last_signal_hash.get(sym) == h and send_only_signals:
                return ""

            last_signal_hash[sym] = h
            last_sent_at[sym] = datetime.now(timezone.utc)
            return format_signal(best)

    except Exception as e:
        if send_only_signals:
            # в авто-режиме можно не спамить ошибками
            return ""
        return f"⚠️ Ошибка анализа: {e}"

async def scheduled_job():
    text = await run_analysis(send_only_signals=True)
    if text:
        await bot.send_message(chat_id=OWNER_ID, text=text, reply_markup=kb_main)

async def main():
    if not BOT_TOKEN or OWNER_ID == 0 or not TD_API_KEY:
        raise RuntimeError("Set BOT_TOKEN, OWNER_ID, TD_API_KEY env vars.")

    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(scheduled_job, "interval", minutes=AUTO_INTERVAL_MINUTES, id="hourly", replace_existing=True)
    scheduler.start()

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
