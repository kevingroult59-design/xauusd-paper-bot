import os
import json
import time
import threading
from collections import deque
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import websocket


# ============================================================
# CONFIG
# ============================================================

API_KEY = os.getenv("TWELVE_DATA_API_KEY", "")
SYMBOL = "XAU/USD"

STARTING_CAPITAL = 50.0
MAX_POSITION_PCT = 0.20

# Paper risk management XAU/USD
STOP_LOSS_PCT = 0.006       # -0.60 %
TAKE_PROFIT_PCT = 0.012     # +1.20 %
TRAILING_STOP_PCT = 0.004   # 0.40 %

MIN_CONFIDENCE = 0.70

# Bougies fabriquées à partir des ticks
CANDLE_SECONDS = 60

MAX_TICKS = 3000
MAX_CANDLES = 500
MAX_TRADES = 200


# ============================================================
# API
# ============================================================

app = FastAPI(title="XAU/USD AI Paper Bot V4")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

lock = threading.Lock()

ticks = deque(maxlen=MAX_TICKS)
candles = deque(maxlen=MAX_CANDLES)

state = {
    "version": "V4",
    "paper_only": True,

    "connected": False,
    "last_tick": None,
    "price": None,

    "capital_start": STARTING_CAPITAL,
    "cash": STARTING_CAPITAL,
    "equity": STARTING_CAPITAL,
    "realized_pnl": 0.0,

    "position": None,

    "signal": "WAIT",
    "confidence": 0.0,
    "reason": "En attente du flux XAU/USD",

    "trend": "UNKNOWN",
    "volatility": 0.0,
    "support": None,
    "resistance": None,

    "bull_score": 0,
    "bear_score": 0,

    "session": "UNKNOWN",
    "fvg": None,

    "setups": 0,
    "trades": [],

    "wins": 0,
    "losses": 0,
    "closed_trades": 0,

    "peak_equity": STARTING_CAPITAL,
    "max_drawdown_pct": 0.0,
}


# ============================================================
# HELPERS
# ============================================================

def sma(values, period):
    if len(values) < period:
        return None

    return sum(values[-period:]) / period


def ema(values, period):
    if not values:
        return None

    alpha = 2 / (period + 1)
    result = values[0]

    for value in values[1:]:
        result = alpha * value + (1 - alpha) * result

    return result


def pct_change(a, b):
    if a == 0:
        return 0.0

    return b / a - 1.0


def market_session():
    hour = datetime.now(timezone.utc).hour

    if 0 <= hour < 7:
        return "ASIA"

    if 7 <= hour < 13:
        return "LONDON"

    if 13 <= hour < 21:
        return "NEW YORK"

    return "LATE US"


def add_trade(side, price, value, pnl, reason):
    item = {
        "time": time.time(),
        "side": side,
        "price": price,
        "value": value,
        "pnl": pnl,
        "reason": reason,
    }

    state["trades"].insert(0, item)
    state["trades"] = state["trades"][:MAX_TRADES]


# ============================================================
# CANDLES
# ============================================================

def update_candle(timestamp, price):
    bucket = int(timestamp // CANDLE_SECONDS) * CANDLE_SECONDS

    if not candles or candles[-1]["time"] != bucket:
        candles.append({
            "time": bucket,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "ticks": 1,
        })

    else:
        candle = candles[-1]

        candle["high"] = max(candle["high"], price)
        candle["low"] = min(candle["low"], price)
        candle["close"] = price
        candle["ticks"] += 1


# ============================================================
# MARKET ANALYSIS
# ============================================================

def calculate_market_analysis():
    closed = list(candles)

    if len(closed) < 20:
        return None

    closes = [c["close"] for c in closed]
    highs = [c["high"] for c in closed]
    lows = [c["low"] for c in closed]

    ema_fast = ema(closes[-20:], 8)
    ema_slow = ema(closes[-40:], 21)

    momentum_3 = pct_change(closes[-4], closes[-1])
    momentum_8 = pct_change(closes[-9], closes[-1])

    ranges = []

    for candle in closed[-20:]:
        if candle["open"]:
            ranges.append(
                (candle["high"] - candle["low"]) / candle["open"]
            )

    volatility = (
        sum(ranges) / len(ranges)
        if ranges
        else 0.0
    )

    support = min(lows[-20:])
    resistance = max(highs[-20:])

    price = closes[-1]

    support_distance = (
        (price - support) / price
