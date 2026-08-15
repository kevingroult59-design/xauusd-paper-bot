import time
import threading
from collections import deque

import requests
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


# =========================
# CONFIG
# =========================

GOLD_API_URL = "https://api.gold-api.com/price/XAU"

START_CAPITAL = 50.0
POSITION_SIZE = 0.20

STOP_LOSS = 0.005
TAKE_PROFIT = 0.010

POLL_SECONDS = 5


# =========================
# APP
# =========================

app = FastAPI(title="XAUUSD Paper Bot V7")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

lock = threading.Lock()

prices = deque(maxlen=500)

state = {
    "version": "V7",
    "source": "Gold-API",
    "paper_only": True,

    "connected": False,
    "stream_status": "STARTING",

    "price": None,
    "last_tick": None,
    "last_change": None,

    "unique_ticks": 0,
    "duplicate_ticks": 0,

    "capital_start": START_CAPITAL,
    "cash": START_CAPITAL,
    "equity": START_CAPITAL,
    "pnl": 0.0,

    "position": None,

    "signal": "WAIT",
    "confidence": 0,
    "reason": "Initialisation",

    "support": None,
    "resistance": None,

    "trades": [],
}


# =========================
# MARKET ANALYSIS
# =========================

def analyse():

    if len(prices) < 20:

        state["signal"] = "WAIT"
        state["confidence"] = 0

        state["reason"] = (
            f"Collecte variations "
            f"{len(prices)}/20"
        )

        return

    p = list(prices)

    recent = p[-20:]

    support = min(recent)
    resistance = max(recent)

    state["support"] = support
    state["resistance"] = resistance

    price_range = resistance - support

    if price_range <= 0:

        state["signal"] = "WAIT"
        state["confidence"] = 0
        state["reason"] = "Marché sans variation"

        return

    fast = sum(p[-5:]) / 5
    medium = sum(p[-10:]) / 10
    slow = sum(p[-20:]) / 20

    bull = 0
    bear = 0

    if fast > medium:
        bull += 2

    elif fast < medium:
        bear += 2


    if medium > slow:
        bull += 2

    elif medium < slow:
        bear += 2


    if p[-1] > p[-5]:
        bull += 1

    elif p[-1] < p[-5]:
        bear += 1


    total = bull + bear

    if total == 0:

        state["signal"] = "WAIT"
        state["confidence"] = 0
        state["reason"] = "Direction neutre"

        return


    if bull > bear:

        signal = "BUY"
        confidence = round(
            bull / total * 100
        )

    elif bear > bull:

        signal = "SELL"
        confidence = round(
            bear / total * 100
        )

    else:

        signal = "WAIT"
        confidence = 50


    # On évite les faux "100%"
    confidence = min(confidence, 85)

    state["signal"] = signal
    state["confidence"] = confidence

    state["reason"] = (
        f"Bull {bull} / Bear {bear}"
    )

    trade_logic(p[-1])


# =========================
# PAPER TRADING
# =========================

def trade_logic(price):

    position = state["position"]


    # POSITION OUVERTE
    if position:

        entry = position["entry"]
        side = position["side"]
        amount = position["amount"]

        move = price / entry - 1

        if side == "SELL":
            move *= -1


        state["equity"] = (
            state["cash"]
            + amount * (1 + move)
        )


        if move <= -STOP_LOSS:

            close_position(
                price,
                "STOP LOSS"
            )

            return


        if move >= TAKE_PROFIT:

            close_position(
                price,
                "TAKE PROFIT"
            )

            return


        return


    # PAS DE POSITION
    if state["stream_status"] != "LIVE":
        return


    if state["confidence"] < 70:
        return


    if state["signal"] not in (
        "BUY",
        "SELL"
    ):
        return


    open_position(
        state["signal"],
        price
    )


def open_position(side, price):

    if state["position"]:
        return


    amount = min(
        state["cash"] * POSITION_SIZE,
        10.0
    )


    if amount < 1:
        return


    state["cash"] -= amount


    state["position"] = {
        "side": side,
        "entry": price,
        "amount": amount,
        "opened_at": time.time(),
    }


    state["trades"].insert(
        0,
        {
            "time": time.time(),
            "type": side,
            "price": price,
            "amount": amount,
        }
    )


def close_position(price, reason):

    position = state["position"]


    if not position:
        return


    entry = position["entry"]
    side = position["side"]
    amount = position["amount"]


    move = price / entry - 1


    if side == "SELL":
        move *= -1


    value = amount * (1 + move)

    pnl = value - amount


    state["cash"] += value

    state["pnl"] += pnl

    state["equity"] = state["cash"]


    state["position"] = None


    state["trades"].insert(
        0,
        {
            "time": time.time(),
            "type": "CLOSE",
            "price": price,
            "pnl": pnl,
            "reason": reason,
        }
    )


# =========================
# GOLD API
# =========================

def fetch_gold():

    response = requests.get(
        GOLD_API_URL,
        timeout=10
    )

    response.raise_for_status()

    data = response.json()


    price = data.get("price")


    if price is None:
        raise ValueError(
            f"Prix absent: {data}"
        )


    return float(price)


def data_loop():

    while True:

        try:

            price = fetch_gold()

            now = time.time()


            with lock:

                previous = state["price"]

                state["connected"] = True

                state["price"] = price

                state["last_tick"] = now


                # PREMIER PRIX
                if previous is None:

                    prices.append(price)

                    state["unique_ticks"] += 1

                    state["last_change"] = now

                    state["stream_status"] = "COLLECTING"

                    state["reason"] = (
                        "Premier prix reçu"
                    )


                # PRIX IDENTIQUE
                elif price == previous:

                    state["duplicate_ticks"] += 1


                    if state["
