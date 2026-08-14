import os
import json
import time
import threading
from collections import deque

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import websocket

API_KEY = os.getenv("TWELVE_DATA_API_KEY", "")
SYMBOL = "XAU/USD"

app = FastAPI(title="XAUUSD Paper Bot")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

prices = deque(maxlen=300)
lock = threading.Lock()

state = {
    "connected": False,
    "price": None,
    "last_tick": None,
    "capital_start": 50.0,
    "cash": 50.0,
    "equity": 50.0,
    "pnl": 0.0,
    "position": None,
    "signal": "WAIT",
    "confidence": 0,
    "reason": "En attente du marché",
    "trades": []
}


def analyse():
    if len(prices) < 30:
        state["signal"] = "WAIT"
        state["confidence"] = 0
        state["reason"] = f"Collecte des données {len(prices)}/30"
        return

    p = list(prices)

    short = sum(p[-5:]) / 5
    medium = sum(p[-15:]) / 15
    long = sum(p[-30:]) / 30

    support = min(p[-30:])
    resistance = max(p[-30:])

    bull = 0
    bear = 0

    if short > medium:
        bull += 2
    else:
        bear += 2

    if medium > long:
        bull += 2
    else:
        bear += 2

    if p[-1] > p[-5]:
        bull += 1
    else:
        bear += 1

    total = bull + bear

    if bull > bear:
        confidence = round(bull / total * 100)
        signal = "BUY"
    elif bear > bull:
        confidence = round(bear / total * 100)
        signal = "SELL"
    else:
        confidence = 50
        signal = "WAIT"

    state["signal"] = signal
    state["confidence"] = confidence
    state["support"] = support
    state["resistance"] = resistance
    state["reason"] = f"Bull {bull} / Bear {bear}"

    trade_logic(p[-1])


def trade_logic(price):

    position = state["position"]

    if position:
        entry = position["entry"]
        side = position["side"]
        amount = position["amount"]

        move = (price / entry) - 1

        if side == "SELL":
            move *= -1

        state["equity"] = state["cash"] + amount * (1 + move)

        # TP +1 %
        if move >= 0.01:
            close_position(price, "TAKE PROFIT")

        # SL -0.5 %
        elif move <= -0.005:
            close_position(price, "STOP LOSS")

        return

    if state["confidence"] < 70:
        return

    if state["signal"] in ["BUY", "SELL"]:
        open_position(
            state["signal"],
            price
        )


def open_position(side, price):

    if state["position"]:
        return

    amount = min(
        state["cash"] * 0.20,
        10
    )

    if amount < 1:
        return

    state["cash"] -= amount

    state["position"] = {
        "side": side,
        "entry": price,
        "amount": amount
    }

    state["trades"].insert(
        0,
        {
            "time": time.time(),
            "type": side,
            "price": price,
            "amount": amount
        }
    )


def close_position(price, reason):

    position = state["position"]

    if not position:
        return

    entry = position["entry"]
    side = position["side"]
    amount = position["amount"]

    move = (price
