import time
import threading
from collections import deque

import requests
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

URL = "https://api.gold-api.com/price/XAU"

app = FastAPI(title="XAUUSD Paper Bot V7")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

prices = deque(maxlen=200)

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
    "signal": "WAIT",
    "confidence": 0,
    "reason": "Initialisation",
    "capital_start": 50.0,
    "cash": 50.0,
    "equity": 50.0,
    "pnl": 0.0,
    "position": None,
    "trades": [],
}


def fetch_price():
    response = requests.get(URL, timeout=10)
    response.raise_for_status()

    data = response.json()
    price = data.get("price")

    if price is None:
        raise ValueError(f"Prix absent: {data}")

    return float(price)


def data_loop():
    while True:
        try:
            price = fetch_price()
            now = time.time()

            previous = state["price"]

            state["connected"] = True
            state["price"] = price
            state["last_tick"] = now

            if previous is None or price != previous:
                prices.append(price)

                state["unique_ticks"] += 1
                state["last_change"] = now
                state["stream_status"] = "LIVE"
                state["reason"] = "Nouveau prix reçu"

            else:
                state["duplicate_ticks"] += 1

                age = now - (state["last_change"] or now)

                if age > 180:
                    state["stream_status"] = "STALE"
                    state["reason"] = "Prix inchangé depuis plus de 3 minutes"

        except Exception as e:
            state["connected"] = False
            state["stream_status"] = "ERROR"
            state["reason"] = f"Gold API: {e}"

        time.sleep(5)


@app.on_event("startup")
def startup():
    threading.Thread(
        target=data_loop,
        daemon=True
    ).start()


@app.get("/")
def home():
    return {
        "ok": True,
        "version": "V7",
        "source": "Gold-API",
        "paper_only": True,
    }


@app.get("/health")
def health():
    return {
        "connected": state["connected"],
        "stream_status": state["stream_status"],
        "source": state["source"],
        "price": state["price"],
    }


@app.get("/status")
def status():
    return {
        **state,
        "ticks": len(prices),
    }
