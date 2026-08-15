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
    "subscribed": False,
    "price": None,
    "last_tick": None,
    "last_change": None,
    "capital_start": 50.0,
    "cash": 50.0,
    "equity": 50.0,
    "pnl": 0.0,
    "position": None,
    "signal": "WAIT",
    "confidence": 0,
    "reason": "En attente du marché",
    "support": None,
    "resistance": None,
    "unique_ticks": 0,
    "duplicate_ticks": 0,
    "stream_status": "STARTING",
    "trades": [],
}


def analyse():

    if len(prices) < 20:
        state["signal"] = "WAIT"
        state["confidence"] = 0
        state["reason"] = f"Collecte vraie variation {len(prices)}/20"
        return

    p = list(prices)

    price_range = max(p[-20:]) - min(p[-20:])

    if price_range <= 0:
        state["signal"] = "WAIT"
        state["confidence"] = 0
        state["reason"] = "Flux plat : aucun trade autorisé"
        return

    short = sum(p[-5:]) / 5
    medium = sum(p[-10:]) / 10
    long = sum(p[-20:]) / 20

    support = min(p[-20:])
    resistance = max(p[-20:])

    state["support"] = support
    state["resistance"] = resistance

    bull = 0
    bear = 0

    if short > medium:
        bull += 2
    elif short < medium:
        bear += 2

    if medium > long:
        bull += 2
    elif medium < long:
        bear += 2

    if p[-1] > p[-5]:
        bull += 1
    elif p[-1] < p[-5]:
        bear += 1

    total = bull + bear

    if total == 0:
        state["signal"] = "WAIT"
        state["confidence"] = 0
        state["reason"] = "Aucune direction claire"
        return

    if bull > bear:
        signal = "BUY"
        confidence = round((bull / total) * 100)

    elif bear > bull:
        signal = "SELL"
        confidence = round((bear / total) * 100)

    else:
        signal = "WAIT"
        confidence = 50

    state["signal"] = signal
    state["confidence"] = min(confidence, 85)
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

        if move >= 0.01:
            close_position(price, "TAKE PROFIT")

        elif move <= -0.005:
            close_position(price, "STOP LOSS")

        return

    if state["confidence"] < 70:
        return

    if state["signal"] not in ("BUY", "SELL"):
        return

    if state["stream_status"] != "LIVE":
        return

    open_position(state["signal"], price)


def open_position(side, price):

    if state["position"]:
        return

    amount = min(state["cash"] * 0.20, 10.0)

    if amount < 1:
        return

    state["cash"] -= amount

    state["position"] = {
        "side": side,
        "entry": price,
        "amount": amount,
    }

    state["trades"].insert(
        0,
        {
            "time": time.time(),
            "type": side,
            "price": price,
            "amount": amount,
        },
    )


def close_position(price, reason):

    position = state["position"]

    if not position:
        return

    entry = position["entry"]
    side = position["side"]
    amount = position["amount"]

    move = (price / entry) - 1

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
        },
    )


def heartbeat_loop(ws):

    while state["connected"]:

        try:
            ws.send(
                json.dumps(
                    {
                        "action": "heartbeat"
                    }
                )
            )

        except Exception:
            break

        time.sleep(10)


def on_open(ws):

    state["connected"] = True
    state["stream_status"] = "CONNECTED"

    ws.send(
        json.dumps(
            {
                "action": "subscribe",
                "params": {
                    "symbols": SYMBOL
                },
            }
        )
    )

    threading.Thread(
        target=heartbeat_loop,
        args=(ws,),
        daemon=True,
    ).start()


def on_message(ws, message):

    try:

        data = json.loads(message)

        event = data.get("event")

        if event == "subscribe-status":

            status = data.get("status")

            state["subscribed"] = status == "ok"
            state["stream_status"] = (
                "SUBSCRIBED"
                if status == "ok"
                else "SUBSCRIBE_ERROR"
            )

            state["reason"] = f"Subscribe status: {status}"

            return


        if event != "price":
            return

        if data.get("symbol") != SYMBOL:
            return

        price = float(data["price"])

        with lock:

            previous = state["price"]

            state["price"] = price
            state["last_tick"] = time.time()
            state["connected"] = True

            if previous is not None and price == previous:

                state["duplicate_ticks"] += 1

                if state["last_change"]:
                    age = time.time() - state["last_change"]

                    if age > 120:
                        state["stream_status"] = "STALE"
                        state["signal"] = "WAIT"
                        state["confidence"] = 0
                        state["reason"] = "Prix inchangé depuis plus de 2 minutes"

                return


            prices.append(price)

            state["unique_ticks"] += 1
            state["last_change"] = time.time()
            state["stream_status"] = "LIVE"

            analyse()

    except Exception as e:

        state["reason"] = f"Erreur message: {e}"


def on_error(ws, error):

    state["connected"] = False
    state["stream_status"] = "ERROR"
    state["reason"] = f"Erreur flux: {error}"


def on_close(ws, *args):

    state["connected"] = False
    state["subscribed"] = False
    state["stream_status"] = "RECONNECTING"
    state["reason"] = "Connexion fermée, reconnexion..."


def websocket_loop():

    while True:

        if not API_KEY:

            state["reason"] = "Clé Twelve Data absente"
            state["stream_status"] = "NO_API_KEY"

            time.sleep(10)

            continue

        try:

            url = (
                "wss://ws.twelvedata.com/v1/quotes/price"
                f"?apikey={API_KEY}"
            )

            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10,
            )

        except Exception as e:

            state["reason"] = str(e)

        time.sleep(5)


@app.on_event("startup")
def startup():

    threading.Thread(
        target=websocket_loop,
        daemon=True,
    ).start()


@app.get("/")
def home():

    return {
        "ok": True,
        "bot": "XAUUSD Paper Bot",
        "paper_only": True,
    }


@app.get("/status")
def status():

    with lock:

        return {
            **state,
            "ticks": len(prices),
        }


@app.get("/health")
def health():

    return {
        "ok": True,
        "connected": state["connected"],
        "subscribed": state["subscribed"],
        "stream_status": state["stream_status"],
        "price": state["price"],
    }
