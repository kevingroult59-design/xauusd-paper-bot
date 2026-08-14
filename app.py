import os, json, time, threading
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
    "support": None,
    "resistance": None,
    "trades": [],
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

    bull += 2 if short > medium else 0
    bear += 2 if short <= medium else 0

    bull += 2 if medium > long else 0
    bear += 2 if medium <= long else 0

    bull += 1 if p[-1] > p[-5] else 0
    bear += 1 if p[-1] <= p[-5] else 0

    total = bull + bear

    if bull > bear:
        signal = "BUY"
        confidence = round(bull / total * 100)
    elif bear > bull:
        signal = "SELL"
        confidence = round(bear / total * 100)
    else:
        signal = "WAIT"
        confidence = 50

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

        if move >= 0.01:
            close_position(price, "TAKE PROFIT")
        elif move <= -0.005:
            close_position(price, "STOP LOSS")

        return

    if state["confidence"] >= 70 and state["signal"] in ("BUY", "SELL"):
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

def on_open(ws):
    state["connected"] = True
    ws.send(
        json.dumps(
            {
                "action": "subscribe",
                "params": {"symbols": SYMBOL},
            }
        )
    )

def on_message(ws, message):
    try:
        data = json.loads(message)

        if data.get("event") != "price":
            return

        if data.get("symbol") != SYMBOL:
            return

        price = float(data["price"])

        with lock:
            prices.append(price)
            state["price"] = price
            state["last_tick"] = time.time()
            state["connected"] = True
            analyse()

    except Exception as e:
        state["reason"] = str(e)

def on_error(ws, error):
    state["connected"] = False
    state["reason"] = f"Erreur flux: {error}"

def on_close(ws, *args):
    state["connected"] = False
    state["reason"] = "Reconnexion..."

def websocket_loop():
    while True:
        if not API_KEY:
            state["reason"] = "Clé Twelve Data absente"
            time.sleep(10)
            continue

        try:
            url = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={API_KEY}"

            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )

            ws.run_forever()

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
        "price": state["price"],
    }
