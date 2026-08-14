import os, json, time, threading
from collections import deque
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import websocket

API_KEY=os.getenv("TWELVE_DATA_API_KEY","")
SYMBOL="XAU/USD"
app=FastAPI(title="XAU/USD Paper Bot")
app.add_middleware(CORSMiddleware,allow_origins=["*"],allow_methods=["*"],allow_headers=["*"])
lock=threading.Lock()
prices=deque(maxlen=600)
state={"connected":False,"last_tick":None,"price":None,"capital_start":50.0,"cash":50.0,
"equity":50.0,"position":None,"realized_pnl":0.0,"signal":"WAIT","confidence":0.0,
"reason":"En attente du flux XAU/USD","trades":[],"setups":0}

def trade(side,price,value,pnl,reason):
    state["trades"].insert(0,{"time":time.time(),"side":side,"price":price,"value":value,"pnl":pnl,"reason":reason})
    state["trades"]=state["trades"][:100]

def open_pos(side,price,conf):
    if state["position"]: return
    amount=min(state["cash"]*.20,10.0)
    if amount<1:return
    state["cash"]-=amount
    state["position"]={"side":side,"entry":price,"margin":amount,"peak":price}
    state["setups"]+=1
    trade(side,price,amount,0.0,f"Signal {conf:.0%}")

def close_pos(price,reason):
    p=state["position"]
    if not p:return
    pct=(price/p["entry"]-1)*(1 if p["side"]=="BUY" else -1)
    value=p["margin"]*(1+pct); pnl=value-p["margin"]
    state["cash"]+=value; state["realized_pnl"]+=pnl
    trade("CLOSE",price,value,pnl,reason)
    state["position"]=None; state["equity"]=state["cash"]

def strategy(price):
    prices.append(price); vals=list(prices)
    if len(vals)<30:
        state.update(signal="WAIT",confidence=0.0,reason=f"Collecte ({len(vals)}/30 ticks)")
        return
    short=vals[-8:]; long=vals[-30:]
    ms=short[-1]/short[0]-1; ml=long[-1]/long[0]-1
    avg=sum(abs(long[i]/long[i-1]-1) for i in range(1,len(long)))/(len(long)-1)
    threshold=max(avg*2,0.00012)
    p=state["position"]
    if p:
        p["peak"]=max(p["peak"],price) if p["side"]=="BUY" else min(p["peak"],price)
        pct=(price/p["entry"]-1)*(1 if p["side"]=="BUY" else -1)
        state["equity"]=state["cash"]+p["margin"]*(1+pct)
        if pct<=-0.01: close_pos(price,"Stop-loss -1%")
        elif pct>=0.015: close_pos(price,"Take-profit +1.5%")
        elif p["side"]=="BUY" and price<=p["peak"]*.995 and pct>0: close_pos(price,"Trailing 0.5%")
        elif p["side"]=="SELL" and price>=p["peak"]*1.005 and pct>0: close_pos(price,"Trailing 0.5%")
        return
    if ms>threshold and ml>0:
        c=min(.95,.62+abs(ms)/threshold*.08); state.update(signal="BUY",confidence=c,reason=f"Momentum +{ms*100:.3f}%")
        if c>=.70: open_pos("BUY",price,c)
    elif ms<-threshold and ml<0:
        c=min(.95,.62+abs(ms)/threshold*.08); state.update(signal="SELL",confidence=c,reason=f"Momentum {ms*100:.3f}%")
        if c>=.70: open_pos("SELL",price,c)
    else: state.update(signal="HOLD",confidence=.55,reason=f"Pas de setup | momentum {ms*100:.3f}%")

def on_open(ws):
    state["connected"]=True
    ws.send(json.dumps({"action":"subscribe","params":{"symbols":SYMBOL}}))
def on_message(ws,msg):
    try:
        d=json.loads(msg)
        if d.get("event")=="price" and d.get("symbol")==SYMBOL:
            price=float(d["price"])
            with lock:
                state["price"]=price; state["last_tick"]=time.time(); strategy(price)
    except Exception as e: state["reason"]=f"Erreur flux: {e}"
def on_close(ws,*args): state["connected"]=False
def loop():
    while True:
        if not API_KEY:
            state["reason"]="TWELVE_DATA_API_KEY absente"; time.sleep(10); continue
        try:
            ws=websocket.WebSocketApp(f"wss://ws.twelvedata.com/v1/quotes/price?apikey={API_KEY}",on_open=on_open,on_message=on_message,on_close=on_close)
            ws.run_forever(ping_interval=20,ping_timeout=10)
        except Exception as e: state["reason"]=f"Reconnexion: {e}"
        time.sleep(5)
@app.on_event("startup")
def startup(): threading.Thread(target=loop,daemon=True).start()
@app.get("/")
def root(): return {"ok":True,"service":"XAU/USD paper bot","paper_only":True}
@app.get("/status")
def status():
    with lock:return dict(state)
@app.get("/health")
def health(): return {"ok":True,"connected":state["connected"],"last_tick":state["last_tick"]}
