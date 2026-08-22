import os
import io
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager
import pandas as pd
import numpy as np
import ccxt.async_support as ccxt
import aiohttp
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel
import uvicorn

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

TURKEY_TZ = timezone(timedelta(hours=3))

def get_now_str():
    return datetime.now(TURKEY_TZ).strftime("%H:%M:%S")

def get_now_datetime():
    return datetime.now(TURKEY_TZ)

CSV_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "daily_reports")
os.makedirs(CSV_DIR, exist_ok=True)

system_state = {
    "initial_balance": 1000.0,
    "total_balance": 1000.0,
    "risk_pct": 5.0,
    "leverage": 50,
    "margin_mode": "ISOLATED",
    "max_open_positions": 5,
    "max_total_margin_pct": 50.0,
    "btc_regime": "YÜKLENİYOR...",
    "btc_15m_change": 0.0,
    "btc_shock_lock": False,
    "btc_shock_reason": "",
    "fear_and_greed": {"value": 55, "classification": "Nötr"},
    "sentiment_data": {
        "btc_rsi": 50.0,
        "btc_volume_24h": "$0",
        "market_bias": "NÖTR"
    },
    "scanned_count": 0,
    "last_scan_time": "-",
    "active_positions": [],
    "trade_history": [],
    "radar_symbols": [],
    "api_settings": {
        "exchange": "BINANCE",
        "mode": "TESTNET",
        "api_key": "",
        "api_secret": "",
        "auto_trade": False
    },
    "equity_curve": [{"time": int(get_now_datetime().timestamp()), "value": 1000.0}],
    "logs": []
}

EXCLUDED_KEYWORDS = [
    'NVDA', 'GOOGL', 'AAPL', 'TSLA', 'MSFT', 'AMZN', 'META', 'NFLX', 'AMD', 'COIN',
    'BABA', 'PLTR', 'SOXS', 'SOXL', 'QQQ', 'SPY', 'WDC', 'DELL', 'IONQ', 'GLW', 'BIRB'
]

def add_log(msg: str):
    ts = get_now_str()
    system_state["logs"].insert(0, f"[{ts}] {msg}")
    if len(system_state["logs"]) > 60:
        system_state["logs"].pop()

def calculate_indicators(df):
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / (loss + 1e-9)
    df['rsi'] = 100 - (100 / (1 + rs))

    df['ema20'] = df['close'].ewm(span=20, adjust=False).mean()
    df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()

    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df['atr'] = tr.rolling(window=14).mean()
    df['vol_ma'] = df['volume'].rolling(window=20).mean()
    return df

def compute_position_metrics(entry, sl):
    balance = system_state["total_balance"]
    risk_pct = system_state["risk_pct"] / 100.0
    leverage = system_state["leverage"]

    risk_amount = balance * risk_pct
    price_risk_pct = abs(entry - sl) / entry
    
    if price_risk_pct == 0:
        return 0, 0, 0

    position_notional = risk_amount / price_risk_pct
    margin_required = position_notional / leverage

    return round(position_notional, 2), round(margin_required, 2), round(risk_amount, 2)

async def fetch_fear_greed():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.alternative.me/fng/?limit=1", timeout=3) as res:
                if res.status == 200:
                    data = await res.json()
                    item = data['data'][0]
                    system_state["fear_and_greed"] = {
                        "value": int(item['value']),
                        "classification": item['value_classification']
                    }
    except Exception:
        pass

async def update_btc_metrics(exchange):
    try:
        candles_15m = await exchange.fetch_ohlcv('BTC/USDT:USDT', timeframe='15m', limit=10)
        candles_1h = await exchange.fetch_ohlcv('BTC/USDT:USDT', timeframe='1h', limit=40)
        
        df_15m = pd.DataFrame(candles_15m, columns=['t', 'open', 'high', 'low', 'close', 'volume'])
        df_1h = calculate_indicators(pd.DataFrame(candles_1h, columns=['t', 'open', 'high', 'low', 'close', 'volume']))
        
        last_1h = df_1h.iloc[-1]
        
        # BTC 15 Dakikalık Değişim (Şok Filtresi)
        c_now = df_15m['close'].iloc[-1]
        c_prev = df_15m['open'].iloc[-1]
        pct_15m = ((c_now - c_prev) / c_prev) * 100
        system_state["btc_15m_change"] = round(pct_15m, 2)

        if pct_15m <= -1.5:
            system_state["btc_shock_lock"] = True
            system_state["btc_shock_reason"] = f"🔴 BTC Ani Düşüş Şoku (%{pct_15m:.2f}) | LONG Kilitlendi"
        elif pct_15m >= 1.5:
            system_state["btc_shock_lock"] = True
            system_state["btc_shock_reason"] = f"🟢 BTC Ani Yükseliş Şoku (+%{pct_15m:.2f}) | SHORT Kilitlendi"
        else:
            system_state["btc_shock_lock"] = False
            system_state["btc_shock_reason"] = ""

        # Rejim Durumu
        if last_1h['close'] > last_1h['ema50']:
            system_state["btc_regime"] = "🟢 BOĞA (YÜKSELİŞ)"
            bias = "BOĞA / LONG AĞIRLIKLI"
        else:
            system_state["btc_regime"] = "🔴 AYI (DÜŞÜŞ)"
            bias = "AYI / SHORT AĞIRLIKLI"

        ticker = await exchange.fetch_ticker('BTC/USDT:USDT')
        vol_quote = ticker.get('quoteVolume', 0)
        vol_str = f"${vol_quote/1e9:.2f} Milyar" if vol_quote > 1e9 else f"${vol_quote/1e6:.1f} Milyon"

        system_state["sentiment_data"] = {
            "btc_rsi": round(float(last_1h['rsi']), 1) if pd.notnull(last_1h['rsi']) else 50.0,
            "btc_volume_24h": vol_str,
            "market_bias": bias
        }
    except Exception:
        system_state["btc_regime"] = "BTC: AKTİF"

async def analyze_symbol(exchange, symbol):
    try:
        base = symbol.split('/')[0].upper()
        if any(exc in base for exc in EXCLUDED_KEYWORDS):
            return None

        tasks = [
            exchange.fetch_ohlcv(symbol, timeframe='5m', limit=35),
            exchange.fetch_ohlcv(symbol, timeframe='15m', limit=35),
            exchange.fetch_ohlcv(symbol, timeframe='1h', limit=35)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        if any(isinstance(r, Exception) or not r or len(r) < 20 for r in results):
            return None

        df_5m = calculate_indicators(pd.DataFrame(results[0], columns=['t', 'open', 'high', 'low', 'close', 'volume']))
        df_15m = calculate_indicators(pd.DataFrame(results[1], columns=['t', 'open', 'high', 'low', 'close', 'volume']))
        df_1h = calculate_indicators(pd.DataFrame(results[2], columns=['t', 'open', 'high', 'low', 'close', 'volume']))

        c_5m = df_5m.iloc[-1]
        c_15m = df_15m.iloc[-1]
        c_1h = df_1h.iloc[-1]

        swing_low = df_5m['low'].iloc[-18:-3].min()
        swing_high = df_5m['high'].iloc[-18:-3].max()
        recent_breakout_high = df_5m['high'].iloc[-5:-1].max()
        recent_breakout_low = df_5m['low'].iloc[-5:-1].min()

        score = 0
        direction = None
        reasons = []

        # 2 Kademeli Likidite Süpürmesi & MSS Karakter Kırılımı
        sweep_low = df_5m['low'].iloc[-4:].min() < swing_low
        mss_bull = c_5m['close'] > recent_breakout_high and c_5m['close'] > c_5m['open']

        sweep_high = df_5m['high'].iloc[-4:].max() > swing_high
        mss_bear = c_5m['close'] < recent_breakout_low and c_5m['close'] < c_5m['open']

        if sweep_low and mss_bull:
            # BTC Şok Kilidi Kontrolü (Düşüş anında LONG açma)
            if not (system_state["btc_shock_lock"] and system_state["btc_15m_change"] <= -1.5):
                direction = "LONG"
                score += 45
                reasons.append("⚡ 5M Dip Likiditesi Alındı + MSS Kırılımı")
        elif sweep_high and mss_bear:
            # BTC Şok Kilidi Kontrolü (Yükseliş anında SHORT açma)
            if not (system_state["btc_shock_lock"] and system_state["btc_15m_change"] >= 1.5):
                direction = "SHORT"
                score += 45
                reasons.append("⚡ 5M Tepe Likiditesi Alındı + MSS Kırılımı")

        if direction == "LONG":
            if c_1h['close'] > c_1h['ema50'] and c_15m['close'] > c_15m['ema50']:
                score += 30
                reasons.append("📈 1H & 15M Çift Trend Uyumu")
            elif c_1h['close'] > c_1h['ema50'] or c_15m['close'] > c_15m['ema50']:
                score += 15
                reasons.append("📈 15M Trend Desteği")
        elif direction == "SHORT":
            if c_1h['close'] < c_1h['ema50'] and c_15m['close'] < c_15m['ema50']:
                score += 30
                reasons.append("📉 1H & 15M Çift Trend Uyumu")
            elif c_1h['close'] < c_1h['ema50'] or c_15m['close'] < c_15m['ema50']:
                score += 15
                reasons.append("📉 15M Trend Desteği")

        vol_ratio = float(c_5m['volume'] / (c_5m['vol_ma'] + 1e-9)) if pd.notnull(c_5m['vol_ma']) else 1.0
        if vol_ratio >= 1.20:
            score += 15
            reasons.append(f"🔥 Onay Hacmi ({vol_ratio:.1f}x)")

        if 40 <= c_5m['rsi'] <= 65:
            score += 10
            reasons.append(f"🎯 Sağlıklı RSI ({c_5m['rsi']:.1f})")

        radar_item = {
            "symbol": symbol,
            "price": float(c_5m['close']),
            "rsi": round(float(c_5m['rsi']), 1) if pd.notnull(c_5m['rsi']) else 50.0,
            "vol_ratio": round(vol_ratio, 2),
            "trend": direction if direction else ("LONG" if c_5m['close'] > c_5m['ema50'] else "SHORT"),
            "score": score
        }
        system_state["radar_symbols"] = [r for r in system_state["radar_symbols"] if r["symbol"] != symbol]
        system_state["radar_symbols"].append(radar_item)
        if len(system_state["radar_symbols"]) > 60:
            system_state["radar_symbols"].pop(0)

        # KALİTE BARAJI: En az 75 Puan
        if not direction or score < 75:
            return None

        entry = float(c_5m['close'])
        atr = float(c_5m['atr']) if pd.notnull(c_5m['atr']) else entry * 0.005

        if direction == "LONG":
            sl = float(df_5m['low'].iloc[-6:].min() - (1.2 * atr))
            if (entry - sl) / entry < 0.006:
                sl = entry * 0.994
            risk_dist = entry - sl

            dyn_tp1 = float(df_15m['high'].iloc[-25:-1].max())
            if (dyn_tp1 - entry) < (1.2 * risk_dist):
                dyn_tp1 = entry + (1.5 * risk_dist)

            dyn_tp2 = float(df_1h['high'].iloc[-25:-1].max())
            if dyn_tp2 <= dyn_tp1 or (dyn_tp2 - entry) < (2.0 * risk_dist):
                dyn_tp2 = entry + (3.0 * risk_dist)

            tp1, tp2 = dyn_tp1, dyn_tp2

        else:
            sl = float(df_5m['high'].iloc[-6:].max() + (1.2 * atr))
            if (sl - entry) / entry < 0.006:
                sl = entry * 1.006
            risk_dist = sl - entry

            dyn_tp1 = float(df_15m['low'].iloc[-25:-1].min())
            if (entry - dyn_tp1) < (1.2 * risk_dist):
                dyn_tp1 = entry - (1.5 * risk_dist)

            dyn_tp2 = float(df_1h['low'].iloc[-25:-1].min())
            if dyn_tp2 >= dyn_tp1 or (entry - dyn_tp2) < (2.0 * risk_dist):
                dyn_tp2 = entry - (3.0 * risk_dist)

            tp1, tp2 = dyn_tp1, dyn_tp2

        pos_size, margin, max_loss = compute_position_metrics(entry, sl)

        return {
            "symbol": symbol,
            "direction": direction,
            "score": score,
            "entry": entry,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "pos_size": pos_size,
            "margin": margin,
            "max_loss": max_loss,
            "leverage": system_state["leverage"],
            "margin_mode": system_state["margin_mode"],
            "tp1_hit": False,
            "active_size": pos_size,
            "current_price": entry,
            "unrealized_pnl": 0.0,
            "progress_pct": 0.0,
            "reasons": reasons,
            "open_time": get_now_str(),
            "open_timestamp": int(get_now_datetime().timestamp())
        }
    except Exception:
        return None

async def keep_alive_loop():
    url = os.environ.get("RENDER_EXTERNAL_URL")
    while True:
        await asyncio.sleep(600)
        if url:
            try:
                async with aiohttp.ClientSession() as session:
                    await session.get(f"{url}/api/health")
            except Exception:
                pass

async def market_scanner_loop():
    await asyncio.sleep(2)
    add_log("Quant Motoru: Çift Yönlü BTC Şok Korumalı SMC Taraması Aktif...")

    while True:
        exchange = None
        try:
            exchange = ccxt.bybit({
                'options': {'defaultType': 'linear'},
                'enableRateLimit': True,
                'timeout': 10000
            })

            await update_btc_metrics(exchange)
            await fetch_fear_greed()

            markets = await exchange.load_markets()
            crypto_symbols = [
                s for s, m in markets.items() 
                if m.get('quote') == 'USDT' and m.get('linear') and m.get('active') 
                and not m.get('delivery') and not '-' in s
                and not any(exc in s.split('/')[0].upper() for exc in EXCLUDED_KEYWORDS)
            ]
            system_state["scanned_count"] = len(crypto_symbols)

            batch_size = 10
            for i in range(0, len(crypto_symbols), batch_size):
                chunk = crypto_symbols[i:i + batch_size]
                tasks = [analyze_symbol(exchange, s) for s in chunk]
                signals = await asyncio.gather(*tasks, return_exceptions=True)

                for sig in signals:
                    if sig and isinstance(sig, dict):
                        exists = any(p['symbol'] == sig['symbol'] for p in system_state["active_positions"])
                        if not exists:
                            max_pos = system_state["max_open_positions"]
                            if max_pos > 0 and len(system_state["active_positions"]) >= max_pos:
                                continue

                            current_total_margin = sum(p['margin'] for p in system_state["active_positions"])
                            allowed_margin = system_state["total_balance"] * (system_state["max_total_margin_pct"] / 100.0)
                            if (current_total_margin + sig['margin']) > allowed_margin:
                                continue

                            system_state["active_positions"].append(sig)
                            mode_label = "İzole" if sig['margin_mode'] == "ISOLATED" else "Cross"
                            add_log(f"🟢 POZİSYON AÇILDI: {sig['symbol']} {sig['direction']} | {sig['score']} Puan | {sig['leverage']}x {mode_label} | Teminat: ${sig['margin']} | Risk: ${sig['max_loss']}")

                system_state["last_scan_time"] = get_now_str()
                await asyncio.sleep(0.1)

            for pos in list(system_state["active_positions"]):
                try:
                    ticker = await exchange.fetch_ticker(pos['symbol'])
                    curr_price = ticker['last']
                    pos['current_price'] = curr_price
                    direction = pos['direction']
                    close_reason = None

                    pnl_raw = ((curr_price - pos['entry']) / pos['entry']) if direction == "LONG" else ((pos['entry'] - curr_price) / pos['entry'])
                    pos['unrealized_pnl'] = round(pos['active_size'] * pnl_raw, 2)

                    target_dist = abs(pos['tp2'] - pos['entry'])
                    favorable_move = (curr_price - pos['entry']) if direction == "LONG" else (pos['entry'] - curr_price)
                    pos['progress_pct'] = max(0.0, min(100.0, round((favorable_move / (target_dist + 1e-9)) * 100, 1)))

                    if (direction == "LONG" and curr_price <= pos['sl']) or (direction == "SHORT" and curr_price >= pos['sl']):
                        close_reason = "❌ Stop-Loss Tetiklendi"
                    elif (direction == "LONG" and curr_price >= pos['tp2']) or (direction == "SHORT" and curr_price <= pos['tp2']):
                        close_reason = "🎯 TP2 Likidite Havuzuna Ulaşıldı"
                    elif (direction == "LONG" and curr_price >= pos['tp1']) or (direction == "SHORT" and curr_price <= pos['tp1']):
                        if not pos.get("tp1_hit"):
                            pos["tp1_hit"] = True
                            pos["sl"] = pos["entry"]
                            partial_pnl = round((pos['pos_size'] * 0.5) * pnl_raw, 2)
                            pos['active_size'] = pos['pos_size'] * 0.5
                            system_state["total_balance"] += partial_pnl
                            now_ts = int(get_now_datetime().timestamp())
                            system_state["equity_curve"].append({"time": now_ts, "value": round(system_state["total_balance"], 2)})
                            add_log(f"⚡ TP1 ALINDI ({pos['symbol']}): %50 Kâr Realize Edildi (+${partial_pnl}) | Stop Başabaşa Çekildi.")

                    if close_reason:
                        pnl_pct = ((curr_price - pos['entry']) / pos['entry'] * 100) if direction == "LONG" else ((pos['entry'] - curr_price) / pos['entry'] * 100)
                        realized_pnl = round(pos['active_size'] * (pnl_pct / 100.0), 2)
                        system_state["total_balance"] += realized_pnl

                        now_dt = get_now_datetime()
                        system_state["equity_curve"].append({"time": int(now_dt.timestamp()), "value": round(system_state["total_balance"], 2)})
                        
                        history_item = {
                            "symbol": pos['symbol'],
                            "direction": pos['direction'],
                            "entry": pos['entry'],
                            "close_price": curr_price,
                            "pnl_pct": round(pnl_pct, 2),
                            "realized_pnl": realized_pnl,
                            "score": pos['score'],
                            "open_reasons": pos['reasons'],
                            "close_reason": close_reason,
                            "close_time": now_dt.strftime("%H:%M:%S"),
                            "close_timestamp": int(now_dt.timestamp())
                        }
                        system_state["trade_history"].insert(0, history_item)
                        system_state["active_positions"].remove(pos)
                        add_log(f"🔴 POZİSYON KAPANDI: {pos['symbol']} | PnL: %{pnl_pct:.2f} (${realized_pnl}) | {close_reason}")
                except Exception:
                    pass

            await exchange.close()
            await asyncio.sleep(1)
        except Exception as e:
            add_log(f"Döngü Uyarısı: {str(e)[:45]}")
            if exchange:
                try:
                    await exchange.close()
                except Exception:
                    pass
            await asyncio.sleep(2)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task1 = asyncio.create_task(market_scanner_loop())
    task2 = asyncio.create_task(keep_alive_loop())
    yield
    task1.cancel()
    task2.cancel()

app = FastAPI(title="Meta Quant Terminal Pro", lifespan=lifespan)

@app.get("/api/health")
async def health_check():
    return {"status": "ok"}

class SettingsPayload(BaseModel):
    total_balance: float
    risk_pct: float
    leverage: int
    margin_mode: str
    max_open_positions: int
    max_total_margin_pct: float

class ApiPayload(BaseModel):
    exchange: str
    mode: str
    api_key: str
    api_secret: str
    auto_trade: bool

class ClosePosPayload(BaseModel):
    symbol: str

class UpdateSlTpPayload(BaseModel):
    symbol: str
    sl: float
    tp2: float

@app.post("/api/update_settings")
async def update_settings(payload: SettingsPayload):
    system_state["total_balance"] = payload.total_balance
    system_state["risk_pct"] = payload.risk_pct
    system_state["leverage"] = payload.leverage
    system_state["margin_mode"] = payload.margin_mode
    system_state["max_open_positions"] = payload.max_open_positions
    system_state["max_total_margin_pct"] = payload.max_total_margin_pct
    
    pos_limit_str = "Sınırsız" if payload.max_open_positions == 0 else f"{payload.max_open_positions} Adet"
    mode_str = "İzole" if payload.margin_mode == "ISOLATED" else "Cross"
    add_log(f"⚙️ AYARLAR GÜNCELLENDİ: Kasa: ${payload.total_balance} | Mod: {mode_str} | Risk: %{payload.risk_pct} | Kaldıraç: {payload.leverage}x | Max Poz: {pos_limit_str} | Max Marjin: %{payload.max_total_margin_pct}")
    return {"status": "success"}

@app.post("/api/update_api")
async def update_api(payload: ApiPayload):
    system_state["api_settings"] = payload.dict()
    status_str = "AKTİF" if payload.auto_trade else "DEVRE DIŞI"
    add_log(f"🔑 API GÜNCELLENDİ: {payload.exchange} ({payload.mode}) | Otomatik Emir: {status_str}")
    return {"status": "success"}

@app.post("/api/manual/close_position")
async def manual_close_position(payload: ClosePosPayload):
    target = next((p for p in system_state["active_positions"] if p['symbol'] == payload.symbol), None)
    if target:
        curr_price = target.get('current_price', target['entry'])
        direction = target['direction']
        pnl_pct = ((curr_price - target['entry']) / target['entry'] * 100) if direction == "LONG" else ((target['entry'] - curr_price) / target['entry'] * 100)
        realized_pnl = round(target['active_size'] * (pnl_pct / 100.0), 2)
        system_state["total_balance"] += realized_pnl

        now_dt = get_now_datetime()
        system_state["equity_curve"].append({"time": int(now_dt.timestamp()), "value": round(system_state["total_balance"], 2)})

        history_item = {
            "symbol": target['symbol'],
            "direction": target['direction'],
            "entry": target['entry'],
            "close_price": curr_price,
            "pnl_pct": round(pnl_pct, 2),
            "realized_pnl": realized_pnl,
            "score": target['score'],
            "open_reasons": target['reasons'],
            "close_reason": "✋ MANUEL KAPATILDI",
            "close_time": now_dt.strftime("%H:%M:%S"),
            "close_timestamp": int(now_dt.timestamp())
        }
        system_state["trade_history"].insert(0, history_item)
        system_state["active_positions"].remove(target)
        add_log(f"✋ MANUEL KAPATMA: {target['symbol']} | PnL: ${realized_pnl}")
        return {"status": "success"}
    return {"status": "error"}

@app.post("/api/manual/close_all")
async def manual_close_all():
    for pos in list(system_state["active_positions"]):
        curr_price = pos.get('current_price', pos['entry'])
        direction = pos['direction']
        pnl_pct = ((curr_price - pos['entry']) / pos['entry'] * 100) if direction == "LONG" else ((pos['entry'] - curr_price) / pos['entry'] * 100)
        realized_pnl = round(pos['active_size'] * (pnl_pct / 100.0), 2)
        system_state["total_balance"] += realized_pnl

        now_dt = get_now_datetime()
        history_item = {
            "symbol": pos['symbol'],
            "direction": pos['direction'],
            "entry": pos['entry'],
            "close_price": curr_price,
            "pnl_pct": round(pnl_pct, 2),
            "realized_pnl": realized_pnl,
            "score": pos['score'],
            "open_reasons": pos['reasons'],
            "close_reason": "🚨 ACİL TÜMÜNÜ KAPAT",
            "close_time": now_dt.strftime("%H:%M:%S"),
            "close_timestamp": int(now_dt.timestamp())
        }
        system_state["trade_history"].insert(0, history_item)
        system_state["active_positions"].remove(pos)
    add_log("🚨 TÜM POZİSYONLAR KAPATILDI!")
    return {"status": "success"}

@app.post("/api/manual/breakeven")
async def manual_breakeven(payload: ClosePosPayload):
    target = next((p for p in system_state["active_positions"] if p['symbol'] == payload.symbol), None)
    if target:
        target['sl'] = target['entry']
        target['tp1_hit'] = True
        add_log(f"🛡️ BAŞABAŞ: {target['symbol']} Stop Girişe çekildi!")
        return {"status": "success"}
    return {"status": "error"}

@app.post("/api/manual/update_sltp")
async def manual_update_sltp(payload: UpdateSlTpPayload):
    target = next((p for p in system_state["active_positions"] if p['symbol'] == payload.symbol), None)
    if target:
        target['sl'] = payload.sl
        target['tp2'] = payload.tp2
        add_log(f"🎯 GÜNCELLEME: {target['symbol']} SL: {payload.sl} | TP2: {payload.tp2}")
        return {"status": "success"}
    return {"status": "error"}

@app.get("/api/export/csv")
async def export_current_csv():
    df = pd.DataFrame(system_state["trade_history"]) if system_state["trade_history"] else pd.DataFrame(columns=["symbol", "direction", "entry", "close_price", "pnl_pct", "realized_pnl", "close_reason", "close_time"])
    stream = io.StringIO()
    df.to_csv(stream, index=False)
    response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
    response.headers["Content-Disposition"] = f"attachment; filename=trades_{get_now_datetime().strftime('%Y-%m-%d')}.csv"
    return response

@app.get("/api/reports/list")
async def list_reports():
    files = [f for f in os.listdir(CSV_DIR) if f.endswith(".csv")]
    return sorted(files, reverse=True)

@app.get("/api/reports/download/{filename}")
async def download_report(filename: str):
    file_path = os.path.join(CSV_DIR, filename)
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
        response = StreamingResponse(iter([content]), media_type="text/csv")
        response.headers["Content-Disposition"] = f"attachment; filename={filename}"
        return response
    return {"error": "File not found"}

@app.get("/api/state")
async def get_state():
    return system_state

@app.get("/", response_class=HTMLResponse)
async def get_dashboard(request: Request):
    html_content = """
    <!DOCTYPE html>
    <html lang="tr">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Meta Quant Terminal Pro Ultimate</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <script src="https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>
        <style>
            html, body { min-height: 100%; background-color: #0b0e14; color: #e2e8f0; font-family: 'Inter', monospace; overflow-y: auto; }
            .card { background-color: #121824; border: 1px solid #1e293b; }
            ::-webkit-scrollbar { width: 6px; height: 6px; }
            ::-webkit-scrollbar-track { background: #0b0e14; }
            ::-webkit-scrollbar-thumb { background: #334155; border-radius: 3px; }
            ::-webkit-scrollbar-thumb:hover { background: #475569; }
            .tf-btn.active { background-color: #10b981; color: #000; font-weight: bold; }
            .pnl-tf-btn.active { background-color: #38bdf8; color: #000; font-weight: bold; }
            .nav-tab.active { background-color: #10b981; color: #000; font-weight: bold; }
            #tv-wrapper { position: relative; width: 100%; height: 100%; }
            #box-canvas { position: absolute; top: 0; left: 0; pointer-events: none; z-index: 2; }
        </style>
    </head>
    <body class="p-3 space-y-3 pb-16">
        
        <!-- ÜST MENÜ -->
        <div class="card p-3 rounded-xl flex flex-wrap justify-between items-center gap-3 border-emerald-500/30">
            <div class="flex items-center space-x-3">
                <div class="w-3 h-3 bg-emerald-500 rounded-full animate-ping"></div>
                <div>
                    <div class="flex items-center space-x-2">
                        <h1 class="text-base font-extrabold tracking-wider text-emerald-400">META QUANT ULTIMATE</h1>
                        <span id="btc-regime-badge" class="text-[9px] font-bold px-2 py-0.5 rounded bg-slate-800 text-slate-300 border border-slate-700">BTC: YÜKLENİYOR</span>
                        <span id="btc-shock-badge" class="hidden text-[9px] font-bold px-2 py-0.5 rounded bg-rose-950 text-rose-400 border border-rose-800 animate-pulse">⚡ BTC ŞOK KORUMASI</span>
                    </div>
                </div>
            </div>

            <!-- SAYFA SEKMELERİ -->
            <div class="flex items-center flex-wrap gap-1 bg-slate-900/90 p-1 rounded-xl border border-slate-800 text-xs">
                <button onclick="switchTab('terminal')" id="tab-terminal" class="nav-tab active px-2.5 py-1 rounded-lg text-slate-400 hover:text-white transition">📊 Terminal</button>
                <button onclick="switchTab('sentiment')" id="tab-sentiment" class="nav-tab px-2.5 py-1 rounded-lg text-slate-400 hover:text-white transition">🧠 Duyarlılık & Endeksler</button>
                <button onclick="switchTab('news')" id="tab-news" class="nav-tab px-2.5 py-1 rounded-lg text-slate-400 hover:text-white transition">📰 Haber & Takvim</button>
                <button onclick="switchTab('manual')" id="tab-manual" class="nav-tab px-2.5 py-1 rounded-lg text-slate-400 hover:text-white transition">🎮 Manuel Kontrol</button>
                <button onclick="switchTab('excel')" id="tab-excel" class="nav-tab px-2.5 py-1 rounded-lg text-slate-400 hover:text-white transition">📑 Excel Arşivi</button>
                <button onclick="switchTab('stats')" id="tab-stats" class="nav-tab px-2.5 py-1 rounded-lg text-slate-400 hover:text-white transition">📈 İstatistik</button>
                <button onclick="switchTab('radar')" id="tab-radar" class="nav-tab px-2.5 py-1 rounded-lg text-slate-400 hover:text-white transition">🔥 Radar</button>
                <button onclick="switchTab('journal')" id="tab-journal" class="nav-tab px-2.5 py-1 rounded-lg text-slate-400 hover:text-white transition">📖 Günlük</button>
                <button onclick="switchTab('api')" id="tab-api" class="nav-tab px-2.5 py-1 rounded-lg text-slate-400 hover:text-white transition">⚙️ API</button>
            </div>

            <div class="flex space-x-3 text-xs text-slate-400">
                <div>Taranan: <span id="scanned-count" class="text-white font-bold">0</span></div>
                <div>Son: <span id="last-scan" class="text-white font-bold">-</span></div>
            </div>
        </div>

        <!-- SAYFA 1: CANLI TERMİNAL -->
        <div id="page-terminal" class="space-y-3">
            <div class="card p-3 rounded-xl flex flex-wrap justify-between items-center gap-3">
                <div class="flex flex-col space-y-1 bg-slate-900/90 p-2 rounded-xl border border-slate-800">
                    <div class="flex items-center justify-between gap-2 border-b border-slate-800 pb-1 text-[9px]">
                        <span class="text-slate-400 font-semibold uppercase">Dönemsel PnL (TSİ 00:00):</span>
                        <div class="flex space-x-1">
                            <button onclick="changePnlFilter('today')" id="pnl-tf-today" class="pnl-tf-btn active px-1 py-0.5 rounded text-slate-400 hover:text-white">Bugün</button>
                            <button onclick="changePnlFilter('yesterday')" id="pnl-tf-yesterday" class="pnl-tf-btn px-1 py-0.5 rounded text-slate-400 hover:text-white">Dün</button>
                            <button onclick="changePnlFilter('week')" id="pnl-tf-week" class="pnl-tf-btn px-1 py-0.5 rounded text-slate-400 hover:text-white">Bu Hafta</button>
                            <button onclick="changePnlFilter('month')" id="pnl-tf-month" class="pnl-tf-btn px-1 py-0.5 rounded text-slate-400 hover:text-white">Bu Ay</button>
                            <button onclick="changePnlFilter('all')" id="pnl-tf-all" class="pnl-tf-btn px-1 py-0.5 rounded text-slate-400 hover:text-white">Tümü</button>
                        </div>
                    </div>

                    <div class="flex items-center space-x-3 pt-0.5">
                        <div>
                            <div class="text-[9px] text-slate-400 uppercase tracking-wider" id="pnl-label">Bugün Net PnL</div>
                            <div id="stat-pnl" class="text-sm font-extrabold font-mono text-emerald-400">$0.00</div>
                        </div>
                        <div class="border-r border-slate-800 h-6"></div>
                        <div>
                            <div class="text-[9px] text-slate-400 uppercase tracking-wider">Win Rate</div>
                            <div id="stat-winrate" class="text-sm font-extrabold font-mono text-sky-400">%0.0</div>
                        </div>
                        <div class="border-r border-slate-800 h-6"></div>
                        <div>
                            <div class="text-[9px] text-slate-400 uppercase tracking-wider">İşlem Adedi</div>
                            <div id="stat-trades" class="text-sm font-extrabold font-mono text-white">0</div>
                        </div>
                        <div class="border-r border-slate-800 h-6"></div>
                        <div>
                            <div class="text-[9px] text-slate-400 uppercase tracking-wider">Kullanılan Marjin</div>
                            <div id="stat-used-margin" class="text-xs font-bold font-mono text-amber-400">$0 (%0)</div>
                        </div>
                    </div>
                </div>

                <div class="flex flex-wrap items-center gap-2 bg-slate-900/90 p-2 rounded-xl border border-slate-800 text-xs">
                    <div>
                        <label class="text-slate-400 block text-[9px]">KASA ($)</label>
                        <input id="input-balance" type="number" value="1000" class="bg-slate-800 text-white font-bold w-16 px-1 py-0.5 rounded outline-none border border-slate-700">
                    </div>
                    <div>
                        <label class="text-slate-400 block text-[9px]">MOD</label>
                        <select id="input-margin-mode" class="bg-slate-800 text-cyan-400 font-bold px-1 py-0.5 rounded outline-none border border-slate-700">
                            <option value="ISOLATED" selected>İzole</option>
                            <option value="CROSS">Cross</option>
                        </select>
                    </div>
                    <div>
                        <label class="text-slate-400 block text-[9px]">RİSK (%)</label>
                        <select id="input-risk" class="bg-slate-800 text-white font-bold px-1 py-0.5 rounded outline-none border border-slate-700">
                            <option value="0.5">%0.5</option>
                            <option value="1.0">%1.0</option>
                            <option value="2.0">%2.0</option>
                            <option value="3.0">%3.0</option>
                            <option value="5.0" selected>%5.0</option>
                        </select>
                    </div>
                    <div>
                        <label class="text-slate-400 block text-[9px]">KALDIRAÇ</label>
                        <select id="input-leverage" class="bg-slate-800 text-emerald-400 font-bold px-1 py-0.5 rounded outline-none border border-slate-700">
                            <option value="5">5x</option>
                            <option value="10">10x</option>
                            <option value="20">20x</option>
                            <option value="50" selected>50x</option>
                            <option value="75">75x</option>
                        </select>
                    </div>
                    <div>
                        <label class="text-slate-400 block text-[9px]">MAX POZİSYON</label>
                        <select id="input-max-pos" class="bg-slate-800 text-amber-400 font-bold px-1 py-0.5 rounded outline-none border border-slate-700">
                            <option value="1">1 Adet</option>
                            <option value="2">2 Adet</option>
                            <option value="3">3 Adet</option>
                            <option value="5" selected>5 Adet</option>
                            <option value="10">10 Adet</option>
                            <option value="0">Sınırsız</option>
                        </select>
                    </div>
                    <div>
                        <label class="text-slate-400 block text-[9px]">MAX KASA PAYI</label>
                        <select id="input-max-margin-pct" class="bg-slate-800 text-fuchsia-400 font-bold px-1 py-0.5 rounded outline-none border border-slate-700">
                            <option value="20">%20</option>
                            <option value="35">%35</option>
                            <option value="50" selected>%50</option>
                            <option value="75">%75</option>
                            <option value="100">%100</option>
                        </select>
                    </div>
                    <button onclick="saveSettings()" class="mt-3 bg-emerald-600 hover:bg-emerald-500 text-black font-bold px-2 py-1 rounded transition text-xs">KAYDET</button>
                </div>
            </div>

            <div class="grid grid-cols-1 lg:grid-cols-3 gap-3">
                <div class="card p-3 rounded-xl lg:col-span-2 h-[520px] flex flex-col">
                    <div class="flex flex-wrap justify-between items-center mb-2 px-1 gap-2">
                        <div class="flex items-center space-x-3">
                            <span id="chart-title" class="text-xs font-bold text-emerald-400 tracking-wider">GRAFİK</span>
                            <div class="flex space-x-1 bg-slate-900 p-0.5 rounded border border-slate-800 text-[10px]">
                                <button onclick="changeTimeframe('1')" class="tf-btn px-2 py-0.5 rounded text-slate-400 hover:text-white" id="tf-1">1M</button>
                                <button onclick="changeTimeframe('5')" class="tf-btn active px-2 py-0.5 rounded text-slate-400 hover:text-white" id="tf-5">5M</button>
                                <button onclick="changeTimeframe('15')" class="tf-btn px-2 py-0.5 rounded text-slate-400 hover:text-white" id="tf-15">15M</button>
                                <button onclick="changeTimeframe('60')" class="tf-btn px-2 py-0.5 rounded text-slate-400 hover:text-white" id="tf-60">1H</button>
                                <button onclick="changeTimeframe('240')" class="tf-btn px-2 py-0.5 rounded text-slate-400 hover:text-white" id="tf-240">4H</button>
                                <button onclick="changeTimeframe('D')" class="tf-btn px-2 py-0.5 rounded text-slate-400 hover:text-white" id="tf-D">1D</button>
                            </div>
                            <div id="ohlc-box" class="flex items-center space-x-2 bg-slate-900/90 px-2 py-0.5 rounded border border-slate-800 text-[10px] font-mono text-slate-300">
                                <span>O: <b id="bar-open" class="text-white">-</b></span>
                                <span>H: <b id="bar-high" class="text-amber-400">-</b></span>
                                <span>L: <b id="bar-low" class="text-indigo-400">-</b></span>
                                <span>C: <b id="bar-close" class="text-white">-</b></span>
                            </div>
                        </div>
                        <span id="chart-levels" class="text-[11px] text-slate-400 space-x-2"></span>
                    </div>
                    <div id="tv-wrapper" class="w-full flex-1 rounded overflow-hidden">
                        <div id="tv-container" class="w-full h-full"></div>
                        <canvas id="box-canvas"></canvas>
                    </div>
                </div>

                <div class="card p-3 rounded-xl flex flex-col justify-between h-[520px]">
                    <div>
                        <h2 class="text-xs font-semibold text-slate-400 mb-2 uppercase tracking-wider">Seçili Parite Giriş Gerekçesi</h2>
                        <div id="active-rationale" class="space-y-2 text-xs">
                            <div class="text-slate-500 italic">Tablodan bir parite seçin...</div>
                        </div>
                    </div>
                    <div class="mt-3">
                        <h3 class="text-[10px] font-semibold text-slate-500 mb-1 uppercase">Sistem Logları</h3>
                        <div id="log-box" class="bg-black/50 p-2 rounded text-[11px] text-emerald-500/80 font-mono h-28 overflow-y-auto space-y-1"></div>
                    </div>
                </div>
            </div>

            <div class="grid grid-cols-1 lg:grid-cols-3 gap-3">
                <div class="card p-3 rounded-xl lg:col-span-2">
                    <h2 class="text-xs font-semibold text-emerald-400 mb-2 flex items-center justify-between">
                        <span class="flex items-center"><span class="w-2 h-2 bg-emerald-400 rounded-full mr-2"></span> AKTİF POZİSYONLAR (Grafik için Tıkla)</span>
                        <span class="text-[10px] text-slate-500">Canlı PnL & TP2 İlerlemesi</span>
                    </h2>
                    <div class="overflow-x-auto max-h-64 overflow-y-auto">
                        <table class="w-full text-left text-[11px]">
                            <thead class="text-slate-500 border-b border-slate-800 sticky top-0 bg-[#121824]">
                                <tr>
                                    <th class="pb-2">PARİTE</th>
                                    <th class="pb-2">GİRİŞ ZAMANI</th>
                                    <th class="pb-2">YÖN/KALDIRAÇ/MOD</th>
                                    <th class="pb-2">TEMİNAT (M.)</th>
                                    <th class="pb-2">GİRİŞ</th>
                                    <th class="pb-2">CANLI FİYAT</th>
                                    <th class="pb-2">ANLIK PnL ($)</th>
                                    <th class="pb-2">HEDEF İLERLEME</th>
                                </tr>
                            </thead>
                            <tbody id="active-pos-table" class="divide-y divide-slate-800/50"></tbody>
                        </table>
                    </div>
                </div>

                <div class="card p-3 rounded-xl flex flex-col h-72">
                    <h2 class="text-xs font-semibold text-sky-400 mb-2 flex items-center">
                        <span class="w-2 h-2 bg-sky-400 rounded-full mr-2"></span> KASA BÜYÜME EĞRİSİ (EQUITY)
                    </h2>
                    <div id="equity-container" class="w-full flex-1 rounded overflow-hidden"></div>
                </div>
            </div>
        </div>

        <!-- ======================================================== -->
        <!-- SAYFA 2: DUYARLILIK & CANLI ENDEKSLER -->
        <!-- ======================================================== -->
        <div id="page-sentiment" class="hidden space-y-3">
            <div class="grid grid-cols-1 md:grid-cols-3 gap-3">
                <div class="card p-4 rounded-xl flex flex-col items-center justify-center text-center">
                    <div class="text-xs text-slate-400 uppercase tracking-wider mb-2">Crypto Fear & Greed Index</div>
                    <div id="fng-val" class="text-5xl font-extrabold font-mono text-emerald-400">55</div>
                    <div id="fng-text" class="text-sm font-bold text-slate-300 mt-1 uppercase">Nötr</div>
                    <div class="w-full bg-slate-800 h-2 rounded-full mt-3 overflow-hidden">
                        <div id="fng-bar" class="bg-emerald-500 h-2 rounded-full" style="width: 55%"></div>
                    </div>
                </div>
                <div class="card p-4 rounded-xl space-y-2.5">
                    <div class="text-xs text-slate-400 uppercase tracking-wider">Bitcoin Canlı Metrikleri</div>
                    <div class="flex justify-between items-center bg-slate-900/80 p-2 rounded border border-slate-800 text-xs">
                        <span>BTC 15M Anlık Değişim:</span>
                        <span id="sent-btc-15m" class="font-bold font-mono text-emerald-400">%0.0</span>
                    </div>
                    <div class="flex justify-between items-center bg-slate-900/80 p-2 rounded border border-slate-800 text-xs">
                        <span>BTC 1H RSI Değeri:</span>
                        <span id="sent-btc-rsi" class="font-bold font-mono text-sky-400">50.0</span>
                    </div>
                    <div class="flex justify-between items-center bg-slate-900/80 p-2 rounded border border-slate-800 text-xs">
                        <span>BTC 24S Hacim Akışı:</span>
                        <span id="sent-btc-vol" class="font-bold font-mono text-amber-400">$0</span>
                    </div>
                </div>
                <div class="card p-4 rounded-xl flex flex-col justify-between">
                    <div class="text-xs text-slate-400 uppercase tracking-wider">Piyasa Rejimi & Tavsiye</div>
                    <div class="bg-slate-900/80 p-2.5 rounded border border-slate-800 text-xs space-y-1">
                        <div class="text-slate-400">Genel Bias: <b id="sent-bias" class="text-emerald-400">BOĞA</b></div>
                        <div class="text-slate-400">Şok Durumu: <b id="sent-shock-status" class="text-slate-300">NORMAL</b></div>
                    </div>
                    <div class="text-[10px] text-slate-500 font-mono">Veriler Bybit & Alternative.me Canlı Akışından Alınır</div>
                </div>
            </div>
        </div>

        <!-- ======================================================== -->
        <!-- SAYFA 3: HABER & VOLATİLİTE TAKVİMİ -->
        <!-- ======================================================== -->
        <div id="page-news" class="hidden space-y-3">
            <div class="card p-4 rounded-xl">
                <h3 class="text-xs font-semibold text-slate-400 mb-3 uppercase">📅 Kripto & Makro Ekonomik Volatilite Takvimi</h3>
                <div class="overflow-x-auto">
                    <table class="w-full text-left text-xs">
                        <thead class="text-slate-500 border-b border-slate-800">
                            <tr>
                                <th class="pb-2">DÖNEM</th>
                                <th class="pb-2">OLAY / HABER</th>
                                <th class="pb-2">ETKİ DERECESİ</th>
                                <th class="pb-2">AÇIKLAMA / BEKLENTİ</th>
                            </tr>
                        </thead>
                        <tbody class="divide-y divide-slate-800/60">
                            <tr>
                                <td class="py-2.5 font-mono text-white">Her Ayın İlk Çarşambası</td>
                                <td class="font-bold text-white">ABD TÜFE (CPI) Enflasyon Verisi (15:30 TSİ)</td>
                                <td><span class="px-2 py-0.5 rounded text-[10px] font-bold bg-rose-500/20 text-rose-400">YÜKSEK</span></td>
                                <td class="text-slate-300">Faiz beklentilerini ve dolar likiditesini doğrudan sarsar.</td>
                            </tr>
                            <tr>
                                <td class="py-2.5 font-mono text-white">6 Haftada Bir</td>
                                <td class="font-bold text-white">FED FOMC Faiz Kararı & Powell Basın Toplantısı (21:00 TSİ)</td>
                                <td><span class="px-2 py-0.5 rounded text-[10px] font-bold bg-purple-500/20 text-purple-400">KRİTİK</span></td>
                                <td class="text-slate-300">Kripto piyasasında geniş çaplı likidasyon dalgaları oluşturur.</td>
                            </tr>
                            <tr>
                                <td class="py-2.5 font-mono text-white">Her Ayın İlk Cuma Günü</td>
                                <td class="font-bold text-white">ABD Tarım Dışı İstihdam (NFP) (15:30 TSİ)</td>
                                <td><span class="px-2 py-0.5 rounded text-[10px] font-bold bg-rose-500/20 text-rose-400">YÜKSEK</span></td>
                                <td class="text-slate-300">ABD istihdam gücünü ölçer, ani fitil oynaklığı yaratır.</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>

        <!-- ======================================================== -->
        <!-- SAYFA 4: MANUEL MÜDAHALE -->
        <!-- ======================================================== -->
        <div id="page-manual" class="hidden space-y-3">
            <div class="card p-4 rounded-xl flex justify-between items-center border-rose-500/30">
                <div>
                    <h2 class="text-sm font-bold text-rose-400">🚨 Acil Durum & Manuel Pozisyon Masası</h2>
                    <p class="text-xs text-slate-400 mt-0.5">Açık pozisyonları tek tıkla kapatabilir veya stopu girişe çekebilirsiniz.</p>
                </div>
                <button onclick="manualCloseAll()" class="bg-rose-600 hover:bg-rose-500 text-white font-bold px-4 py-2 rounded-lg text-xs transition">
                    TÜMÜNÜ KAPAT (PANIC CLOSE)
                </button>
            </div>
            <div class="card p-4 rounded-xl">
                <div class="overflow-x-auto">
                    <table class="w-full text-left text-xs">
                        <thead class="text-slate-500 border-b border-slate-800">
                            <tr>
                                <th class="pb-2">PARİTE</th>
                                <th class="pb-2">YÖN</th>
                                <th class="pb-2">GİRİŞ / CANLI</th>
                                <th class="pb-2">ANLIK PnL</th>
                                <th class="pb-2">SL GÜNCELLE</th>
                                <th class="pb-2">TP2 GÜNCELLE</th>
                                <th class="pb-2 text-right">EYLEMLER</th>
                            </tr>
                        </thead>
                        <tbody id="manual-pos-table" class="divide-y divide-slate-800/60"></tbody>
                    </table>
                </div>
            </div>
        </div>

        <!-- ======================================================== -->
        <!-- SAYFA 5: GÜNLÜK EXCEL ARŞİVİ -->
        <!-- ======================================================== -->
        <div id="page-excel" class="hidden space-y-3">
            <div class="grid grid-cols-1 lg:grid-cols-3 gap-3">
                <div class="card p-4 rounded-xl space-y-3">
                    <h2 class="text-xs font-bold text-emerald-400 uppercase">📊 Canlı CSV Raporu İndir</h2>
                    <p class="text-xs text-slate-400">Bugün kapanan tüm işlemlerin ve geçmişin güncel dökümünü CSV formatında indirin.</p>
                    <a href="/api/export/csv" class="inline-block bg-emerald-600 hover:bg-emerald-500 text-black font-bold px-4 py-2 rounded-lg text-xs transition">
                        📥 BUGÜNÜN TÜM İŞLEMLERİNİ İNDİR (CSV)
                    </a>
                </div>
                <div class="card p-4 rounded-xl lg:col-span-2 space-y-3">
                    <h2 class="text-xs font-bold text-sky-400 uppercase">📁 Tarihli Arşiv Dosyaları</h2>
                    <div class="overflow-x-auto max-h-56 overflow-y-auto">
                        <table class="w-full text-left text-xs">
                            <thead class="text-slate-500 border-b border-slate-800">
                                <tr>
                                    <th class="pb-2">DOSYA ADI</th>
                                    <th class="pb-2 text-right">EYLEM</th>
                                </tr>
                            </thead>
                            <tbody id="reports-table" class="divide-y divide-slate-800/60"></tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>

        <!-- ======================================================== -->
        <!-- SAYFA 6: PERFORMANS & İSTATİSTİK (DÜZELTİLDİ) -->
        <!-- ======================================================== -->
        <div id="page-stats" class="hidden space-y-3">
            <div class="grid grid-cols-1 md:grid-cols-4 gap-3">
                <div class="card p-4 rounded-xl">
                    <div class="text-[10px] text-slate-400 uppercase">Kâr Faktörü (Profit Factor)</div>
                    <div id="stat-pf" class="text-xl font-bold font-mono text-emerald-400 mt-1">0.00</div>
                    <div class="text-[10px] text-slate-500 mt-1">Toplam Kazanç / Toplam Kayıp</div>
                </div>
                <div class="card p-4 rounded-xl">
                    <div class="text-[10px] text-slate-400 uppercase">Ortalama Kârlı İşlem</div>
                    <div id="stat-avg-win" class="text-xl font-bold font-mono text-emerald-400 mt-1">+$0.00</div>
                    <div class="text-[10px] text-slate-500 mt-1">Başarılı işlemlerin ortalaması</div>
                </div>
                <div class="card p-4 rounded-xl">
                    <div class="text-[10px] text-slate-400 uppercase">Ortalama Zararlı İşlem</div>
                    <div id="stat-avg-loss" class="text-xl font-bold font-mono text-red-400 mt-1">-$0.00</div>
                    <div class="text-[10px] text-slate-500 mt-1">Stoplanan işlemlerin ortalaması</div>
                </div>
                <div class="card p-4 rounded-xl">
                    <div class="text-[10px] text-slate-400 uppercase">Yön Dağılımı</div>
                    <div id="stat-long-short-win" class="text-sm font-bold font-mono text-sky-400 mt-1">L: %0 | S: %0</div>
                    <div class="text-[10px] text-slate-500 mt-1">Long vs Short Kazanma Oranı</div>
                </div>
            </div>

            <div class="card p-4 rounded-xl">
                <h2 class="text-xs font-semibold text-emerald-400 mb-3 uppercase">🏆 En Çok Kazandıran Lider Pariteler</h2>
                <div class="overflow-x-auto max-h-64 overflow-y-auto">
                    <table class="w-full text-left text-xs">
                        <thead class="text-slate-500 border-b border-slate-800">
                            <tr>
                                <th class="pb-2">PARİTE</th>
                                <th class="pb-2">İŞLEM SAYISI</th>
                                <th class="pb-2">TOPLAM PnL ($)</th>
                            </tr>
                        </thead>
                        <tbody id="top-symbols-table" class="divide-y divide-slate-800/50"></tbody>
                    </table>
                </div>
            </div>
        </div>

        <!-- ======================================================== -->
        <!-- SAYFA 7: RADAR -->
        <!-- ======================================================== -->
        <div id="page-radar" class="hidden space-y-3">
            <div class="card p-4 rounded-xl">
                <h2 class="text-xs font-semibold text-emerald-400 uppercase mb-3">🔥 700+ Canlı Taranan Parite Radarı</h2>
                <div class="overflow-x-auto max-h-[500px] overflow-y-auto">
                    <table class="w-full text-left text-xs">
                        <thead class="text-slate-500 border-b border-slate-800 sticky top-0 bg-[#121824]">
                            <tr>
                                <th class="pb-2">PARİTE</th>
                                <th class="pb-2">SON FİYAT</th>
                                <th class="pb-2">TREND</th>
                                <th class="pb-2">5M RSI</th>
                                <th class="pb-2">HACİM KAT</th>
                                <th class="pb-2">PUAN</th>
                            </tr>
                        </thead>
                        <tbody id="radar-table" class="divide-y divide-slate-800/50"></tbody>
                    </table>
                </div>
            </div>
        </div>

        <!-- ======================================================== -->
        <!-- SAYFA 8: GÜNLÜK (JOURNAL) -->
        <!-- ======================================================== -->
        <div id="page-journal" class="hidden space-y-3">
            <div class="card p-4 rounded-xl">
                <h2 class="text-xs font-semibold text-sky-400 mb-3 uppercase">📖 Kapanan İşlem Günlüğü</h2>
                <div class="overflow-x-auto max-h-[500px] overflow-y-auto">
                    <table class="w-full text-left text-xs">
                        <thead class="text-slate-500 border-b border-slate-800 sticky top-0 bg-[#121824]">
                            <tr>
                                <th class="pb-2">ZAMAN</th>
                                <th class="pb-2">PARİTE</th>
                                <th class="pb-2">YÖN</th>
                                <th class="pb-2">GİRİŞ / ÇIKIŞ</th>
                                <th class="pb-2">NET PnL ($)</th>
                                <th class="pb-2">KAPANIŞ NEDENİ</th>
                            </tr>
                        </thead>
                        <tbody id="journal-table" class="divide-y divide-slate-800/50"></tbody>
                    </table>
                </div>
            </div>
        </div>

        <!-- ======================================================== -->
        <!-- SAYFA 9: BORSA API -->
        <!-- ======================================================== -->
        <div id="page-api" class="hidden space-y-3">
            <div class="card p-4 rounded-xl space-y-3 max-w-lg">
                <h2 class="text-sm font-bold text-amber-400 uppercase">🔑 Borsa API Ayarları</h2>
                <div class="space-y-2 text-xs">
                    <div>
                        <label class="text-slate-400 block mb-1">BORSA</label>
                        <select id="api-exchange" class="w-full bg-slate-900 border border-slate-700 text-white rounded p-1.5 outline-none">
                            <option value="BINANCE" selected>Binance Futures</option>
                            <option value="BYBIT">Bybit Linear</option>
                        </select>
                    </div>
                    <div>
                        <label class="text-slate-400 block mb-1">AĞ TÜRÜ</label>
                        <select id="api-mode" class="w-full bg-slate-900 border border-slate-700 text-white rounded p-1.5 outline-none">
                            <option value="TESTNET" selected>Testnet (Sanal Mod)</option>
                            <option value="LIVE">Live (Gerçek Mod)</option>
                        </select>
                    </div>
                    <div>
                        <label class="text-slate-400 block mb-1">API KEY</label>
                        <input id="api-key" type="password" placeholder="API Key..." class="w-full bg-slate-900 border border-slate-700 text-white rounded p-1.5 outline-none font-mono">
                    </div>
                    <div>
                        <label class="text-slate-400 block mb-1">API SECRET</label>
                        <input id="api-secret" type="password" placeholder="API Secret..." class="w-full bg-slate-900 border border-slate-700 text-white rounded p-1.5 outline-none font-mono">
                    </div>
                    <button onclick="saveApiSettings()" class="w-full bg-amber-500 hover:bg-amber-400 text-black font-bold py-2 rounded transition">KAYDET</button>
                </div>
            </div>
        </div>

        <script>
            let chart = null;
            let candleSeries = null;
            let equityChart = null;
            let equitySeries = null;

            let currentSymbol = localStorage.getItem("selected_sym") || "BTC/USDT:USDT";
            let currentTimeframe = "5";
            let currentPnlFilter = "today";
            let selectedPos = null;
            let priceLines = [];
            let lastPositions = [];
            let tradeHistoryCache = [];
            let lastKnownPosCount = 0;

            function resizeCanvas() {
                const wrapper = document.getElementById('tv-wrapper');
                const canvas = document.getElementById('box-canvas');
                if (wrapper && canvas) {
                    canvas.width = wrapper.clientWidth;
                    canvas.height = wrapper.clientHeight;
                }
            }

            function drawPositionBoxes() {
                const canvas = document.getElementById('box-canvas');
                if (!canvas) return;
                const ctx = canvas.getContext('2d');
                ctx.clearRect(0, 0, canvas.width, canvas.height);

                if (!selectedPos || !candleSeries || !chart) return;

                const timeScale = chart.timeScale();
                const startX = timeScale.timeToCoordinate(selectedPos.open_timestamp);
                
                const rightX = canvas.width - 55;
                const boxStartX = startX !== null ? Math.max(0, startX) : 40;
                const boxWidth = rightX - boxStartX;

                if (boxWidth <= 0) return;

                const entryY = candleSeries.priceToCoordinate(selectedPos.entry);
                const slY = candleSeries.priceToCoordinate(selectedPos.sl);
                const tp1Y = candleSeries.priceToCoordinate(selectedPos.tp1);
                const tp2Y = candleSeries.priceToCoordinate(selectedPos.tp2);

                if (entryY === null || slY === null) return;

                // 1. SL Risk Kutusu (Açık Orta Kırmızı)
                const slTop = Math.min(entryY, slY);
                const slHeight = Math.abs(slY - entryY);
                ctx.fillStyle = 'rgba(239, 68, 68, 0.28)';
                ctx.fillRect(boxStartX, slTop, boxWidth, slHeight);
                ctx.strokeStyle = 'rgba(239, 68, 68, 0.6)';
                ctx.lineWidth = 1;
                ctx.strokeRect(boxStartX, slTop, boxWidth, slHeight);

                // 2. TP1 Kâr Kutusu (Açık Yeşil)
                if (tp1Y !== null) {
                    const tp1Top = Math.min(entryY, tp1Y);
                    const tp1Height = Math.abs(tp1Y - entryY);
                    ctx.fillStyle = 'rgba(74, 222, 128, 0.25)';
                    ctx.fillRect(boxStartX, tp1Top, boxWidth, tp1Height);
                    ctx.strokeStyle = 'rgba(74, 222, 128, 0.6)';
                    ctx.strokeRect(boxStartX, tp1Top, boxWidth, tp1Height);
                }

                // 3. TP2 Kâr Kutusu (TP1'den Daha Koyu Zümrüt Yeşili)
                if (tp2Y !== null && tp1Y !== null) {
                    const tp2Top = Math.min(tp1Y, tp2Y);
                    const tp2Height = Math.abs(tp2Y - tp1Y);
                    ctx.fillStyle = 'rgba(4, 120, 87, 0.40)';
                    ctx.fillRect(boxStartX, tp2Top, boxWidth, tp2Height);
                    ctx.strokeStyle = 'rgba(4, 120, 87, 0.8)';
                    ctx.strokeRect(boxStartX, tp2Top, boxWidth, tp2Height);
                }
            }

            function switchTab(tabId) {
                const pages = ['terminal', 'sentiment', 'news', 'manual', 'excel', 'stats', 'radar', 'journal', 'api'];
                pages.forEach(p => {
                    const el = document.getElementById(`page-${p}`);
                    const tabBtn = document.getElementById(`tab-${p}`);
                    if (el) el.classList.add('hidden');
                    if (tabBtn) tabBtn.classList.remove('active');
                });
                const targetPage = document.getElementById(`page-${tabId}`);
                const targetTab = document.getElementById(`tab-${tabId}`);
                if (targetPage) targetPage.classList.remove('hidden');
                if (targetTab) targetTab.classList.add('active');

                if (tabId === 'terminal') {
                    setTimeout(() => {
                        if (chart) chart.timeScale().fitContent();
                        if (equityChart) equityChart.timeScale().fitContent();
                        resizeCanvas();
                        drawPositionBoxes();
                    }, 50);
                } else if (tabId === 'stats') {
                    recalculatePnlMetrics();
                } else if (tabId === 'excel') {
                    loadReportsList();
                }
            }

            function playAlertSound() {
                try {
                    const ctx = new (window.AudioContext || window.webkitAudioContext)();
                    const osc = ctx.createOscillator();
                    const gain = ctx.createGain();
                    osc.type = "sine";
                    osc.frequency.setValueAtTime(880, ctx.currentTime);
                    osc.frequency.exponentialRampToValueAtTime(1760, ctx.currentTime + 0.15);
                    gain.gain.setValueAtTime(0.3, ctx.currentTime);
                    gain.gain.linearRampToValueAtTime(0.01, ctx.currentTime + 0.25);
                    osc.connect(gain);
                    gain.connect(ctx.destination);
                    osc.start();
                    osc.stop(ctx.currentTime + 0.25);
                } catch(e) {}
            }

            function getPrecisionConfig(price) {
                if (price < 0.001) return { precision: 6, minMove: 0.000001 };
                if (price < 1) return { precision: 4, minMove: 0.0001 };
                if (price < 100) return { precision: 3, minMove: 0.001 };
                return { precision: 2, minMove: 0.01 };
            }

            function initCharts() {
                const container = document.getElementById('tv-container');
                container.innerHTML = '';
                chart = LightweightCharts.createChart(container, {
                    layout: { background: { color: '#121824' }, textColor: '#94a3b8' },
                    grid: { vertLines: { color: '#1e293b' }, horzLines: { color: '#1e293b' } },
                    crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
                    timeScale: { timeVisible: true, secondsVisible: false },
                    rightPriceScale: { autoScale: true, scaleMargins: { top: 0.15, bottom: 0.15 } }
                });

                candleSeries = chart.addCandlestickSeries({
                    upColor: '#10b981', downColor: '#ef4444',
                    borderUpColor: '#10b981', borderDownColor: '#ef4444',
                    wickUpColor: '#10b981', wickDownColor: '#ef4444'
                });

                chart.timeScale().subscribeVisibleLogicalRangeChange(() => {
                    drawPositionBoxes();
                });

                window.addEventListener('resize', () => {
                    resizeCanvas();
                    drawPositionBoxes();
                });

                chart.subscribeCrosshairMove(param => {
                    if (!param.time || !param.seriesData.get(candleSeries)) return;
                    const data = param.seriesData.get(candleSeries);
                    const dec = data.close < 1 ? 6 : 2;
                    document.getElementById('bar-open').innerText = `$${data.open.toFixed(dec)}`;
                    document.getElementById('bar-high').innerText = `$${data.high.toFixed(dec)}`;
                    document.getElementById('bar-low').innerText = `$${data.low.toFixed(dec)}`;
                    document.getElementById('bar-close').innerText = `$${data.close.toFixed(dec)}`;
                });

                const eqContainer = document.getElementById('equity-container');
                eqContainer.innerHTML = '';
                equityChart = LightweightCharts.createChart(eqContainer, {
                    layout: { background: { color: '#121824' }, textColor: '#94a3b8' },
                    grid: { vertLines: { color: '#1e293b' }, horzLines: { color: '#1e293b' } },
                    timeScale: { timeVisible: true, secondsVisible: false },
                    rightPriceScale: { autoScale: true }
                });
                equitySeries = equityChart.addAreaSeries({
                    topColor: 'rgba(56, 189, 248, 0.4)',
                    bottomColor: 'rgba(56, 189, 248, 0.0)',
                    lineColor: '#38bdf8',
                    lineWidth: 2
                });

                resizeCanvas();
            }

            function parseBybitSymbol(symbol) {
                return symbol.replace('/USDT:USDT', 'USDT').replace('/USDT', 'USDT').replace(':', '');
            }

            function changeTimeframe(tf) {
                currentTimeframe = tf;
                document.querySelectorAll('.tf-btn').forEach(b => b.classList.remove('active'));
                const btn = document.getElementById(`tf-${tf}`);
                if (btn) btn.classList.add('active');
                loadChartCandles(currentSymbol, selectedPos, false);
            }

            function changePnlFilter(filter) {
                currentPnlFilter = filter;
                document.querySelectorAll('.pnl-tf-btn').forEach(b => b.classList.remove('active'));
                const btn = document.getElementById(`pnl-tf-${filter}`);
                if (btn) btn.classList.add('active');
                recalculatePnlMetrics();
            }

            function getTurkeyTimeBoundaries() {
                const now = new Date();
                const trOffset = 3 * 60;
                const localOffset = now.getTimezoneOffset();
                const trNow = new Date(now.getTime() + (trOffset + localOffset) * 60 * 1000);

                const todayStart = new Date(trNow.getFullYear(), trNow.getMonth(), trNow.getDate(), 0, 0, 0);
                const todayStartTs = Math.floor((todayStart.getTime() - (trOffset + localOffset) * 60 * 1000) / 1000);

                const yesterdayStart = new Date(todayStart.getTime() - 24 * 60 * 60 * 1000);
                const yesterdayStartTs = Math.floor((yesterdayStart.getTime() - (trOffset + localOffset) * 60 * 1000) / 1000);

                const dayOfWeek = trNow.getDay() === 0 ? 6 : trNow.getDay() - 1;
                const weekStart = new Date(todayStart.getTime() - dayOfWeek * 24 * 60 * 60 * 1000);
                const weekStartTs = Math.floor((weekStart.getTime() - (trOffset + localOffset) * 60 * 1000) / 1000);

                const monthStart = new Date(trNow.getFullYear(), trNow.getMonth(), 1, 0, 0, 0);
                const monthStartTs = Math.floor((monthStart.getTime() - (trOffset + localOffset) * 60 * 1000) / 1000);

                return { todayStartTs, yesterdayStartTs, yesterdayEndTs: todayStartTs, weekStartTs, monthStartTs };
            }

            function recalculatePnlMetrics() {
                const boundaries = getTurkeyTimeBoundaries();
                let filtered = [];

                tradeHistoryCache.forEach(h => {
                    const ts = h.close_timestamp || 0;
                    if (currentPnlFilter === 'today' && ts >= boundaries.todayStartTs) filtered.push(h);
                    else if (currentPnlFilter === 'yesterday' && ts >= boundaries.yesterdayStartTs && ts < boundaries.yesterdayEndTs) filtered.push(h);
                    else if (currentPnlFilter === 'week' && ts >= boundaries.weekStartTs) filtered.push(h);
                    else if (currentPnlFilter === 'month' && ts >= boundaries.monthStartTs) filtered.push(h);
                    else if (currentPnlFilter === 'all') filtered.push(h);
                });

                let periodPnl = 0;
                let winCount = 0;
                let totalWin = 0, totalLoss = 0, winOps = 0, lossOps = 0;
                let longWins = 0, longTotal = 0, shortWins = 0, shortTotal = 0;
                let symbolPnlMap = {};

                filtered.forEach(h => {
                    periodPnl += h.realized_pnl;
                    if (h.realized_pnl > 0) {
                        winCount++;
                        totalWin += h.realized_pnl;
                        winOps++;
                    } else {
                        totalLoss += Math.abs(h.realized_pnl);
                        lossOps++;
                    }

                    if (h.direction === 'LONG') {
                        longTotal++;
                        if (h.realized_pnl > 0) longWins++;
                    } else {
                        shortTotal++;
                        if (h.realized_pnl > 0) shortWins++;
                    }

                    symbolPnlMap[h.symbol] = (symbolPnlMap[h.symbol] || 0) + h.realized_pnl;
                });

                periodPnl = Math.round(periodPnl * 100) / 100;
                const winRate = filtered.length > 0 ? ((winCount / filtered.length) * 100).toFixed(1) : "0.0";

                const pnlElem = document.getElementById('stat-pnl');
                pnlElem.innerText = `${periodPnl >= 0 ? '+' : ''}$${periodPnl.toFixed(2)}`;
                pnlElem.className = `text-sm font-extrabold font-mono ${periodPnl >= 0 ? 'text-emerald-400' : 'text-red-400'}`;

                document.getElementById('stat-winrate').innerText = `%${winRate}`;
                document.getElementById('stat-trades').innerText = filtered.length;

                // İstatistik Sayfası Metrikleri
                const pf = totalLoss > 0 ? (totalWin / totalLoss).toFixed(2) : (totalWin > 0 ? "∞" : "0.00");
                const avgWin = winOps > 0 ? (totalWin / winOps).toFixed(2) : "0.00";
                const avgLoss = lossOps > 0 ? (totalLoss / lossOps).toFixed(2) : "0.00";

                const lWr = longTotal > 0 ? Math.round((longWins / longTotal) * 100) : 0;
                const sWr = shortTotal > 0 ? Math.round((shortWins / shortTotal) * 100) : 0;

                const statPfElem = document.getElementById('stat-pf');
                if (statPfElem) statPfElem.innerText = pf;
                const statAvgWinElem = document.getElementById('stat-avg-win');
                if (statAvgWinElem) statAvgWinElem.innerText = `+$${avgWin}`;
                const statAvgLossElem = document.getElementById('stat-avg-loss');
                if (statAvgLossElem) statAvgLossElem.innerText = `-$${avgLoss}`;
                const statLsElem = document.getElementById('stat-long-short-win');
                if (statLsElem) statLsElem.innerText = `L: %${lWr} | S: %${sWr}`;

                const topSymbolsTbody = document.getElementById('top-symbols-table');
                if (topSymbolsTbody) {
                    const sortedSymbols = Object.keys(symbolPnlMap).sort((a,b) => symbolPnlMap[b] - symbolPnlMap[a]);
                    topSymbolsTbody.innerHTML = sortedSymbols.slice(0, 8).map(sym => `
                        <tr>
                            <td class="py-2 font-bold text-white">${sym}</td>
                            <td class="text-slate-400">${tradeHistoryCache.filter(h => h.symbol === sym).length}</td>
                            <td class="font-bold ${symbolPnlMap[sym] >= 0 ? 'text-emerald-400' : 'text-red-400'}">${symbolPnlMap[sym] >= 0 ? '+' : ''}$${symbolPnlMap[sym].toFixed(2)}</td>
                        </tr>
                    `).join('') || '<tr><td colspan="3" class="py-2 text-slate-500 italic">Veri bulunmuyor...</td></tr>';
                }
            }

            async function loadReportsList() {
                try {
                    const res = await fetch('/api/reports/list');
                    const files = await res.json();
                    const tbody = document.getElementById('reports-table');
                    if (tbody) {
                        tbody.innerHTML = files.map(f => `
                            <tr>
                                <td class="py-2 font-mono text-slate-300">📄 ${f}</td>
                                <td class="text-right">
                                    <a href="/api/reports/download/${f}" class="bg-sky-600 hover:bg-sky-500 text-white font-bold px-2 py-1 rounded text-[10px]">İndir</a>
                                </td>
                            </tr>
                        `).join('') || '<tr><td colspan="2" class="py-2 text-slate-500 italic">Henüz arşivlenmiş dosya yok...</td></tr>';
                    }
                } catch(e) {}
            }

            async function fetchCandlesDirect(symbol, interval = '5') {
                const rawSym = parseBybitSymbol(symbol);
                const url = `https://api.bybit.com/v5/market/kline?category=linear&symbol=${rawSym}&interval=${interval}&limit=1000`;
                try {
                    const res = await fetch(url);
                    const json = await res.json();
                    if (json.result && json.result.list) {
                        return json.result.list.map(c => ({
                            time: Math.floor(parseInt(c[0]) / 1000),
                            open: parseFloat(c[1]),
                            high: parseFloat(c[2]),
                            low: parseFloat(c[3]),
                            close: parseFloat(c[4])
                        })).sort((a, b) => a.time - b.time);
                    }
                } catch(e) {}
                return [];
            }

            async function loadChartCandles(symbol, posData = null, isLiveTick = false) {
                try {
                    const candles = await fetchCandlesDirect(symbol, currentTimeframe);
                    if (candles.length > 0) {
                        const lastCandle = candles[candles.length - 1];
                        const pConf = getPrecisionConfig(lastCandle.close);
                        candleSeries.applyOptions({
                            priceFormat: { type: 'price', precision: pConf.precision, minMove: pConf.minMove }
                        });
                        candleSeries.setData(candles);

                        if (!isLiveTick) {
                            chart.priceScale('right').applyOptions({ autoScale: true });
                            chart.timeScale().fitContent();
                            chart.timeScale().resetTimeScale();

                            const dec = lastCandle.close < 1 ? pConf.precision : 2;
                            document.getElementById('bar-open').innerText = `$${lastCandle.open.toFixed(dec)}`;
                            document.getElementById('bar-high').innerText = `$${lastCandle.high.toFixed(dec)}`;
                            document.getElementById('bar-low').innerText = `$${lastCandle.low.toFixed(dec)}`;
                            document.getElementById('bar-close').innerText = `$${lastCandle.close.toFixed(dec)}`;
                        }

                        resizeCanvas();
                        drawPositionBoxes();
                    }

                    if (!isLiveTick) {
                        priceLines.forEach(l => candleSeries.removePriceLine(l));
                        priceLines = [];
                        const tfLabel = currentTimeframe === '60' ? '1H' : (currentTimeframe === '240' ? '4H' : (currentTimeframe === 'D' ? '1D' : `${currentTimeframe}M`));
                        document.getElementById('chart-title').innerText = `${symbol} (${tfLabel})`;

                        if (posData) {
                            const entryLine = candleSeries.createPriceLine({ price: posData.entry, color: '#38bdf8', lineWidth: 2, lineStyle: LightweightCharts.LineStyle.Solid, axisLabelVisible: true, title: 'GİRİŞ' });
                            const slLine = candleSeries.createPriceLine({ price: posData.sl, color: '#ef4444', lineWidth: 2, lineStyle: LightweightCharts.LineStyle.Dashed, axisLabelVisible: true, title: 'STOP (SL)' });
                            const tp1Line = candleSeries.createPriceLine({ price: posData.tp1, color: '#4ade80', lineWidth: 2, lineStyle: LightweightCharts.LineStyle.Dashed, axisLabelVisible: true, title: 'TP1 (15M)' });
                            const tp2Line = candleSeries.createPriceLine({ price: posData.tp2, color: '#047857', lineWidth: 2, lineStyle: LightweightCharts.LineStyle.Dashed, axisLabelVisible: true, title: 'TP2 (Likidite)' });
                            priceLines.push(entryLine, slLine, tp1Line, tp2Line);

                            const p = posData.entry < 1 ? 6 : 2;
                            document.getElementById('chart-levels').innerHTML = `
                                <span class="text-sky-400 font-mono">Giriş: ${posData.entry}</span> | 
                                <span class="text-red-400 font-mono">SL: ${posData.sl.toFixed(p)}</span> | 
                                <span class="text-green-400 font-mono">TP1: ${posData.tp1.toFixed(p)}</span> | 
                                <span class="text-emerald-600 font-mono">TP2: ${posData.tp2.toFixed(p)}</span>
                            `;
                        } else {
                            document.getElementById('chart-levels').innerHTML = '';
                        }
                    }
                } catch(e) {}
            }

            function selectPosition(pos) {
                selectedPos = pos;
                currentSymbol = pos.symbol;
                localStorage.setItem("selected_sym", pos.symbol);
                renderRationale(pos);
                loadChartCandles(pos.symbol, pos, false);
            }

            function renderRationale(pos) {
                if (!pos) return;
                const p = pos.entry < 1 ? 6 : 4;
                const modeLabel = pos.margin_mode === "ISOLATED" ? "İzole" : "Cross";
                document.getElementById('active-rationale').innerHTML = `
                    <div class="flex justify-between items-center mb-2">
                        <span class="font-bold text-base text-white">${pos.symbol}</span>
                        <span class="px-2 py-0.5 rounded text-xs font-bold ${pos.direction === 'LONG' ? 'bg-emerald-500/20 text-emerald-400' : 'bg-red-500/20 text-red-400'}">${pos.direction} (${pos.score} Puan)</span>
                    </div>
                    <div class="space-y-1 text-slate-300">
                        ${pos.reasons.map(r => `<div class="bg-slate-900/60 p-1.5 rounded border border-slate-800">✓ ${r}</div>`).join('')}
                    </div>
                    <div class="mt-2 p-2 bg-black/40 rounded border border-slate-800 text-[11px] space-y-1">
                        <div class="text-slate-400">Giriş Saati: <span class="text-white font-bold">${pos.open_time}</span></div>
                        <div class="text-slate-400">Kaldıraç & Mod: <span class="text-white font-bold">${pos.leverage}x ${modeLabel} ($${pos.margin})</span></div>
                        <div class="text-red-400">Stop-Loss: <span class="font-mono">${pos.sl.toFixed(p)}</span> (Maks Risk: $${pos.max_loss})</div>
                        <div class="text-emerald-400">TP1 / TP2 (Dinamik): <span class="font-mono">${pos.tp1.toFixed(p)} / ${pos.tp2.toFixed(p)}</span></div>
                    </div>
                `;
            }

            async function manualClosePos(symbol) {
                await fetch('/api/manual/close_position', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({symbol})
                });
                updateDashboard();
            }

            async function manualCloseAll() {
                if (confirm("Tüm açık pozisyonları kapatmak istediğinize emin misiniz?")) {
                    await fetch('/api/manual/close_all', { method: 'POST' });
                    updateDashboard();
                }
            }

            async function manualBreakeven(symbol) {
                await fetch('/api/manual/breakeven', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({symbol})
                });
                updateDashboard();
            }

            async function manualUpdateSlTp(symbol) {
                const sl = parseFloat(document.getElementById(`manual-sl-${symbol}`).value);
                const tp2 = parseFloat(document.getElementById(`manual-tp-${symbol}`).value);
                await fetch('/api/manual/update_sltp', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({symbol, sl, tp2})
                });
                alert("SL ve TP Güncellendi!");
                updateDashboard();
            }

            async function saveSettings() {
                const total_balance = parseFloat(document.getElementById('input-balance').value);
                const risk_pct = parseFloat(document.getElementById('input-risk').value);
                const leverage = parseInt(document.getElementById('input-leverage').value);
                const margin_mode = document.getElementById('input-margin-mode').value;
                const max_open_positions = parseInt(document.getElementById('input-max-pos').value);
                const max_total_margin_pct = parseFloat(document.getElementById('input-max-margin-pct').value);

                await fetch('/api/update_settings', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({total_balance, risk_pct, leverage, margin_mode, max_open_positions, max_total_margin_pct})
                });
                updateDashboard();
            }

            async function saveApiSettings() {
                const exchange = document.getElementById('api-exchange').value;
                const mode = document.getElementById('api-mode').value;
                const api_key = document.getElementById('api-key').value;
                const api_secret = document.getElementById('api-secret').value;
                const auto_trade = false;

                await fetch('/api/update_api', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({exchange, mode, api_key, api_secret, auto_trade})
                });
                alert("API Ayarları Kaydedildi!");
                updateDashboard();
            }

            async function updateDashboard() {
                try {
                    const res = await fetch('/api/state');
                    const data = await res.json();

                    if (data.active_positions.length > lastKnownPosCount) playAlertSound();
                    lastKnownPosCount = data.active_positions.length;

                    document.getElementById('scanned-count').innerText = data.scanned_count;
                    document.getElementById('last-scan').innerText = data.last_scan_time;

                    // Canlı Duyarlılık Verileri
                    if (data.fear_and_greed) {
                        document.getElementById('fng-val').innerText = data.fear_and_greed.value;
                        document.getElementById('fng-text').innerText = data.fear_and_greed.classification;
                        document.getElementById('fng-bar').style.width = `${data.fear_and_greed.value}%`;
                    }
                    if (data.sentiment_data) {
                        const sent15m = document.getElementById('sent-btc-15m');
                        if (sent15m) {
                            sent15m.innerText = `${data.btc_15m_change >= 0 ? '+' : ''}%${data.btc_15m_change}`;
                            sent15m.className = `font-bold font-mono ${data.btc_15m_change >= 0 ? 'text-emerald-400' : 'text-red-400'}`;
                        }
                        const sentRsi = document.getElementById('sent-btc-rsi');
                        if (sentRsi) sentRsi.innerText = data.sentiment_data.btc_rsi;
                        const sentVol = document.getElementById('sent-btc-vol');
                        if (sentVol) sentVol.innerText = data.sentiment_data.btc_volume_24h;
                        const sentBias = document.getElementById('sent-bias');
                        if (sentBias) {
                            sentBias.innerText = data.sentiment_data.market_bias;
                            sentBias.className = data.sentiment_data.market_bias.includes('BOĞA') ? 'text-emerald-400' : 'text-red-400';
                        }
                        const sentShock = document.getElementById('sent-shock-status');
                        if (sentShock) {
                            sentShock.innerText = data.btc_shock_lock ? data.btc_shock_reason : "GÜVENLİ / NORMAL";
                            sentShock.className = data.btc_shock_lock ? "text-rose-400 font-bold animate-pulse" : "text-emerald-400 font-bold";
                        }
                    }

                    // BTC Şok Rozeti
                    const shockBadge = document.getElementById('btc-shock-badge');
                    if (shockBadge) {
                        if (data.btc_shock_lock) {
                            shockBadge.classList.remove('hidden');
                            shockBadge.innerText = data.btc_shock_reason;
                        } else {
                            shockBadge.classList.add('hidden');
                        }
                    }

                    const btcBadge = document.getElementById('btc-regime-badge');
                    btcBadge.innerText = data.btc_regime || "BTC: AKTİF";

                    const totalUsedMargin = data.active_positions.reduce((acc, p) => acc + p.margin, 0);
                    const usedPct = data.total_balance > 0 ? ((totalUsedMargin / data.total_balance) * 100).toFixed(1) : "0.0";
                    document.getElementById('stat-used-margin').innerText = `$${totalUsedMargin.toFixed(1)} (%${usedPct})`;

                    tradeHistoryCache = data.trade_history;
                    recalculatePnlMetrics();

                    if (data.equity_curve && data.equity_curve.length > 0) equitySeries.setData(data.equity_curve);

                    const logBox = document.getElementById('log-box');
                    logBox.innerHTML = data.logs.map(l => `<div>${l}</div>`).join('');

                    lastPositions = data.active_positions;
                    const activeTbody = document.getElementById('active-pos-table');
                    activeTbody.innerHTML = data.active_positions.map((p, idx) => {
                        const dec = p.entry < 1 ? 6 : 4;
                        const modeStr = p.margin_mode === "ISOLATED" ? "İzole" : "Cross";
                        const pnlVal = (p.unrealized_pnl !== undefined) ? p.unrealized_pnl : 0.0;
                        const pnlColor = pnlVal >= 0 ? "text-emerald-400" : "text-red-400";
                        const progVal = (p.progress_pct !== undefined) ? p.progress_pct : 0.0;

                        return `
                        <tr class="hover:bg-slate-800/80 cursor-pointer ${selectedPos && selectedPos.symbol === p.symbol ? 'bg-slate-800/60' : ''}" onclick="selectPosition(lastPositions[${idx}])">
                            <td class="py-2 font-bold text-white">${p.symbol}</td>
                            <td class="text-slate-400 font-mono text-[10px]">${p.open_time}</td>
                            <td class="${p.direction === 'LONG' ? 'text-emerald-400' : 'text-red-400'} font-bold">${p.direction} (${p.leverage}x ${modeStr})</td>
                            <td class="text-white font-mono">$${p.margin}</td>
                            <td class="font-mono text-slate-300">${p.entry}</td>
                            <td class="font-mono text-white font-bold">${p.current_price || p.entry}</td>
                            <td class="font-mono font-bold ${pnlColor}">${pnlVal >= 0 ? '+' : ''}$${pnlVal.toFixed(2)}</td>
                            <td class="w-24">
                                <div class="w-full bg-slate-800 rounded-full h-1.5 overflow-hidden">
                                    <div class="bg-emerald-500 h-1.5 rounded-full" style="width: ${progVal}%"></div>
                                </div>
                                <span class="text-[9px] text-slate-400 font-mono">%${progVal.toFixed(1)}</span>
                            </td>
                        </tr>
                    `}).join('');

                    const manualTbody = document.getElementById('manual-pos-table');
                    manualTbody.innerHTML = data.active_positions.map(p => `
                        <tr>
                            <td class="py-2 font-bold text-white">${p.symbol}</td>
                            <td class="${p.direction === 'LONG' ? 'text-emerald-400' : 'text-red-400'} font-bold">${p.direction}</td>
                            <td class="font-mono text-slate-300">${p.entry} / ${p.current_price || p.entry}</td>
                            <td class="font-bold ${p.unrealized_pnl >= 0 ? 'text-emerald-400' : 'text-red-400'}">${p.unrealized_pnl >= 0 ? '+' : ''}$${p.unrealized_pnl.toFixed(2)}</td>
                            <td><input id="manual-sl-${p.symbol}" type="number" step="any" value="${p.sl}" class="bg-slate-900 border border-slate-700 w-20 px-1 py-0.5 rounded text-white font-mono"></td>
                            <td><input id="manual-tp-${p.symbol}" type="number" step="any" value="${p.tp2}" class="bg-slate-900 border border-slate-700 w-20 px-1 py-0.5 rounded text-white font-mono"></td>
                            <td class="text-right space-x-1">
                                <button onclick="manualUpdateSlTp('${p.symbol}')" class="bg-slate-700 hover:bg-slate-600 px-2 py-1 rounded text-[10px] text-white">Kaydet</button>
                                <button onclick="manualBreakeven('${p.symbol}')" class="bg-sky-600 hover:bg-sky-500 px-2 py-1 rounded text-[10px] text-white">Başa Baş</button>
                                <button onclick="manualClosePos('${p.symbol}')" class="bg-rose-600 hover:bg-rose-500 px-2 py-1 rounded text-[10px] text-white font-bold">Kapat</button>
                            </td>
                        </tr>
                    `).join('') || '<tr><td colspan="7" class="py-3 text-slate-500 italic">Şu an açık pozisyon bulunmuyor...</td></tr>';

                    const journalTbody = document.getElementById('journal-table');
                    journalTbody.innerHTML = data.trade_history.map(h => `
                        <tr class="hover:bg-slate-800/40">
                            <td class="py-2 font-mono text-slate-400">${h.close_time}</td>
                            <td class="font-bold text-white">${h.symbol}</td>
                            <td class="${h.direction === 'LONG' ? 'text-emerald-400' : 'text-red-400'} font-bold">${h.direction}</td>
                            <td class="font-mono">${h.entry} ➔ ${h.close_price}</td>
                            <td class="font-bold ${h.realized_pnl >= 0 ? 'text-emerald-400' : 'text-red-400'}">
                                ${h.realized_pnl >= 0 ? '+' : ''}$${h.realized_pnl.toFixed(2)} (%${h.pnl_pct})
                            </td>
                            <td class="text-[10px] text-sky-300 font-semibold">${h.close_reason}</td>
                        </tr>
                    `).join('') || '<tr><td colspan="6" class="py-4 text-center text-slate-500 italic">Henüz kapanan bir işlem kaydı yok...</td></tr>';

                    const radarTbody = document.getElementById('radar-table');
                    if (data.radar_symbols && data.radar_symbols.length > 0) {
                        const sortedRadar = [...data.radar_symbols].sort((a,b) => b.score - a.score);
                        radarTbody.innerHTML = sortedRadar.map(r => `
                            <tr class="hover:bg-slate-800/40 cursor-pointer" onclick="currentSymbol='${r.symbol}'; switchTab('terminal'); loadChartCandles('${r.symbol}', null, false);">
                                <td class="py-2 font-bold text-white">${r.symbol}</td>
                                <td class="font-mono">$${r.price}</td>
                                <td class="font-bold ${r.trend === 'LONG' ? 'text-emerald-400' : (r.trend === 'SHORT' ? 'text-red-400' : 'text-slate-400')}">${r.trend}</td>
                                <td class="font-mono">${r.rsi}</td>
                                <td class="font-mono text-amber-400">${r.vol_ratio}x</td>
                                <td><span class="px-2 py-0.5 rounded text-[10px] font-bold ${r.score >= 75 ? 'bg-emerald-500/20 text-emerald-400' : 'bg-slate-800 text-slate-400'}">${r.score} Puan</span></td>
                            </tr>
                        `).join('');
                    }

                    if (currentSymbol) loadChartCandles(currentSymbol, selectedPos, true);

                    if (!selectedPos && data.active_positions.length > 0) {
                        const saved = localStorage.getItem("selected_sym");
                        const target = data.active_positions.find(p => p.symbol === saved) || data.active_positions[0];
                        selectPosition(target);
                    } else if (selectedPos) {
                        const updated = data.active_positions.find(p => p.symbol === selectedPos.symbol);
                        if (updated) {
                            selectedPos = updated;
                            renderRationale(updated);
                        }
                    }
                } catch (e) {}
            }

            initCharts();
            loadChartCandles(currentSymbol, null, false);
            setInterval(updateDashboard, 2000);
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
