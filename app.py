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
    "locked_margin": 0.0,
    "free_balance": 1000.0,
    "peak_balance": 1000.0,
    "max_drawdown_pct": 0.0,
    "risk_pct": 5.0,
    "leverage": 50,
    "margin_mode": "ISOLATED",
    "max_open_positions": 5,
    "max_total_margin_pct": 50.0,
    "daily_drawdown_limit_pct": 10.0,
    "daily_loss_locked": False,
    "daily_start_balance": 1000.0,
    "last_day_reset": get_now_datetime().strftime("%Y-%m-%d"),
    "bot_trading_active": True,
    "btc_regime": "🟢 BOĞA (YÜKSELİŞ)",
    "btc_15m_change": 0.5,
    "btc_shock_lock": False,
    "btc_shock_reason": "",
    "fear_and_greed": {"value": 66, "classification": "Açgözlülük"},
    "sentiment_data": {
        "btc_rsi": 52.2,
        "btc_volume_24h": "$3.87 Milyar",
        "market_bias": "BOĞA / LONG",
        "long_short_ratio": 52.4,
        "market_volatility": "DÜŞÜK",
        "total_liquidations_24h": "$143.7 Milyon",
        "long_liq_pct": 57.5,
        "short_liq_pct": 42.5,
        "total_oi_change": "-%1.2",
        "btc_dominance": "%57.6",
        "avg_funding_rate": "+0.0098%",
        "bid_pressure": 54.2,
        "ask_pressure": 45.8,
        "whale_inflow": "$420M USDT",
        "whale_outflow": "$180M USDT",
        "net_whale_flow": "+$240M (Boğa / Giriş)"
    },
    "scanned_count": 75,
    "last_scan_time": get_now_str(),
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
    "logs": ["[İSLEM] Sistem Başarıyla Başlatıldı. Tablolar Hazır."]
}

EXCLUDED_KEYWORDS = [
    'NVDA', 'GOOGL', 'AAPL', 'TSLA', 'MSFT', 'AMZN', 'META', 'NFLX', 'AMD', 'COIN',
    'BABA', 'PLTR', 'SOXS', 'SOXL', 'QQQ', 'SPY', 'WDC', 'DELL', 'IONQ', 'GLW', 'BIRB',
    'TBT', 'TLT', 'PDD', 'NIO', 'BILI', 'LI', 'XPEV', 'MSTR', 'MARA', 'RIOT', 'CLSK',
    'CASHCAT', 'WLFI', 'TRUMP', 'MELANIA', 'PEPE2', 'SHIB2'
]

def update_wallet_pools():
    system_state["locked_margin"] = round(sum(p['margin'] for p in system_state["active_positions"]), 2)
    system_state["free_balance"] = round(max(0.0, system_state["total_balance"] - system_state["locked_margin"]), 2)

def translate_fng(classification_en):
    mapping = {
        "Extreme Fear": "Aşırı Korku",
        "Fear": "Korku",
        "Neutral": "Nötr",
        "Greed": "Açgözlülük",
        "Extreme Greed": "Aşırı Açgözlülük"
    }
    return mapping.get(classification_en, classification_en)

def add_log(msg: str):
    ts = get_now_str()
    system_state["logs"].insert(0, f"[{ts}] {msg}")
    if len(system_state["logs"]) > 60:
        system_state["logs"].pop()

def check_daily_drawdown():
    now_str = get_now_datetime().strftime("%Y-%m-%d")
    if system_state["last_day_reset"] != now_str:
        system_state["last_day_reset"] = now_str
        system_state["daily_start_balance"] = system_state["total_balance"]
        system_state["daily_loss_locked"] = False
        add_log("🌅 TSİ 00:00: Günlük Kasa Dengesi ve Drawdown Limiti Sıfırlandı.")

    if system_state["total_balance"] > system_state["peak_balance"]:
        system_state["peak_balance"] = system_state["total_balance"]
    
    peak = system_state["peak_balance"]
    curr = system_state["total_balance"]
    if peak > 0:
        dd = ((peak - curr) / peak) * 100
        if dd > system_state["max_drawdown_pct"]:
            system_state["max_drawdown_pct"] = round(dd, 2)

    daily_loss = system_state["daily_start_balance"] - system_state["total_balance"]
    max_allowed_loss = system_state["daily_start_balance"] * (system_state["daily_drawdown_limit_pct"] / 100.0)
    
    if daily_loss >= max_allowed_loss and not system_state["daily_loss_locked"]:
        system_state["daily_loss_locked"] = True
        add_log(f"🛑 GÜNLÜK ZARAR LİMİTİ TETİKLENDİ: -${daily_loss:.2f} (%{system_state['daily_drawdown_limit_pct']}) Kayıp.")

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
                    tr_class = translate_fng(item.get('value_classification', 'Neutral'))
                    system_state["fear_and_greed"] = {"value": int(item['value']), "classification": tr_class}
    except Exception:
        pass

async def update_btc_metrics(exchange):
    try:
        candles_15m = await exchange.fetch_ohlcv('BTC/USDT:USDT', timeframe='15m', limit=10)
        candles_1h = await exchange.fetch_ohlcv('BTC/USDT:USDT', timeframe='1h', limit=40)
        
        df_15m = pd.DataFrame(candles_15m, columns=['t', 'open', 'high', 'low', 'close', 'volume'])
        df_1h = calculate_indicators(pd.DataFrame(candles_1h, columns=['t', 'open', 'high', 'low', 'close', 'volume']))
        
        last_1h = df_1h.iloc[-1]
        c_now = df_15m['close'].iloc[-1]
        c_prev = df_15m['open'].iloc[-1]
        pct_15m = ((c_now - c_prev) / c_prev) * 100
        system_state["btc_15m_change"] = round(pct_15m, 2)

        if last_1h['close'] > last_1h['ema50']:
            system_state["btc_regime"] = "🟢 BOĞA (YÜKSELİŞ)"
        else:
            system_state["btc_regime"] = "🔴 AYI (DÜŞÜŞ)"
    except Exception:
        system_state["btc_regime"] = "BTC: AKTİF"

async def analyze_symbol(exchange, symbol):
    try:
        if system_state["daily_loss_locked"] or not system_state["bot_trading_active"]:
            return None

        base = symbol.split('/')[0].upper()
        if any(exc in base for exc in EXCLUDED_KEYWORDS):
            return None

        tasks = [
            exchange.fetch_ohlcv(symbol, timeframe='5m', limit=50),
            exchange.fetch_ohlcv(symbol, timeframe='1h', limit=50),
            exchange.fetch_ohlcv(symbol, timeframe='4h', limit=30),
            exchange.fetch_open_interest_history(symbol, timeframe='5m', limit=6)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        if any(isinstance(r, Exception) or not r or len(r) < 25 for r in results[:3]):
            return None

        df_5m = calculate_indicators(pd.DataFrame(results[0], columns=['t', 'open', 'high', 'low', 'close', 'volume']))
        df_1h = calculate_indicators(pd.DataFrame(results[1], columns=['t', 'open', 'high', 'low', 'close', 'volume']))
        df_4h = calculate_indicators(pd.DataFrame(results[2], columns=['t', 'open', 'high', 'low', 'close', 'volume']))
        oi_data = results[3] if not isinstance(results[3], Exception) and results[3] else []

        c_5m = df_5m.iloc[-1]
        c_1h = df_1h.iloc[-1]
        c_4h = df_4h.iloc[-1]

        score = 0
        direction = None
        reasons = []

        trend_bull = (c_1h['close'] > c_1h['ema20'] and c_1h['ema20'] > c_1h['ema50']) and (c_4h['close'] > c_4h['ema50'])
        trend_bear = (c_1h['close'] < c_1h['ema20'] and c_1h['ema20'] < c_1h['ema50']) and (c_4h['close'] < c_4h['ema50'])

        if trend_bull:
            direction = "LONG"
            score += 35
            reasons.append("📈 1H & 4H Güçlü Boğa Trend Dizilimi")
        elif trend_bear:
            direction = "SHORT"
            score += 35
            reasons.append("📉 1H & 4H Güçlü Ayı Trend Dizilimi")
        else:
            return None

        dist_to_ema20 = abs(c_5m['close'] - c_5m['ema20']) / c_5m['close']
        if dist_to_ema20 <= 0.012:
            score += 25
            reasons.append("🎯 Akıllı Geri Çekilme (EMA 20)")
        else:
            return None

        vol_ratio = float(c_5m['volume'] / (c_5m['vol_ma'] + 1e-9)) if pd.notnull(c_5m['vol_ma']) else 1.0
        if vol_ratio >= 1.5:
            score += 20
            reasons.append(f"🔥 Güçlü Momentum ({vol_ratio:.1f}x)")
        else:
            return None

        if score < 75:
            return None

        entry = float(c_5m['close'])
        atr = float(c_5m['atr']) if pd.notnull(c_5m['atr']) else entry * 0.008

        effective_leverage = system_state["leverage"]
        try:
            market_info = exchange.markets.get(symbol, {})
            max_lev_allowed = market_info.get('limits', {}).get('leverage', {}).get('max', 50)
            if effective_leverage > max_lev_allowed:
                effective_leverage = int(max_lev_allowed)
        except Exception:
            pass

        if direction == "LONG":
            sl = float(df_5m['low'].iloc[-12:].min() - (2.2 * atr))
            if (entry - sl) / entry < 0.015: sl = entry * 0.985
            risk_dist = entry - sl
            tp1 = entry + (1.5 * risk_dist)
            tp2 = entry + (3.0 * risk_dist)
        else:
            sl = float(df_5m['high'].iloc[-12:].max() + (2.2 * atr))
            if (sl - entry) / entry < 0.015: sl = entry * 1.015
            risk_dist = sl - entry
            tp1 = entry - (1.5 * risk_dist)
            tp2 = entry - (3.0 * risk_dist)

        pos_size, margin, max_loss = compute_position_metrics(entry, sl)

        return {
            "symbol": symbol, "direction": direction, "score": score, "entry": entry,
            "sl": sl, "tp1": tp1, "tp2": tp2, "pos_size": pos_size, "margin": margin,
            "max_loss": max_loss, "leverage": effective_leverage, "margin_mode": system_state["margin_mode"],
            "tp1_hit": False, "trailing_active": False, "active_size": pos_size,
            "current_price": entry, "unrealized_pnl": 0.0, "progress_pct": 0.0,
            "reasons": reasons, "open_time": get_now_str(), "open_timestamp": int(get_now_datetime().timestamp())
        }
    except Exception:
        return None

async def keep_alive_loop():
    while True:
        await asyncio.sleep(600)

async def market_scanner_loop():
    await asyncio.sleep(2)
    add_log("Quant Motoru: Sistem Tarama Döngüsü Aktif.")
    while True:
        exchange = None
        try:
            exchange = ccxt.binance({
                'options': {'defaultType': 'linear'},
                'enableRateLimit': True,
                'timeout': 10000
            })
            check_daily_drawdown()
            update_wallet_pools()
            await update_btc_metrics(exchange)
            await fetch_fear_greed()

            markets = await exchange.load_markets()
            tickers = await exchange.fetch_tickers()
            crypto_symbols = []
            for s, m in markets.items():
                if m.get('quote') == 'USDT' and m.get('linear') and m.get('active') and not m.get('delivery') and not '-' in s:
                    base = s.split('/')[0].upper()
                    if not any(exc in base for exc in EXCLUDED_KEYWORDS):
                        t_data = tickers.get(s, {})
                        if (t_data.get('quoteVolume', 0) or 0) >= 10_000_000:
                            crypto_symbols.append(s)

            system_state["scanned_count"] = len(crypto_symbols)
            for i in range(0, len(crypto_symbols), 10):
                chunk = crypto_symbols[i:i + 10]
                tasks = [analyze_symbol(exchange, s) for s in chunk]
                signals = await asyncio.gather(*tasks, return_exceptions=True)
                for sig in signals:
                    if sig and isinstance(sig, dict):
                        exists = any(p['symbol'] == sig['symbol'] for p in system_state["active_positions"])
                        if not exists:
                            max_pos = system_state["max_open_positions"]
                            if max_pos > 0 and len(system_state["active_positions"]) >= max_pos:
                                continue
                            curr_margin = sum(p['margin'] for p in system_state["active_positions"])
                            allowed = system_state["total_balance"] * (system_state["max_total_margin_pct"] / 100.0)
                            if (curr_margin + sig['margin']) > allowed or sig['margin'] > system_state["free_balance"]:
                                continue
                            system_state["active_positions"].append(sig)
                            update_wallet_pools()
                            add_log(f"🟢 POZİSYON AÇILDI: {sig['symbol']} {sig['direction']} | Teminat: ${sig['margin']}")

            system_state["last_scan_time"] = get_now_str()

            for pos in list(system_state["active_positions"]):
                try:
                    ticker = await exchange.fetch_ticker(pos['symbol'])
                    curr_price = ticker['last']
                    pos['current_price'] = curr_price
                    pnl_raw = ((curr_price - pos['entry']) / pos['entry']) if pos['direction'] == "LONG" else ((pos['entry'] - curr_price) / pos['entry'])
                    pos['unrealized_pnl'] = round(pos['active_size'] * pnl_raw, 2)

                    close_reason = None
                    if (pos['direction'] == "LONG" and curr_price <= pos['sl']) or (pos['direction'] == "SHORT" and curr_price >= pos['sl']):
                        close_reason = "❌ Stop-Loss Tetiklendi"
                    elif (pos['direction'] == "LONG" and curr_price >= pos['tp2']) or (pos['direction'] == "SHORT" and curr_price <= pos['tp2']):
                        close_reason = "🎯 TP2 Hedefine Ulaşıldı"

                    if close_reason:
                        pnl_pct = pnl_raw * 100
                        realized_pnl = round(pos['active_size'] * (pnl_pct / 100.0), 2)
                        system_state["total_balance"] += realized_pnl
                        update_wallet_pools()
                        now_dt = get_now_datetime()
                        system_state["equity_curve"].append({"time": int(now_dt.timestamp()), "value": round(system_state["total_balance"], 2)})
                        
                        system_state["trade_history"].insert(0, {
                            "symbol": pos['symbol'], "direction": pos['direction'], "entry": pos['entry'],
                            "close_price": curr_price, "pnl_pct": round(pnl_pct, 2), "realized_pnl": realized_pnl,
                            "score": pos['score'], "duration_mins": 5, "open_reasons": pos['reasons'],
                            "close_reason": close_reason, "close_time": now_dt.strftime("%H:%M:%S"),
                            "close_timestamp": int(now_dt.timestamp())
                        })
                        system_state["active_positions"].remove(pos)
                        update_wallet_pools()
                        add_log(f"🔴 POZİSYON KAPANDI: {pos['symbol']} | PnL: ${realized_pnl}")
                except Exception:
                    pass

            await exchange.close()
            await asyncio.sleep(2)
        except Exception as e:
            if exchange: await exchange.close()
            await asyncio.sleep(2)

@asynccontextmanager
async def lifespan(app: FastAPI):
    t1 = asyncio.create_task(market_scanner_loop())
    t2 = asyncio.create_task(keep_alive_loop())
    yield
    t1.cancel()
    t2.cancel()

app = FastAPI(title="Meta Quant Terminal Pro", lifespan=lifespan)

@app.get("/api/health")
async def health_check(): return {"status": "ok"}

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

class PartialClosePayload(BaseModel):
    symbol: str
    ratio: float

class UpdateSlTpPayload(BaseModel):
    symbol: str
    sl: float
    tp2: float

class DateRangePayload(BaseModel):
    start_date: str
    end_date: str

@app.post("/api/update_settings")
async def update_settings(payload: SettingsPayload):
    system_state["total_balance"] = payload.total_balance
    system_state["risk_pct"] = payload.risk_pct
    system_state["leverage"] = payload.leverage
    system_state["margin_mode"] = payload.margin_mode
    system_state["max_open_positions"] = payload.max_open_positions
    system_state["max_total_margin_pct"] = payload.max_total_margin_pct
    update_wallet_pools()
    add_log("⚙️ Ayarlar Güncellendi.")
    return {"status": "success"}

@app.post("/api/update_api")
async def update_api(payload: ApiPayload):
    system_state["api_settings"] = payload.dict()
    add_log("🔑 API Bilgileri Güncellendi.")
    return {"status": "success"}

@app.post("/api/toggle_bot_trading")
async def toggle_bot_trading():
    system_state["bot_trading_active"] = not system_state["bot_trading_active"]
    return {"status": "success", "active": system_state["bot_trading_active"]}

@app.post("/api/manual/close_position")
async def manual_close_position(payload: ClosePosPayload):
    target = next((p for p in system_state["active_positions"] if p['symbol'] == payload.symbol), None)
    if target:
        system_state["active_positions"].remove(target)
        update_wallet_pools()
        return {"status": "success"}
    return {"status": "error"}

@app.post("/api/manual/close_all")
async def manual_close_all():
    system_state["active_positions"].clear()
    update_wallet_pools()
    return {"status": "success"}

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
            .tf-btn.active { background-color: #10b981; color: #000; font-weight: bold; }
            .pnl-tf-btn.active { background-color: #38bdf8; color: #000; font-weight: bold; }
            .stats-tf-btn.active { background-color: #10b981; color: #000; font-weight: bold; }
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
                    </div>
                </div>
            </div>

            <!-- SAYFA SEKMELERİ -->
            <div class="flex items-center flex-wrap gap-1 bg-slate-900/90 p-1 rounded-xl border border-slate-800 text-xs">
                <button onclick="switchTab('terminal')" id="tab-terminal" class="nav-tab active px-2.5 py-1 rounded-lg text-slate-400 hover:text-white transition">📊 Terminal</button>
                <button onclick="switchTab('sentiment')" id="tab-sentiment" class="nav-tab px-2.5 py-1 rounded-lg text-slate-400 hover:text-white transition">🧠 Duyarlılık</button>
                <button onclick="switchTab('news')" id="tab-news" class="nav-tab px-2.5 py-1 rounded-lg text-slate-400 hover:text-white transition">📰 Haber</button>
                <button onclick="switchTab('manual')" id="tab-manual" class="nav-tab px-2.5 py-1 rounded-lg text-slate-400 hover:text-white transition">🎮 Manuel</button>
                <button onclick="switchTab('excel')" id="tab-excel" class="nav-tab px-2.5 py-1 rounded-lg text-slate-400 hover:text-white transition">📑 Excel</button>
                <button onclick="switchTab('stats')" id="tab-stats" class="nav-tab px-2.5 py-1 rounded-lg text-slate-400 hover:text-white transition">📈 İstatistik</button>
                <button onclick="switchTab('radar')" id="tab-radar" class="nav-tab px-2.5 py-1 rounded-lg text-slate-400 hover:text-white transition">🔥 Radar</button>
                <button onclick="switchTab('journal')" id="tab-journal" class="nav-tab px-2.5 py-1 rounded-lg text-slate-400 hover:text-white transition">📖 Günlük</button>
                <button onclick="switchTab('api')" id="tab-api" class="nav-tab px-2.5 py-1 rounded-lg text-slate-400 hover:text-white transition">⚙️ API</button>
            </div>

            <div class="flex items-center space-x-3 text-xs">
                <button onclick="toggleBotTrading()" id="bot-toggle-btn" class="px-2.5 py-1 rounded-lg font-bold bg-emerald-600 text-black transition">🤖 Bot: AÇIK</button>
                <div class="text-slate-400">Taranan: <span id="scanned-count" class="text-white font-bold">0</span></div>
            </div>
        </div>

        <!-- SAYFA 1: CANLI TERMİNAL -->
        <div id="page-terminal" class="space-y-3">
            <div class="card p-3 rounded-xl flex flex-wrap justify-between items-center gap-3">
                <div class="flex flex-col space-y-1 bg-slate-900/90 p-2 rounded-xl border border-slate-800">
                    <div class="flex items-center space-x-3 pt-0.5">
                        <div>
                            <div class="text-[9px] text-slate-400 uppercase">Bugün Net PnL</div>
                            <div id="stat-pnl" class="text-sm font-extrabold font-mono text-emerald-400">$0.00</div>
                        </div>
                        <div class="border-r border-slate-800 h-6"></div>
                        <div>
                            <div class="text-[9px] text-slate-400 uppercase">Win Rate</div>
                            <div id="stat-winrate" class="text-sm font-extrabold font-mono text-sky-400">%0.0</div>
                        </div>
                        <div class="border-r border-slate-800 h-6"></div>
                        <div>
                            <div class="text-[9px] text-slate-400 uppercase">İşlem Adedi</div>
                            <div id="stat-trades" class="text-sm font-extrabold font-mono text-white">0</div>
                        </div>
                        <div class="border-r border-slate-800 h-6"></div>
                        <div>
                            <div class="text-[9px] text-slate-400 uppercase">Kullanılan Marjin</div>
                            <div id="stat-used-margin" class="text-xs font-bold font-mono text-amber-400">$0 (%0)</div>
                        </div>
                    </div>
                </div>

                <div class="flex flex-wrap items-center gap-2 bg-slate-900/90 p-2 rounded-xl border border-slate-800 text-xs">
                    <div><label class="text-slate-400 block text-[9px]">KASA ($)</label><input id="input-balance" type="number" value="1000" class="bg-slate-800 text-white font-bold w-16 px-1 py-0.5 rounded"></div>
                    <div><label class="text-slate-400 block text-[9px]">MOD</label><select id="input-margin-mode" class="bg-slate-800 text-cyan-400 font-bold px-1 py-0.5 rounded"><option value="ISOLATED" selected>İzole</option><option value="CROSS">Cross</option></select></div>
                    <div><label class="text-slate-400 block text-[9px]">RİSK (%)</label><select id="input-risk" class="bg-slate-800 text-white font-bold px-1 py-0.5 rounded"><option value="5.0" selected>%5.0</option></select></div>
                    <div><label class="text-slate-400 block text-[9px]">KALDIRAÇ</label><select id="input-leverage" class="bg-slate-800 text-emerald-400 font-bold px-1 py-0.5 rounded"><option value="50" selected>50x</option></select></div>
                    <div><label class="text-slate-400 block text-[9px]">MAX POZ</label><select id="input-max-pos" class="bg-slate-800 text-amber-400 font-bold px-1 py-0.5 rounded"><option value="5" selected>5 Adet</option></select></div>
                    <div><label class="text-slate-400 block text-[9px]">MAX MARJİN</label><select id="input-max-margin-pct" class="bg-slate-800 text-fuchsia-400 font-bold px-1 py-0.5 rounded"><option value="50" selected>%50</option></select></div>
                    <button onclick="saveSettings()" class="mt-3 bg-emerald-600 hover:bg-emerald-500 text-black font-bold px-2 py-1 rounded text-xs">KAYDET</button>
                </div>
            </div>

            <div class="grid grid-cols-1 lg:grid-cols-3 gap-3">
                <div class="card p-3 rounded-xl lg:col-span-2 h-[520px] flex flex-col">
                    <div class="flex justify-between items-center mb-2 px-1">
                        <span id="chart-title" class="text-xs font-bold text-emerald-400">BTC/USDT:USDT (5M)</span>
                    </div>
                    <div id="tv-wrapper" class="w-full flex-1 rounded overflow-hidden">
                        <div id="tv-container" class="w-full h-full"></div>
                    </div>
                </div>
                <div class="card p-3 rounded-xl flex flex-col justify-between h-[520px]">
                    <div>
                        <h2 class="text-xs font-semibold text-slate-400 mb-2 uppercase">Giriş Gerekçesi</h2>
                        <div id="active-rationale" class="text-xs text-slate-500 italic">Tablodan bir parite seçin...</div>
                    </div>
                    <div>
                        <h3 class="text-[10px] font-semibold text-slate-500 mb-1 uppercase">Loglar</h3>
                        <div id="log-box" class="bg-black/50 p-2 rounded text-[11px] text-emerald-400 font-mono h-28 overflow-y-auto"></div>
                    </div>
                </div>
            </div>

            <div class="card p-3 rounded-xl">
                <h2 class="text-xs font-semibold text-emerald-400 mb-2">AKTİF POZİSYONLAR</h2>
                <table class="w-full text-left text-[11px]">
                    <thead class="text-slate-500 border-b border-slate-800">
                        <tr><th>PARİTE</th><th>YÖN</th><th>TEMİNAT</th><th>GİRİŞ</th><th>ANLIK PnL</th></tr>
                    </thead>
                    <tbody id="active-pos-table" class="divide-y divide-slate-800/50"></tbody>
                </table>
            </div>
        </div>

        <!-- DİĞER SAYFALAR (SEKMELER) -->
        <div id="page-sentiment" class="hidden card p-4 rounded-xl text-center text-slate-400">🧠 Duyarlılık & Endeksler Modülü Aktif</div>
        <div id="page-news" class="hidden card p-4 rounded-xl text-center text-slate-400">📰 Haber & Takvim Modülü Aktif</div>
        <div id="page-manual" class="hidden card p-4 rounded-xl text-center text-slate-400">🎮 Manuel Kontrol Paneli Aktif</div>
        <div id="page-excel" class="hidden card p-4 rounded-xl text-center text-slate-400">📑 Excel Arşivi Paneli Aktif</div>
        <div id="page-stats" class="hidden card p-4 rounded-xl text-center text-slate-400">📈 İstatistik Paneli Aktif</div>
        <div id="page-radar" class="hidden card p-4 rounded-xl text-center text-slate-400">🔥 Radar Paneli Aktif</div>
        <div id="page-journal" class="hidden card p-4 rounded-xl text-center text-slate-400">📖 İşlem Günlüğü Aktif</div>
        <div id="page-api" class="hidden card p-4 rounded-xl text-center text-slate-400">⚙️ Borsa API Ayarları Aktif</div>

        <script>
            let chart = null;
            let candleSeries = null;
            let currentSymbol = "BTC/USDT:USDT";

            function initCharts() {
                const container = document.getElementById('tv-container');
                if(!container) return;
                container.innerHTML = '';
                chart = LightweightCharts.createChart(container, {
                    layout: { background: { color: '#121824' }, textColor: '#94a3b8' },
                    grid: { vertLines: { color: '#1e293b' }, horzLines: { color: '#1e293b' } }
                });
                candleSeries = chart.addCandlestickSeries({
                    upColor: '#10b981', downColor: '#ef4444',
                    borderUpColor: '#10b981', borderDownColor: '#ef4444',
                    wickUpColor: '#10b981', wickDownColor: '#ef4444'
                });
            }

            async function fetchDefaultChart() {
                try {
                    const res = await fetch('https://api.bybit.com/v5/market/kline?category=linear&symbol=BTCUSDT&interval=5&limit=200');
                    const json = await res.json();
                    if (json.result && json.result.list && candleSeries) {
                        const formatted = json.result.list.map(c => ({
                            time: Math.floor(parseInt(c[0]) / 1000), open: parseFloat(c[1]), high: parseFloat(c[2]), low: parseFloat(c[3]), close: parseFloat(c[4])
                        })).sort((a, b) => a.time - b.time);
                        candleSeries.setData(formatted);
                    }
                } catch(e) {}
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
            }

            async function toggleBotTrading() {
                try {
                    const res = await fetch('/api/toggle_bot_trading', { method: 'POST' });
                    const data = await res.json();
                    const btn = document.getElementById('bot-toggle-btn');
                    if (data.active) {
                        btn.className = "px-2.5 py-1 rounded-lg font-bold bg-emerald-600 text-black transition";
                        btn.innerText = "🤖 Bot: AÇIK";
                    } else {
                        btn.className = "px-2.5 py-1 rounded-lg font-bold bg-rose-600 text-white transition";
                        btn.innerText = "🤖 Bot: KAPALI";
                    }
                } catch(e) {}
            }

            async function saveSettings() {
                const total_balance = parseFloat(document.getElementById('input-balance').value);
                await fetch('/api/update_settings', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({total_balance, risk_pct: 5.0, leverage: 50, margin_mode: 'ISOLATED', max_open_positions: 5, max_total_margin_pct: 50.0})
                });
                alert("Ayarlar Kaydedildi!");
            }

            async function updateDashboard() {
                try {
                    const res = await fetch('/api/state');
                    const data = await res.json();
                    document.getElementById('scanned-count').innerText = data.scanned_count;
                    document.getElementById('btc-regime-badge').innerText = data.btc_regime;
                    document.getElementById('stat-used-margin').innerText = `$${data.locked_margin} (%${((data.locked_margin/data.total_balance)*100).toFixed(1)})`;
                    document.getElementById('log-box').innerHTML = data.logs.map(l => `<div>${l}</div>`).join('');
                } catch(e) {}
            }

            initCharts();
            fetchDefaultChart();
            setInterval(updateDashboard, 2000);
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
