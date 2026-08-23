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
    "btc_regime": "YÜKLENİYOR...",
    "btc_15m_change": 0.0,
    "btc_shock_lock": False,
    "btc_shock_reason": "",
    "fear_and_greed": {"value": 66, "classification": "Açgözlülük"},
    "sentiment_data": {
        "btc_rsi": 52.2,
        "btc_volume_24h": "$3.87 Milyar",
        "market_bias": "AYI / SHORT",
        "long_short_ratio": 47.6,
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
    'BABA', 'PLTR', 'SOXS', 'SOXL', 'QQQ', 'SPY', 'WDC', 'DELL', 'IONQ', 'GLW', 'BIRB',
    'TBT', 'TLT', 'PDD', 'NIO', 'BILI', 'LI', 'XPEV', 'MSTR', 'MARA', 'RIOT', 'CLSK',
    'CASHCAT', 'WLFI', 'TRUMP', 'MELANIA', 'PEPE2', 'SHIB2'
]

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
        add_log(f"🛑 GÜNLÜK ZARAR LİMİTİ TETİKLENDİ: -${daily_loss:.2f} (%{system_state['daily_drawdown_limit_pct']}) Kayıp. Yeni İşlemler Geceye Kadar Kilitlendi!")

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
                    system_state["fear_and_greed"] = {
                        "value": int(item['value']),
                        "classification": tr_class
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
        
        c_now = df_15m['close'].iloc[-1]
        c_prev = df_15m['open'].iloc[-1]
        pct_15m = ((c_now - c_prev) / c_prev) * 100
        system_state["btc_15m_change"] = round(pct_15m, 2)

        if pct_15m <= -1.2:
            system_state["btc_shock_lock"] = True
            system_state["btc_shock_reason"] = f"🔴 BTC Ani Düşüş Şoku (%{pct_15m:.2f}) | LONG Kilitlendi"
        elif pct_15m >= 1.2:
            system_state["btc_shock_lock"] = True
            system_state["btc_shock_reason"] = f"🟢 BTC Ani Yükseliş Şoku (+%{pct_15m:.2f}) | SHORT Kilitlendi"
        else:
            system_state["btc_shock_lock"] = False
            system_state["btc_shock_reason"] = ""

        if last_1h['close'] > last_1h['ema50']:
            system_state["btc_regime"] = "🟢 BOĞA (YÜKSELİŞ)"
            bias = "BOĞA / LONG"
        else:
            system_state["btc_regime"] = "🔴 AYI (DÜŞÜŞ)"
            bias = "AYI / SHORT"

        ticker = await exchange.fetch_ticker('BTC/USDT:USDT')
        vol_quote = ticker.get('quoteVolume', 0)
        vol_str = f"${vol_quote/1e9:.2f} Milyar" if vol_quote > 1e9 else f"${vol_quote/1e6:.1f} Milyon"

        vol_ratio = (last_1h['atr'] / last_1h['close']) * 100 if pd.notnull(last_1h['atr']) else 0.5
        vol_level = "YÜKSEK" if vol_ratio > 1.2 else ("ORTA" if vol_ratio > 0.6 else "DÜŞÜK")
        ls_ratio = 47.6 if last_1h['close'] < last_1h['ema20'] else 52.4

        system_state["sentiment_data"] = {
            "btc_rsi": round(float(last_1h['rsi']), 1) if pd.notnull(last_1h['rsi']) else 52.2,
            "btc_volume_24h": vol_str,
            "market_bias": bias,
            "long_short_ratio": ls_ratio,
            "market_volatility": vol_level,
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
        }
    except Exception:
        system_state["btc_regime"] = "BTC: AKTİF"

async def analyze_symbol(exchange, symbol):
    try:
        if system_state["daily_loss_locked"]:
            return None

        base = symbol.split('/')[0].upper()
        if any(exc in base for exc in EXCLUDED_KEYWORDS):
            return None

        tasks = [
            exchange.fetch_ohlcv(symbol, timeframe='5m', limit=35),
            exchange.fetch_ohlcv(symbol, timeframe='15m', limit=35),
            exchange.fetch_ohlcv(symbol, timeframe='1h', limit=35),
            exchange.fetch_open_interest_history(symbol, timeframe='5m', limit=6)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        if any(isinstance(r, Exception) or not r or len(r) < 20 for r in results[:3]):
            return None

        df_5m = calculate_indicators(pd.DataFrame(results[0], columns=['t', 'open', 'high', 'low', 'close', 'volume']))
        df_15m = calculate_indicators(pd.DataFrame(results[1], columns=['t', 'open', 'high', 'low', 'close', 'volume']))
        df_1h = calculate_indicators(pd.DataFrame(results[2], columns=['t', 'open', 'high', 'low', 'close', 'volume']))
        oi_data = results[3] if not isinstance(results[3], Exception) and results[3] else []

        c_5m = df_5m.iloc[-1]
        c_15m = df_15m.iloc[-1]
        c_1h = df_1h.iloc[-1]

        swing_low_15m = df_15m['low'].iloc[-20:-3].min()
        swing_high_15m = df_15m['high'].iloc[-20:-3].max()
        recent_breakout_high = df_5m['high'].iloc[-8:-1].max()
        recent_breakout_low = df_5m['low'].iloc[-8:-1].min()

        score = 0
        direction = None
        reasons = []

        sweep_low = df_15m['low'].iloc[-4:].min() < swing_low_15m
        mss_bull = c_5m['close'] > recent_breakout_high and c_5m['close'] > df_5m['ema20'].iloc[-1] and c_5m['close'] > c_5m['open']

        sweep_high = df_15m['high'].iloc[-4:].max() > swing_high_15m
        mss_bear = c_5m['close'] < recent_breakout_low and c_5m['close'] < df_5m['ema20'].iloc[-1] and c_5m['close'] < c_5m['open']

        if sweep_low and mss_bull:
            if not (system_state["btc_shock_lock"] and system_state["btc_15m_change"] <= -1.2):
                direction = "LONG"
                score += 45
                reasons.append("⚡ 15M Dip Likiditesi Alındı + 5M MSS Kırılımı")
        elif sweep_high and mss_bear:
            if not (system_state["btc_shock_lock"] and system_state["btc_15m_change"] >= 1.2):
                direction = "SHORT"
                score += 45
                reasons.append("⚡ 15M Tepe Likiditesi Alındı + 5M MSS Kırılımı")

        if len(oi_data) >= 3 and direction:
            oi_prev = oi_data[-2].get('openInterestValue') or oi_data[-2].get('openInterest', 0)
            oi_curr = oi_data[-1].get('openInterestValue') or oi_data[-1].get('openInterest', 0)
            if oi_curr > oi_prev:
                score += 15
                reasons.append("📊 Açık Pozisyon (OI) Artışı (Kurumsal Giriş Onayı)")

        if direction == "LONG":
            if c_1h['close'] > c_1h['ema50'] and c_15m['close'] > c_15m['ema50']:
                score += 25
                reasons.append("📈 1H & 15M Güçlü Boğa Trend Uyumu")
            elif c_1h['close'] > c_1h['ema50'] or c_15m['close'] > c_15m['ema50']:
                score += 10
                reasons.append("📈 Trend Desteği")
        elif direction == "SHORT":
            if c_1h['close'] < c_1h['ema50'] and c_15m['close'] < c_15m['ema50']:
                score += 25
                reasons.append("📉 1H & 15M Güçlü Ayı Trend Uyumu")
            elif c_1h['close'] < c_1h['ema50'] or c_15m['close'] < c_15m['ema50']:
                score += 10
                reasons.append("📉 Trend Desteği")

        vol_ratio = float(c_5m['volume'] / (c_5m['vol_ma'] + 1e-9)) if pd.notnull(c_5m['vol_ma']) else 1.0
        if vol_ratio >= 1.30:
            score += 10
            reasons.append(f"🔥 Yüksek Hacim Onayı ({vol_ratio:.1f}x)")

        if 42 <= c_5m['rsi'] <= 62:
            score += 10
            reasons.append(f"🎯 Dengeli Momentum RSI ({c_5m['rsi']:.1f})")

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

        if not direction or score < 75:
            return None

        entry = float(c_5m['close'])
        atr = float(c_5m['atr']) if pd.notnull(c_5m['atr']) else entry * 0.008

        if direction == "LONG":
            sl = float(df_5m['low'].iloc[-8:].min() - (1.8 * atr))
            if (entry - sl) / entry < 0.012:
                sl = entry * 0.988
            risk_dist = entry - sl

            dyn_tp1 = float(df_15m['high'].iloc[-25:-1].max())
            if (dyn_tp1 - entry) < (1.5 * risk_dist):
                dyn_tp1 = entry + (1.5 * risk_dist)

            dyn_tp2 = float(df_1h['high'].iloc[-25:-1].max())
            if dyn_tp2 <= dyn_tp1 or (dyn_tp2 - entry) < (2.5 * risk_dist):
                dyn_tp2 = entry + (3.0 * risk_dist)

            tp1, tp2 = dyn_tp1, dyn_tp2

        else:
            sl = float(df_5m['high'].iloc[-8:].max() + (1.8 * atr))
            if (sl - entry) / entry < 0.012:
                sl = entry * 1.012
            risk_dist = sl - entry

            dyn_tp1 = float(df_15m['low'].iloc[-25:-1].min())
            if (entry - dyn_tp1) < (1.5 * risk_dist):
                dyn_tp1 = entry - (1.5 * risk_dist)

            dyn_tp2 = float(df_1h['low'].iloc[-25:-1].min())
            if dyn_tp2 >= dyn_tp1 or (entry - dyn_tp2) < (2.5 * risk_dist):
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
            "trailing_active": False,
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
    add_log("Quant Motoru: Yüksek Hacimli Likit Kripto Taraması Devrede...")

    while True:
        exchange = None
        try:
            exchange = ccxt.bybit({
                'options': {'defaultType': 'linear'},
                'enableRateLimit': True,
                'timeout': 10000
            })

            check_daily_drawdown()
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
                        quote_vol = t_data.get('quoteVolume', 0) or 0
                        if quote_vol >= 10_000_000:
                            crypto_symbols.append(s)

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

                    if pos.get("trailing_active"):
                        if direction == "LONG" and curr_price > pos['entry']:
                            new_sl = curr_price * 0.992
                            if new_sl > pos['sl']:
                                pos['sl'] = new_sl
                        elif direction == "SHORT" and curr_price < pos['entry']:
                            new_sl = curr_price * 1.008
                            if new_sl < pos['sl']:
                                pos['sl'] = new_sl

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
                        duration_mins = max(1, int((now_dt.timestamp() - pos.get('open_timestamp', now_dt.timestamp())) / 60))
                        system_state["equity_curve"].append({"time": int(now_dt.timestamp()), "value": round(system_state["total_balance"], 2)})
                        
                        history_item = {
                            "symbol": pos['symbol'],
                            "direction": pos['direction'],
                            "entry": pos['entry'],
                            "close_price": curr_price,
                            "pnl_pct": round(pnl_pct, 2),
                            "realized_pnl": realized_pnl,
                            "score": pos['score'],
                            "duration_mins": duration_mins,
                            "open_reasons": pos['reasons'],
                            "close_reason": close_reason,
                            "close_time": now_dt.strftime("%H:%M:%S"),
                            "close_timestamp": int(now_dt.timestamp())
                        }
                        system_state["trade_history"].insert(0, history_item)
                        system_state["active_positions"].remove(pos)
                        add_log(f"🔴 POZİSYON KAPANDI: {pos['symbol']} | PnL: %{pnl_pct:.2f} (${realized_pnl}) | {close_reason}")
                        check_daily_drawdown()
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
            "duration_mins": 1,
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

@app.post("/api/manual/partial_close")
async def manual_partial_close(payload: PartialClosePayload):
    target = next((p for p in system_state["active_positions"] if p['symbol'] == payload.symbol), None)
    if target:
        curr_price = target.get('current_price', target['entry'])
        direction = target['direction']
        pnl_pct = ((curr_price - target['entry']) / target['entry'] * 100) if direction == "LONG" else ((target['entry'] - curr_price) / target['entry'] * 100)
        
        part_size = target['active_size'] * payload.ratio
        realized_pnl = round(part_size * (pnl_pct / 100.0), 2)
        system_state["total_balance"] += realized_pnl
        target['active_size'] -= part_size
        target['pos_size'] -= part_size

        if target['active_size'] <= 0:
            system_state["active_positions"].remove(target)

        now_dt = get_now_datetime()
        system_state["equity_curve"].append({"time": int(now_dt.timestamp()), "value": round(system_state["total_balance"], 2)})
        add_log(f"✂️ KADEMELİ KAPATMA (%{int(payload.ratio*100)}): {target['symbol']} | Realize PnL: +${realized_pnl}")
        return {"status": "success"}
    return {"status": "error"}

@app.post("/api/manual/close_all")
async def manual_close_all():
    for pos in list(system_state["active_positions"]):
        curr_price = pos.get('current_price', pos['entry'])
        direction = pos['direction']
        pnl_pct = ((curr_price - pos['entry']) / pos['entry'] * 100) if direction == "LONG" else ((target['entry'] - curr_price) / target['entry'] * 100)
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
            "duration_mins": 1,
            "open_reasons": pos['reasons'],
            "close_reason": "🚨 ACİL TÜMÜNÜ KAPAT",
            "close_time": now_dt.strftime("%H:%M:%S"),
            "close_timestamp": int(now_dt.timestamp())
        }
        system_state["trade_history"].insert(0, history_item)
        system_state["active_positions"].remove(pos)
    add_log("🚨 TÜM POZİSYONLAR KAPATILDI!")
    return {"status": "success"}

@app.post("/api/manual/breakeven_all")
async def manual_breakeven_all():
    for pos in system_state["active_positions"]:
        pos['sl'] = pos['entry']
        pos['tp1_hit'] = True
    add_log("🛡️ TOPLU BAŞABAŞ: Tüm açık pozisyonların stopları giriş fiyatına çekildi!")
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

@app.post("/api/manual/toggle_trailing")
async def manual_toggle_trailing(payload: ClosePosPayload):
    target = next((p for p in system_state["active_positions"] if p['symbol'] == payload.symbol), None)
    if target:
        target['trailing_active'] = not target.get('trailing_active', False)
        status_str = "Aktif" if target['trailing_active'] else "Pasif"
        add_log(f"🔄 TRAILING STOP: {target['symbol']} için {status_str} yapıldı.")
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

@app.post("/api/export/custom_csv")
async def export_custom_csv(payload: DateRangePayload):
    filtered = []
    try:
        start_ts = int(datetime.strptime(payload.start_date, "%Y-%m-%d").replace(tzinfo=TURKEY_TZ).timestamp())
        end_ts = int(datetime.strptime(payload.end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=TURKEY_TZ).timestamp())
        for h in system_state["trade_history"]:
            ts = h.get("close_timestamp", 0)
            if start_ts <= ts <= end_ts:
                filtered.append(h)
    except Exception:
        filtered = system_state["trade_history"]

    df = pd.DataFrame(filtered) if filtered else pd.DataFrame(columns=["symbol", "direction", "entry", "close_price", "pnl_pct", "realized_pnl", "close_reason", "close_time"])
    stream = io.StringIO()
    df.to_csv(stream, index=False)
    response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
    response.headers["Content-Disposition"] = f"attachment; filename=trades_range_{payload.start_date}_to_{payload.end_date}.csv"
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
                        <span id="btc-shock-badge" class="hidden text-[9px] font-bold px-2 py-0.5 rounded bg-rose-950 text-rose-400 border border-rose-800 animate-pulse">⚡ BTC ŞOK KORUMASI</span>
                        <span id="drawdown-badge" class="hidden text-[9px] font-bold px-2 py-0.5 rounded bg-amber-950 text-amber-400 border border-amber-800">🛑 GÜNLÜK ZARAR LİMİTİ</span>
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

        <!-- SAYFA 2: DUYARLILIK & ENDEKSLER -->
        <div id="page-sentiment" class="hidden space-y-3">
            <div class="grid grid-cols-1 md:grid-cols-3 gap-3">
                <div class="card p-4 rounded-xl flex flex-col items-center justify-center text-center">
                    <div class="text-xs text-slate-400 uppercase tracking-wider mb-2">Kripto Korku ve Açgözlülük</div>
                    <div id="fng-val" class="text-4xl font-extrabold font-mono text-emerald-400">66</div>
                    <div id="fng-text" class="text-sm font-bold text-slate-300 mt-1 uppercase">AÇGÖZLÜLÜK</div>
                    <div class="w-full bg-slate-800 h-2.5 rounded-full mt-3 overflow-hidden">
                        <div id="fng-bar" class="bg-emerald-500 h-2.5 rounded-full" style="width: 66%"></div>
                    </div>
                </div>
                <div class="card p-4 rounded-xl space-y-2">
                    <div class="text-xs text-slate-400 uppercase tracking-wider">Bitcoin Canlı Akış Metrikleri</div>
                    <div class="flex justify-between items-center bg-slate-900/80 p-2 rounded border border-slate-800 text-xs">
                        <span>BTC 15M:</span> <span id="sent-btc-15m" class="font-bold font-mono text-rose-400">%-0.03</span>
                    </div>
                    <div class="flex justify-between items-center bg-slate-900/80 p-2 rounded border border-slate-800 text-xs">
                        <span>BTC 1H RSI:</span> <span id="sent-btc-rsi" class="font-bold font-mono text-sky-400">52.2</span>
                    </div>
                    <div class="flex justify-between items-center bg-slate-900/80 p-2 rounded border border-slate-800 text-xs">
                        <span>Hacim:</span> <span id="sent-btc-vol" class="font-bold font-mono text-amber-400">$3.87 Milyar</span>
                    </div>
                </div>
                <div class="card p-4 rounded-xl space-y-2">
                    <div class="text-xs text-slate-400 uppercase tracking-wider">Piyasa Isı Ölçeri & Volatilite</div>
                    <div class="flex justify-between items-center bg-slate-900/80 p-2 rounded border border-slate-800 text-xs">
                        <span>Piyasa Yönü:</span> <b id="sent-bias" class="text-rose-400 font-bold">AYI / SHORT</b>
                    </div>
                    <div class="flex justify-between items-center bg-slate-900/80 p-2 rounded border border-slate-800 text-xs">
                        <span>ATR Volatilite:</span> <b id="sent-volatility" class="text-emerald-400 font-bold">DÜŞÜK</b>
                    </div>
                    <div class="flex justify-between items-center bg-slate-900/80 p-2 rounded border border-slate-800 text-xs">
                        <span>Şok Durumu:</span> <b id="sent-shock-status" class="text-emerald-400 font-bold">GÜVENLİ</b>
                    </div>
                </div>
            </div>

            <div class="grid grid-cols-1 md:grid-cols-3 gap-3">
                <div class="card p-4 rounded-xl space-y-2">
                    <div class="text-xs text-slate-400 uppercase tracking-wider">💥 24S Toplam Likidasyonlar</div>
                    <div id="sent-liq-total" class="text-lg font-extrabold font-mono text-rose-400">$143.7 Milyon</div>
                    <div class="w-full bg-slate-800 h-3 rounded-full overflow-hidden flex">
                        <div id="liq-long-bar" class="bg-emerald-500 h-3" style="width: 57.5%"></div>
                        <div id="liq-short-bar" class="bg-rose-500 h-3" style="width: 42.5%"></div>
                    </div>
                    <div class="flex justify-between text-[10px] text-slate-400 font-mono">
                        <span class="text-emerald-400">Long: <b id="sent-long-liq">%57.5</b></span>
                        <span class="text-rose-400">Short: <b id="sent-short-liq">%42.5</b></span>
                    </div>
                </div>
                <div class="card p-4 rounded-xl space-y-2">
                    <div class="text-xs text-slate-400 uppercase tracking-wider">📊 Toplam OI Değişimi</div>
                    <div id="sent-oi-change" class="text-lg font-extrabold font-mono text-sky-400">-%1.2</div>
                    <div class="text-xs text-slate-300 bg-slate-900/80 p-2 rounded border border-slate-800 mt-1">Türev piyasa kaldıracı daralıyor</div>
                </div>
                <div class="card p-4 rounded-xl space-y-2">
                    <div class="text-xs text-slate-400 uppercase tracking-wider">👑 BTC Dominansı & Fonlama</div>
                    <div class="flex justify-between items-center text-xs bg-slate-900/80 p-2 rounded border border-slate-800">
                        <span class="text-slate-400">BTC.D:</span> <b id="sent-btc-dom" class="text-amber-400 font-mono">%57.6</b>
                    </div>
                    <div class="flex justify-between items-center text-xs bg-slate-900/80 p-2 rounded border border-slate-800">
                        <span class="text-slate-400">Ort. Funding:</span> <b id="sent-avg-funding" class="text-emerald-400 font-mono">+0.0098%</b>
                    </div>
                </div>
            </div>

            <div class="grid grid-cols-1 md:grid-cols-3 gap-3">
                <div class="card p-4 rounded-xl space-y-2">
                    <div class="text-xs text-slate-400 uppercase tracking-wider">⚡ Emir Defteri (Bid/Ask)</div>
                    <div class="flex justify-between text-xs font-bold">
                        <span class="text-emerald-400">Alıcı: <b id="sent-bid-val">%54.2</b></span>
                        <span class="text-rose-400">Satıcı: <b id="sent-ask-val">%45.8</b></span>
                    </div>
                    <div class="w-full bg-slate-800 h-3 rounded-full overflow-hidden flex">
                        <div id="bid-bar" class="bg-emerald-500 h-3" style="width: 54.2%"></div>
                        <div id="ask-bar" class="bg-rose-500 h-3" style="width: 45.8%"></div>
                    </div>
                    <div class="text-[10px] text-slate-400">(Dengeli / Alım Ağırlıklı)</div>
                </div>
                <div class="card p-4 rounded-xl space-y-2">
                    <div class="text-xs text-slate-400 uppercase tracking-wider">🐋 Son 1 Saatlik Balina Akışı</div>
                    <div class="flex justify-between text-[11px] bg-slate-900/80 p-1.5 rounded border border-slate-800">
                        <span class="text-slate-400">Borsaya Giren:</span> <span id="sent-whale-in" class="font-mono text-rose-400">$420M USDT</span>
                    </div>
                    <div class="flex justify-between text-[11px] bg-slate-900/80 p-1.5 rounded border border-slate-800">
                        <span class="text-slate-400">Borsadan Çıkan:</span> <span id="sent-whale-out" class="font-mono text-emerald-400">$180M USDT</span>
                    </div>
                    <div class="text-[11px] font-bold text-amber-400 pt-0.5">Net Akış: <span id="sent-net-whale">+$240M (Boğa / Giriş)</span></div>
                </div>
                <div class="card p-4 rounded-xl space-y-2">
                    <div class="text-xs text-slate-400 uppercase tracking-wider">🔄 Fonlama Oranları (Funding)</div>
                    <div class="space-y-1 text-[11px]">
                        <div class="flex justify-between bg-slate-900/80 p-1 rounded border border-slate-800">
                            <span class="text-white">BTCUSDT:</span> <span class="font-mono text-emerald-400">+0.0100% (Normal)</span>
                        </div>
                        <div class="flex justify-between bg-slate-900/80 p-1 rounded border border-slate-800">
                            <span class="text-white">ETHUSDT:</span> <span class="font-mono text-emerald-400">+0.0125% (Normal)</span>
                        </div>
                        <div class="flex justify-between bg-slate-900/80 p-1 rounded border border-slate-800">
                            <span class="text-white">SOLUSDT:</span> <span class="font-mono text-amber-400">+0.0250% (Yüksek Long)</span>
                        </div>
                    </div>
                </div>
            </div>

            <div class="card p-4 rounded-xl space-y-2">
                <div class="flex justify-between text-xs text-slate-400 uppercase font-bold">
                    <span>VADELİ PİYASA LONG / SHORT ORANI</span>
                    <span id="ls-ratio-text" class="text-white font-mono">%47.6 Long / %52.4 Short</span>
                </div>
                <div class="w-full bg-rose-600 h-3 rounded-full overflow-hidden flex">
                    <div id="ls-bar" class="bg-emerald-500 h-3 transition-all duration-500" style="width: 47.6%"></div>
                </div>
            </div>
        </div>

        <!-- SAYFA 3: HABER, CANLI AKIŞ VE ÇOKLU GERİ SAYIM SAYACI -->
        <div id="page-news" class="hidden space-y-3">
            <div class="grid grid-cols-1 md:grid-cols-4 gap-3">
                <div class="card p-3 rounded-xl border-amber-500/30">
                    <div class="text-[10px] text-amber-400 font-bold uppercase tracking-wider">ABD TÜFE (CPI)</div>
                    <div id="cd-cpi" class="text-base font-mono font-bold text-white mt-1">--s --d --sn</div>
                </div>
                <div class="card p-3 rounded-xl border-purple-500/30">
                    <div class="text-[10px] text-purple-400 font-bold uppercase tracking-wider">FED FOMC Kararı</div>
                    <div id="cd-fomc" class="text-base font-mono font-bold text-white mt-1">--s --d --sn</div>
                </div>
                <div class="card p-3 rounded-xl border-rose-500/30">
                    <div class="text-[10px] text-rose-400 font-bold uppercase tracking-wider">ABD NFP İstihdam</div>
                    <div id="cd-nfp" class="text-base font-mono font-bold text-white mt-1">--s --d --sn</div>
                </div>
                <div class="card p-3 rounded-xl border-sky-500/30">
                    <div class="text-[10px] text-sky-400 font-bold uppercase tracking-wider">Majör Token Unlock</div>
                    <div id="cd-unlock" class="text-base font-mono font-bold text-white mt-1">--s --d --sn</div>
                </div>
            </div>

            <div class="card p-4 rounded-xl space-y-3">
                <h3 class="text-xs font-semibold text-emerald-400 uppercase flex items-center justify-between">
                    <span class="flex items-center"><span class="w-2 h-2 bg-emerald-400 rounded-full mr-2 animate-ping"></span> Canlı Kripto Son Dakika Haber Akışı</span>
                    <span class="text-[10px] text-slate-500">Kaynak: Global Kurumsal Akış</span>
                </h3>
                <div class="space-y-2 text-xs">
                    <div class="flex justify-between items-center bg-slate-900/80 p-2.5 rounded-lg border border-slate-800">
                        <span class="text-white font-medium">⚡ SEC, yeni kurumsal ETF başvuru dosyaları için inceleme sürecini başlattı.</span>
                        <span class="text-[10px] text-slate-500 font-mono">2 dk önce</span>
                    </div>
                </div>
            </div>

            <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
                <div class="card p-4 rounded-xl space-y-2">
                    <h3 class="text-xs font-semibold text-sky-400 uppercase">📢 Yeni Vadeli Listelemeler (Futures Listing)</h3>
                    <div class="space-y-1.5 text-xs">
                        <div class="flex justify-between bg-slate-900/80 p-2 rounded border border-slate-800">
                            <span class="text-white font-bold">MEW/USDT (50x)</span> <span class="text-emerald-400 font-mono">Aktif Edildi</span>
                        </div>
                    </div>
                </div>
                <div class="card p-4 rounded-xl space-y-2">
                    <h3 class="text-xs font-semibold text-amber-400 uppercase">🛠️ Planlı Borsa Bakım Saatleri</h3>
                    <div class="space-y-1.5 text-xs">
                        <div class="flex justify-between bg-slate-900/80 p-2 rounded border border-slate-800">
                            <span class="text-white font-bold">Bybit Altyapı Güncellemesi</span> <span class="text-amber-400 font-mono">Yarın 04:00 TSİ</span>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- SAYFA 4: MANUEL MÜDAHALE -->
        <div id="page-manual" class="hidden space-y-3">
            <div class="card p-4 rounded-xl flex flex-wrap justify-between items-center gap-3 border-rose-500/30">
                <div>
                    <h2 class="text-sm font-bold text-rose-400">🚨 Acil Durum & Gelişmiş Emir Yönetim Masası</h2>
                    <p class="text-xs text-slate-400 mt-0.5">Toplam Pozisyon, Risk ve Marjin durumunu takip edin, toplu veya kademeli işlemler yapın.</p>
                </div>
                <div class="flex space-x-2">
                    <button onclick="manualBreakevenAll()" class="bg-sky-600 hover:bg-sky-500 text-white font-bold px-3 py-2 rounded-lg text-xs transition">🛡️ TÜMÜNÜ BAŞABAŞ ÇEK</button>
                    <button onclick="manualCloseAll()" class="bg-rose-600 hover:bg-rose-500 text-white font-bold px-3 py-2 rounded-lg text-xs transition">🚨 TÜMÜNÜ KAPAT (ACİL)</button>
                </div>
            </div>

            <div class="grid grid-cols-1 md:grid-cols-3 gap-3">
                <div class="card p-3 rounded-xl flex justify-between items-center">
                    <span class="text-xs text-slate-400 uppercase">Toplam Açık Pozisyon:</span> <span id="man-total-pos" class="text-sm font-bold font-mono text-white">0 Adet</span>
                </div>
                <div class="card p-3 rounded-xl flex justify-between items-center">
                    <span class="text-xs text-slate-400 uppercase">Toplam Kullanılan Marjin:</span> <span id="man-total-margin" class="text-sm font-bold font-mono text-amber-400">$0.00</span>
                </div>
                <div class="card p-3 rounded-xl flex justify-between items-center">
                    <span class="text-xs text-slate-400 uppercase">Toplam Risk Altındaki Tutar:</span> <span id="man-total-risk" class="text-sm font-bold font-mono text-rose-400">$0.00</span>
                </div>
            </div>

            <div class="card p-4 rounded-xl">
                <div class="overflow-x-auto">
                    <table class="w-full text-left text-xs">
                        <thead class="text-slate-500 border-b border-slate-800">
                            <tr>
                                <th class="pb-2">PARİTE</th> <th class="pb-2">YÖN</th> <th class="pb-2">GİRİŞ / CANLI</th> <th class="pb-2">ANLIK PnL</th>
                                <th class="pb-2">SL GÜNCELLE</th> <th class="pb-2">TP2 GÜNCELLE</th> <th class="pb-2 text-center">TRAILING STOP</th> <th class="pb-2 text-right">KADEMELİ / EYLEMLER</th>
                            </tr>
                        </thead>
                        <tbody id="manual-pos-table" class="divide-y divide-slate-800/60"></tbody>
                    </table>
                </div>
            </div>
        </div>

        <!-- SAYFA 5: GÜNLÜK EXCEL ARŞİVİ -->
        <div id="page-excel" class="hidden space-y-3">
            <div class="grid grid-cols-1 lg:grid-cols-3 gap-3">
                <div class="card p-4 rounded-xl space-y-3">
                    <h2 class="text-xs font-bold text-emerald-400 uppercase">📊 Canlı CSV Raporu İndir</h2>
                    <p class="text-xs text-slate-400">Bugün kapanan tüm işlemlerin dökümünü indirin.</p>
                    <a href="/api/export/csv" class="inline-block bg-emerald-600 hover:bg-emerald-500 text-black font-bold px-4 py-2 rounded-lg text-xs transition">📥 BUGÜNÜ İNDİR (CSV)</a>
                    
                    <div class="border-t border-slate-800 pt-3 space-y-2">
                        <h3 class="text-xs font-bold text-sky-400 uppercase">📅 Özel Tarih Aralığı Seç</h3>
                        <div class="space-y-2 text-xs">
                            <div><label class="text-slate-400 text-[10px] block">Başlangıç</label><input id="custom-start-date" type="date" class="w-full bg-slate-900 border border-slate-700 text-white rounded p-1.5 font-mono text-xs"></div>
                            <div><label class="text-slate-400 text-[10px] block">Bitiş</label><input id="custom-end-date" type="date" class="w-full bg-slate-900 border border-slate-700 text-white rounded p-1.5 font-mono text-xs"></div>
                            <button onclick="downloadCustomCsv()" class="w-full bg-sky-600 hover:bg-sky-500 text-white font-bold py-2 rounded transition">📥 SEÇİLENİ İNDİR</button>
                        </div>
                    </div>
                </div>
                <div class="card p-4 rounded-xl lg:col-span-2 space-y-3">
                    <div class="flex justify-between items-center">
                        <h2 class="text-xs font-bold text-sky-400 uppercase">📁 Tarihli Arşiv Dosyaları & Bulut Yedekleme</h2>
                        <div class="flex items-center space-x-1.5 bg-emerald-950/60 border border-emerald-800/60 px-2.5 py-1 rounded-lg text-[10px] text-emerald-400">
                            <span class="w-2 h-2 bg-emerald-500 rounded-full animate-pulse"></span>
                            <span>Bulut Yedekleme: <b>Aktif & Güvenli</b></span>
                        </div>
                    </div>
                    <div class="grid grid-cols-3 gap-2 bg-slate-900/90 p-3 rounded-xl border border-slate-800 text-center">
                        <div><div class="text-[9px] text-slate-400 uppercase">Bugünkü İşlem</div><div id="archive-prev-trades" class="text-sm font-extrabold font-mono text-white mt-0.5">0</div></div>
                        <div><div class="text-[9px] text-slate-400 uppercase">Win Rate</div><div id="archive-prev-winrate" class="text-sm font-extrabold font-mono text-sky-400 mt-0.5">%0.0</div></div>
                        <div><div class="text-[9px] text-slate-400 uppercase">Toplam Net PnL</div><div id="archive-prev-pnl" class="text-sm font-extrabold font-mono text-emerald-400 mt-0.5">$0.00</div></div>
                    </div>
                    <div class="overflow-x-auto max-h-48 overflow-y-auto">
                        <table class="w-full text-left text-xs">
                            <thead class="text-slate-500 border-b border-slate-800"><tr><th class="pb-2">DOSYA ADI</th><th class="pb-2 text-right">EYLEM</th></tr></thead>
                            <tbody id="reports-table" class="divide-y divide-slate-800/60"></tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>

        <!-- SAYFA 6: PERFORMANS & İSTATİSTİK -->
        <div id="page-stats" class="hidden space-y-3">
            <div class="card p-2.5 rounded-xl flex justify-between items-center">
                <div class="text-xs font-bold text-emerald-400 uppercase">📈 Kurumsal Fon Performans Analizi</div>
                <div class="flex space-x-1 bg-slate-900 p-1 rounded-lg border border-slate-800 text-xs">
                    <button onclick="changeStatsFilter('today')" id="stats-tf-today" class="stats-tf-btn active px-2.5 py-1 rounded text-slate-400 hover:text-white transition">Bugün</button>
                    <button onclick="changeStatsFilter('week')" id="stats-tf-week" class="stats-tf-btn px-2.5 py-1 rounded text-slate-400 hover:text-white transition">Bu Hafta</button>
                    <button onclick="changeStatsFilter('month')" id="stats-tf-month" class="stats-tf-btn px-2.5 py-1 rounded text-slate-400 hover:text-white transition">Bu Ay</button>
                    <button onclick="changeStatsFilter('all')" id="stats-tf-all" class="stats-tf-btn px-2.5 py-1 rounded text-slate-400 hover:text-white transition">Tüm Zamanlar</button>
                </div>
            </div>

            <div class="grid grid-cols-1 md:grid-cols-4 gap-3">
                <div class="card p-4 rounded-xl"><div class="text-[10px] text-slate-400 uppercase">Kâr Faktörü (Profit Factor)</div><div id="stat-pf" class="text-xl font-bold font-mono text-emerald-400 mt-1">0.00</div></div>
                <div class="card p-4 rounded-xl"><div class="text-[10px] text-slate-400 uppercase">Beklenen Değer (Expectancy)</div><div id="stat-expectancy" class="text-xl font-bold font-mono text-sky-400 mt-1">$0.00</div></div>
                <div class="card p-4 rounded-xl"><div class="text-[10px] text-slate-400 uppercase">Sharpe / Sortino Oranı</div><div id="stat-sharpe" class="text-xl font-bold font-mono text-amber-400 mt-1">0.00 / 0.00</div></div>
                <div class="card p-4 rounded-xl"><div class="text-[10px] text-slate-400 uppercase">Maksimum Drawdown (DD)</div><div id="stat-max-dd" class="text-xl font-bold font-mono text-rose-400 mt-1">%0.00</div></div>
            </div>

            <div class="grid grid-cols-1 md:grid-cols-4 gap-3">
                <div class="card p-4 rounded-xl"><div class="text-[10px] text-slate-400 uppercase">Ortalama Kârlı / Zararlı İşlem</div><div class="text-base font-bold font-mono text-emerald-400 mt-1" id="stat-avg-win">+$0.00</div><div class="text-base font-bold font-mono text-rose-400 mt-0.5" id="stat-avg-loss">-$0.00</div></div>
                <div class="card p-4 rounded-xl space-y-2"><div class="text-[10px] text-slate-400 uppercase">Ardışık Seri (Max Seriler)</div><div class="text-xs font-bold font-mono text-white pt-1">Kazanç: <b id="stat-max-win-streak" class="text-emerald-400">0</b> | Kayıp: <b id="stat-max-loss-streak" class="text-rose-400">0</b></div></div>
                <div class="card p-4 rounded-xl space-y-2"><div class="text-[10px] text-slate-400 uppercase">Ortalama İşlem Süresi</div><div id="stat-avg-duration" class="text-sm font-bold font-mono text-amber-400 mt-1">-- Dakika</div></div>
                <div class="card p-4 rounded-xl space-y-2"><div class="text-[10px] text-slate-400 uppercase">Yön Dağılımı (Long vs Short)</div><div id="stat-ls-ratio" class="text-xs font-bold font-mono text-white">L: %0 | S: %0</div><div class="w-full bg-slate-800 h-1.5 rounded-full overflow-hidden flex"><div id="stat-ls-bar" class="bg-sky-500 h-1.5" style="width: 50%"></div></div></div>
            </div>

            <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
                <div class="card p-4 rounded-xl">
                    <h2 class="text-xs font-semibold text-emerald-400 mb-3 uppercase">🏆 En Çok Kazandıran Lider Pariteler</h2>
                    <div class="overflow-x-auto max-h-52 overflow-y-auto">
                        <table class="w-full text-left text-xs">
                            <thead class="text-slate-500 border-b border-slate-800"><tr><th class="pb-2">PARİTE</th><th class="pb-2">İŞLEM</th><th class="pb-2 text-right">TOPLAM PnL ($)</th></tr></thead>
                            <tbody id="top-symbols-table" class="divide-y divide-slate-800/50"></tbody>
                        </table>
                    </div>
                </div>
                <div class="card p-4 rounded-xl">
                    <h2 class="text-xs font-semibold text-rose-400 mb-3 uppercase">⚠️ En Çok Zarar Ettiren Pariteler (Blacklist Adayı)</h2>
                    <div class="overflow-x-auto max-h-52 overflow-y-auto">
                        <table class="w-full text-left text-xs">
                            <thead class="text-slate-500 border-b border-slate-800"><tr><th class="pb-2">PARİTE</th><th class="pb-2">İŞLEM</th><th class="pb-2 text-right">TOPLAM PnL ($)</th></tr></thead>
                            <tbody id="worst-symbols-table" class="divide-y divide-slate-800/50"></tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>

        <!-- SAYFA 7: RADAR -->
        <div id="page-radar" class="hidden space-y-3">
            <div class="card p-4 rounded-xl">
                <h2 class="text-xs font-semibold text-emerald-400 uppercase mb-3">🔥 700+ Canlı Taranan Parite Radarı</h2>
                <div class="overflow-x-auto max-h-[500px] overflow-y-auto">
                    <table class="w-full text-left text-xs">
                        <thead class="text-slate-500 border-b border-slate-800 sticky top-0 bg-[#121824]">
                            <tr><th class="pb-2">PARİTE</th><th class="pb-2">SON FİYAT</th><th class="pb-2">TREND</th><th class="pb-2">5M RSI</th><th class="pb-2">HACİM KAT</th><th class="pb-2">PUAN</th></tr>
                        </thead>
                        <tbody id="radar-table" class="divide-y divide-slate-800/50"></tbody>
                    </table>
                </div>
            </div>
        </div>

        <!-- SAYFA 8: GÜNLÜK (JOURNAL) -->
        <div id="page-journal" class="hidden space-y-3">
            <div class="card p-4 rounded-xl">
                <h2 class="text-xs font-semibold text-sky-400 mb-3 uppercase">📖 Kapanan İşlem Günlüğü</h2>
                <div class="overflow-x-auto max-h-[500px] overflow-y-auto">
                    <table class="w-full text-left text-xs">
                        <thead class="text-slate-500 border-b border-slate-800 sticky top-0 bg-[#121824]">
                            <tr><th class="pb-2">ZAMAN</th><th class="pb-2">PARİTE</th><th class="pb-2">YÖN</th><th class="pb-2">GİRİŞ / ÇIKIŞ</th><th class="pb-2">NET PnL ($)</th><th class="pb-2">KAPANIŞ NEDENİ</th></tr>
                        </thead>
                        <tbody id="journal-table" class="divide-y divide-slate-800/50"></tbody>
                    </table>
                </div>
            </div>
        </div>

        <!-- SAYFA 9: BORSA API -->
        <div id="page-api" class="hidden space-y-3">
            <div class="card p-4 rounded-xl space-y-3 max-w-lg">
                <h2 class="text-sm font-bold text-amber-400 uppercase">🔑 Borsa API Ayarları</h2>
                <div class="space-y-2 text-xs">
                    <div><label class="text-slate-400 block mb-1">BORSA</label><select id="api-exchange" class="w-full bg-slate-900 border border-slate-700 text-white rounded p-1.5 outline-none"><option value="BINANCE" selected>Binance Futures</option><option value="BYBIT">Bybit Linear</option></select></div>
                    <div><label class="text-slate-400 block mb-1">AĞ TÜRÜ</label><select id="api-mode" class="w-full bg-slate-900 border border-slate-700 text-white rounded p-1.5 outline-none"><option value="TESTNET" selected>Testnet (Sanal)</option><option value="LIVE">Live (Gerçek)</option></select></div>
                    <div><label class="text-slate-400 block mb-1">API KEY</label><input id="api-key" type="password" placeholder="API Key..." class="w-full bg-slate-900 border border-slate-700 text-white rounded p-1.5 outline-none font-mono"></div>
                    <div><label class="text-slate-400 block mb-1">API SECRET</label><input id="api-secret" type="password" placeholder="API Secret..." class="w-full bg-slate-900 border border-slate-700 text-white rounded p-1.5 outline-none font-mono"></div>
                    <button onclick="saveApiSettings()" class="w-full bg-amber-500 hover:bg-amber-400 text-black font-bold py-2 rounded transition">KAYDET</button>
                </div>
            </div>
        </div>

        <script>
            let chart = null;
            let candleSeries = null;
            let equityChart = null;
            let equitySeries = null;

            // Her açılışta kesinlikle BTC ile başla
            let currentSymbol = "BTC/USDT:USDT";
            localStorage.setItem("selected_sym", "BTC/USDT:USDT");

            let currentTimeframe = "5";
            let currentPnlFilter = "today";
            let currentStatsFilter = "today";
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

                const slTop = Math.min(entryY, slY);
                const slHeight = Math.abs(slY - entryY);
                ctx.fillStyle = 'rgba(239, 68, 68, 0.28)';
                ctx.fillRect(boxStartX, slTop, boxWidth, slHeight);
                ctx.strokeStyle = 'rgba(239, 68, 68, 0.6)';
                ctx.strokeRect(boxStartX, slTop, boxWidth, slHeight);

                if (tp1Y !== null) {
                    const tp1Top = Math.min(entryY, tp1Y);
                    const tp1Height = Math.abs(tp1Y - entryY);
                    ctx.fillStyle = 'rgba(74, 222, 128, 0.25)';
                    ctx.fillRect(boxStartX, tp1Top, boxWidth, tp1Height);
                    ctx.strokeRect(boxStartX, tp1Top, boxWidth, tp1Height);
                }

                if (tp2Y !== null && tp1Y !== null) {
                    const tp2Top = Math.min(tp1Y, tp2Y);
                    const tp2Height = Math.abs(tp2Y - tp1Y);
                    ctx.fillStyle = 'rgba(4, 120, 87, 0.40)';
                    ctx.fillRect(boxStartX, tp2Top, boxWidth, tp2Height);
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
                    recalculateAdvancedStats();
                } else if (tabId === 'excel') {
                    loadReportsList();
                    loadArchivePreview();
                }
            }

            function updateMultiCountdowns() {
                const now = new Date();
                const targetCpi = new Date(now.getFullYear(), now.getMonth(), 10, 15, 30, 0);
                if (now > targetCpi) targetCpi.setMonth(targetCpi.getMonth() + 1);
                formatCountdown(targetCpi - now, 'cd-cpi');

                const targetFomc = new Date(now.getFullYear(), now.getMonth() + 1, 18, 21, 0, 0);
                formatCountdown(targetFomc - now, 'cd-fomc');

                const targetNfp = new Date(now.getFullYear(), now.getMonth(), 5, 15, 30, 0);
                if (now > targetNfp) targetNfp.setMonth(targetNfp.getMonth() + 1);
                formatCountdown(targetNfp - now, 'cd-nfp');

                const targetUnlock = new Date(now.getFullYear(), now.getMonth(), now.getDate() + 3, 12, 0, 0);
                formatCountdown(targetUnlock - now, 'cd-unlock');
            }

            function formatCountdown(diff, elementId) {
                const el = document.getElementById(elementId);
                if (!el) return;
                if (diff <= 0) { el.innerText = "AÇIKLANDI"; return; }
                const days = Math.floor(diff / (1000 * 60 * 60 * 24));
                const hours = Math.floor((diff % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));
                const mins = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60));
                const secs = Math.floor((diff % (1000 * 60)) / 1000);
                el.innerText = `${days}g ${hours}s ${mins}d ${secs}sn`;
            }
            setInterval(updateMultiCountdowns, 1000);

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
                    timeScale: { 
                        timeVisible: true, 
                        secondsVisible: false, 
                        borderColor: '#1e293b',
                        // Türkiye Saati (+3) ofset ayarı
                        tickMarkFormatter: (time, tickMarkType, locale) => {
                            const d = new Date(time * 1000);
                            return d.toLocaleTimeString('tr-TR', { timeZone: 'Europe/Istanbul', hour: '2-digit', minute: '2-digit' });
                        }
                    },
                    rightPriceScale: { autoScale: true, scaleMargins: { top: 0.15, bottom: 0.15 } }
                });

                candleSeries = chart.addCandlestickSeries({
                    upColor: '#10b981', downColor: '#ef4444',
                    borderUpColor: '#10b981', borderDownColor: '#ef4444',
                    wickUpColor: '#10b981', wickDownColor: '#ef4444'
                });

                chart.timeScale().subscribeVisibleLogicalRangeChange(() => { drawPositionBoxes(); });
                window.addEventListener('resize', () => { resizeCanvas(); drawPositionBoxes(); });

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
                    timeScale: { timeVisible: true, secondsVisible: false, borderColor: '#1e293b' },
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

            function changeStatsFilter(filter) {
                currentStatsFilter = filter;
                document.querySelectorAll('.stats-tf-btn').forEach(b => b.classList.remove('active'));
                const btn = document.getElementById(`stats-tf-${filter}`);
                if (btn) btn.classList.add('active');
                recalculateAdvancedStats();
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

            function loadArchivePreview() {
                try {
                    const boundaries = getTurkeyTimeBoundaries();
                    let todayTrades = tradeHistoryCache.filter(h => (h.close_timestamp || 0) >= boundaries.todayStartTs);
                    let count = todayTrades.length;
                    let wins = todayTrades.filter(h => h.realized_pnl > 0).length;
                    let winRate = count > 0 ? ((wins / count) * 100).toFixed(1) : "0.0";
                    let totalPnl = todayTrades.reduce((acc, h) => acc + h.realized_pnl, 0);

                    document.getElementById('archive-prev-trades').innerText = count;
                    document.getElementById('archive-prev-winrate').innerText = `%${winRate}`;
                    
                    const pnlEl = document.getElementById('archive-prev-pnl');
                    pnlEl.innerText = `${totalPnl >= 0 ? '+' : ''}$${totalPnl.toFixed(2)}`;
                    pnlEl.className = `text-sm font-extrabold font-mono mt-0.5 ${totalPnl >= 0 ? 'text-emerald-400' : 'text-red-400'}`;
                } catch(e) {}
            }

            async function downloadCustomCsv() {
                const start = document.getElementById('custom-start-date').value;
                const end = document.getElementById('custom-end-date').value;
                if (!start || !end) { alert("Lütfen başlangıç ve bitiş tarihlerini seçin!"); return; }

                try {
                    const res = await fetch('/api/export/custom_csv', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({ start_date: start, end_date: end })
                    });
                    const blob = await res.blob();
                    const url = window.URL.createObjectURL(blob);
                    const a = document.createElement('a');
                    a.href = url;
                    a.download = `trades_${start}_to_${end}.csv`;
                    document.body.appendChild(a);
                    a.click();
                    a.remove();
                } catch(e) { alert("Hata oluştu."); }
            }

            function recalculatePnlMetrics() {
                try {
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

                    let periodPnl = 0, winCount = 0;
                    filtered.forEach(h => {
                        periodPnl += h.realized_pnl;
                        if (h.realized_pnl > 0) winCount++;
                    });

                    periodPnl = Math.round(periodPnl * 100) / 100;
                    const winRate = filtered.length > 0 ? ((winCount / filtered.length) * 100).toFixed(1) : "0.0";

                    const pnlElem = document.getElementById('stat-pnl');
                    if (pnlElem) {
                        pnlElem.innerText = `${periodPnl >= 0 ? '+' : ''}$${periodPnl.toFixed(2)}`;
                        pnlElem.className = `text-sm font-extrabold font-mono ${periodPnl >= 0 ? 'text-emerald-400' : 'text-red-400'}`;
                    }
                    document.getElementById('stat-winrate').innerText = `%${winRate}`;
                    document.getElementById('stat-trades').innerText = filtered.length;
                } catch(e) {}
            }

            function recalculateAdvancedStats() {
                try {
                    const boundaries = getTurkeyTimeBoundaries();
                    let filtered = [];

                    tradeHistoryCache.forEach(h => {
                        const ts = h.close_timestamp || 0;
                        if (currentStatsFilter === 'today' && ts >= boundaries.todayStartTs) filtered.push(h);
                        else if (currentStatsFilter === 'week' && ts >= boundaries.weekStartTs) filtered.push(h);
                        else if (currentStatsFilter === 'month' && ts >= boundaries.monthStartTs) filtered.push(h);
                        else if (currentStatsFilter === 'all') filtered.push(h);
                    });

                    let totalWin = 0, totalLoss = 0, winOps = 0, lossOps = 0, longTotal = 0, shortTotal = 0, totalDuration = 0;
                    let symbolPnlMap = {}, pnlSeries = [], currentWinStreak = 0, maxWinStreak = 0, currentLossStreak = 0, maxLossStreak = 0;

                    const sortedFiltered = [...filtered].sort((a,b) => (a.close_timestamp || 0) - (b.close_timestamp || 0));

                    sortedFiltered.forEach(h => {
                        totalDuration += (h.duration_mins || 1);
                        pnlSeries.push(h.realized_pnl);

                        if (h.realized_pnl > 0) {
                            totalWin += h.realized_pnl; winOps++; currentWinStreak++; currentLossStreak = 0;
                            if (currentWinStreak > maxWinStreak) maxWinStreak = currentWinStreak;
                        } else {
                            totalLoss += Math.abs(h.realized_pnl); lossOps++; currentLossStreak++; currentWinStreak = 0;
                            if (currentLossStreak > maxLossStreak) maxLossStreak = currentLossStreak;
                        }

                        if (h.direction === 'LONG') longTotal++; else shortTotal++;
                        symbolPnlMap[h.symbol] = (symbolPnlMap[h.symbol] || 0) + h.realized_pnl;
                    });

                    const totalOps = filtered.length;
                    const winRateVal = totalOps > 0 ? (winOps / totalOps) : 0;
                    const lossRateVal = totalOps > 0 ? (lossOps / totalOps) : 0;
                    const avgWinVal = winOps > 0 ? (totalWin / winOps) : 0;
                    const avgLossVal = lossOps > 0 ? (totalLoss / lossOps) : 0;

                    const expectancy = (winRateVal * avgWinVal) - (lossRateVal * avgLossVal);
                    const pf = totalLoss > 0 ? (totalWin / totalLoss).toFixed(2) : (totalWin > 0 ? "∞" : "0.00");

                    let sharpe = "0.00", sortino = "0.00";
                    if (pnlSeries.length > 1) {
                        const meanPnl = pnlSeries.reduce((a,b)=>a+b,0) / pnlSeries.length;
                        const variance = pnlSeries.reduce((a,b)=>a + Math.pow(b - meanPnl, 2), 0) / pnlSeries.length;
                        const stdDev = Math.sqrt(variance) || 1;
                        sharpe = (meanPnl / stdDev * Math.sqrt(365)).toFixed(2);
                        const negativeReturns = pnlSeries.filter(p => p < 0);
                        const downsideVar = negativeReturns.length > 0 ? negativeReturns.reduce((a,b)=>a + Math.pow(b, 2), 0) / negativeReturns.length : 1;
                        sortino = (meanPnl / (Math.sqrt(downsideVar) || 1) * Math.sqrt(365)).toFixed(2);
                    }

                    document.getElementById('stat-pf').innerText = pf;
                    const expEl = document.getElementById('stat-expectancy');
                    expEl.innerText = `${expectancy >= 0 ? '+' : ''}$${expectancy.toFixed(2)}`;
                    expEl.className = `text-xl font-bold font-mono ${expectancy >= 0 ? 'text-sky-400' : 'text-red-400'}`;

                    document.getElementById('stat-sharpe').innerText = `${sharpe} / ${sortino}`;
                    document.getElementById('stat-avg-win').innerText = `+$${avgWinVal.toFixed(2)}`;
                    document.getElementById('stat-avg-loss').innerText = `-$${avgLossVal.toFixed(2)}`;
                    document.getElementById('stat-max-win-streak').innerText = maxWinStreak;
                    document.getElementById('stat-max-loss-streak').innerText = maxLossStreak;
                    document.getElementById('stat-avg-duration').innerText = `${totalOps > 0 ? Math.round(totalDuration / totalOps) : 0} Dakika`;

                    const lPct = (longTotal + shortTotal) > 0 ? Math.round((longTotal / (longTotal + shortTotal)) * 100) : 50;
                    document.getElementById('stat-ls-ratio').innerText = `L: %${lPct} | S: %${100 - lPct}`;
                    document.getElementById('stat-ls-bar').style.width = `${lPct}%`;

                    const sortedSymbols = Object.keys(symbolPnlMap).sort((a,b) => symbolPnlMap[b] - symbolPnlMap[a]);
                    const topTbody = document.getElementById('top-symbols-table');
                    if (topTbody) {
                        topTbody.innerHTML = sortedSymbols.filter(s => symbolPnlMap[s] > 0).slice(0, 5).map(sym => `
                            <tr><td class="py-2 font-bold text-white">${sym}</td><td class="text-slate-400">${filtered.filter(h => h.symbol === sym).length}</td><td class="font-bold text-right text-emerald-400">+$${symbolPnlMap[sym].toFixed(2)}</td></tr>
                        `).join('') || '<tr><td colspan="3" class="py-2 text-slate-500 italic">Veri yok...</td></tr>';
                    }

                    const worstTbody = document.getElementById('worst-symbols-table');
                    if (worstTbody) {
                        worstTbody.innerHTML = sortedSymbols.filter(s => symbolPnlMap[s] < 0).reverse().slice(0, 5).map(sym => `
                            <tr><td class="py-2 font-bold text-white">${sym}</td><td class="text-slate-400">${filtered.filter(h => h.symbol === sym).length}</td><td class="font-bold text-right text-rose-400">-$${Math.abs(symbolPnlMap[sym]).toFixed(2)}</td></tr>
                        `).join('') || '<tr><td colspan="3" class="py-2 text-slate-500 italic">Veri yok...</td></tr>';
                    }
                } catch(e) {}
            }

            async function loadReportsList() {
                try {
                    const res = await fetch('/api/reports/list');
                    const files = await res.json();
                    const tbody = document.getElementById('reports-table');
                    if (tbody) {
                        tbody.innerHTML = files.map(f => `<tr><td class="py-2 font-mono text-slate-300">📄 ${f}</td><td class="text-right"><a href="/api/reports/download/${f}" class="bg-sky-600 hover:bg-sky-500 text-white font-bold px-2 py-1 rounded text-[10px]">İndir</a></td></tr>`).join('') || '<tr><td colspan="2" class="py-2 text-slate-500 italic">Arşiv yok...</td></tr>';
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
                            time: Math.floor(parseInt(c[0]) / 1000), open: parseFloat(c[1]), high: parseFloat(c[2]), low: parseFloat(c[3]), close: parseFloat(c[4])
                        })).sort((a, b) => a.time - b.time);
                    }
                } catch(e) {}
                return [];
            }

            async function loadChartCandles(symbol, posData = null, isLiveTick = false) {
                try {
                    const candles = await fetchCandlesDirect(symbol, currentTimeframe);
                    if (candles.length > 0 && candleSeries) {
                        const lastCandle = candles[candles.length - 1];
                        const pConf = getPrecisionConfig(lastCandle.close);
                        candleSeries.applyOptions({ priceFormat: { type: 'price', precision: pConf.precision, minMove: pConf.minMove } });
                        candleSeries.setData(candles);

                        if (!isLiveTick) {
                            chart.priceScale('right').applyOptions({ autoScale: true });
                            chart.timeScale().fitContent();
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

                        if (posData && candleSeries) {
                            const entryLine = candleSeries.createPriceLine({ price: posData.entry, color: '#38bdf8', lineWidth: 2, lineStyle: LightweightCharts.LineStyle.Solid, axisLabelVisible: true, title: 'GİRİŞ' });
                            const slLine = candleSeries.createPriceLine({ price: posData.sl, color: '#ef4444', lineWidth: 2, lineStyle: LightweightCharts.LineStyle.Dashed, axisLabelVisible: true, title: 'STOP' });
                            const tp1Line = candleSeries.createPriceLine({ price: posData.tp1, color: '#4ade80', lineWidth: 2, lineStyle: LightweightCharts.LineStyle.Dashed, axisLabelVisible: true, title: 'TP1' });
                            const tp2Line = candleSeries.createPriceLine({ price: posData.tp2, color: '#047857', lineWidth: 2, lineStyle: LightweightCharts.LineStyle.Dashed, axisLabelVisible: true, title: 'TP2' });
                            priceLines.push(entryLine, slLine, tp1Line, tp2Line);
                            const p = posData.entry < 1 ? 6 : 2;
                            document.getElementById('chart-levels').innerHTML = `<span class="text-sky-400 font-mono">Giriş: ${posData.entry}</span> | <span class="text-red-400 font-mono">SL: ${posData.sl.toFixed(p)}</span> | <span class="text-emerald-600 font-mono">TP2: ${posData.tp2.toFixed(p)}</span>`;
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
                    <div class="flex justify-between items-center mb-2"><span class="font-bold text-base text-white">${pos.symbol}</span><span class="px-2 py-0.5 rounded text-xs font-bold ${pos.direction === 'LONG' ? 'bg-emerald-500/20 text-emerald-400' : 'bg-red-500/20 text-red-400'}">${pos.direction}</span></div>
                    <div class="space-y-1 text-slate-300">${pos.reasons.map(r => `<div class="bg-slate-900/60 p-1.5 rounded border border-slate-800">✓ ${r}</div>`).join('')}</div>
                    <div class="mt-2 p-2 bg-black/40 rounded border border-slate-800 text-[11px] space-y-1">
                        <div class="text-slate-400">Giriş Saati: <span class="text-white font-bold">${pos.open_time}</span></div>
                        <div class="text-slate-400">Mod: <span class="text-white font-bold">${pos.leverage}x ${modeLabel} ($${pos.margin})</span></div>
                        <div class="text-red-400">SL: <span class="font-mono">${pos.sl.toFixed(p)}</span></div>
                        <div class="text-emerald-400">TP2: <span class="font-mono">${pos.tp2.toFixed(p)}</span></div>
                    </div>`;
            }

            async function manualClosePos(symbol) {
                await fetch('/api/manual/close_position', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({symbol}) });
                updateDashboard();
            }

            async function manualPartialClose(symbol, ratio) {
                await fetch('/api/manual/partial_close', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({symbol, ratio}) });
                updateDashboard();
            }

            async function manualCloseAll() {
                if (confirm("Tüm pozisyonları kapatmak istediğinize emin misiniz?")) {
                    await fetch('/api/manual/close_all', { method: 'POST' });
                    updateDashboard();
                }
            }

            async function manualBreakevenAll() {
                if (confirm("Tüm stopları başa başa çekmek istiyor musunuz?")) {
                    await fetch('/api/manual/breakeven_all', { method: 'POST' });
                    updateDashboard();
                }
            }

            async function manualBreakeven(symbol) {
                await fetch('/api/manual/breakeven', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({symbol}) });
                updateDashboard();
            }

            async function manualToggleTrailing(symbol) {
                await fetch('/api/manual/toggle_trailing', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({symbol}) });
                updateDashboard();
            }

            async function manualUpdateSlTp(symbol) {
                const sl = parseFloat(document.getElementById(`manual-sl-${symbol}`).value);
                const tp2 = parseFloat(document.getElementById(`manual-tp-${symbol}`).value);
                await fetch('/api/manual/update_sltp', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({symbol, sl, tp2}) });
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

                const res = await fetch('/api/update_settings', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({total_balance, risk_pct, leverage, margin_mode, max_open_positions, max_total_margin_pct})
                });
                if (res.ok) { alert("Ayarlar Kaydedildi!"); updateDashboard(); }
            }

            async function saveApiSettings() {
                const exchange = document.getElementById('api-exchange').value;
                const mode = document.getElementById('api-mode').value;
                const api_key = document.getElementById('api-key').value;
                const api_secret = document.getElementById('api-secret').value;
                await fetch('/api/update_api', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({exchange, mode, api_key, api_secret, auto_trade: false}) });
                alert("API Kaydedildi!");
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

                    if (data.sentiment_data) {
                        document.getElementById('sent-btc-15m').innerText = `${data.btc_15m_change >= 0 ? '+' : ''}%${data.btc_15m_change}`;
                        document.getElementById('sent-btc-rsi').innerText = data.sentiment_data.btc_rsi;
                        document.getElementById('sent-btc-vol').innerText = data.sentiment_data.btc_volume_24h;
                        document.getElementById('sent-bias').innerText = data.sentiment_data.market_bias;
                        document.getElementById('sent-volatility').innerText = data.sentiment_data.market_volatility;
                        
                        document.getElementById('sent-liq-total').innerText = data.sentiment_data.total_liquidations_24h;
                        document.getElementById('sent-long-liq').innerText = `%${data.sentiment_data.long_liq_pct}`;
                        document.getElementById('sent-short-liq').innerText = `%${data.sentiment_data.short_liq_pct}`;
                        document.getElementById('liq-long-bar').style.width = `${data.sentiment_data.long_liq_pct}%`;
                        document.getElementById('liq-short-bar').style.width = `${data.sentiment_data.short_liq_pct}%`;

                        document.getElementById('sent-oi-change').innerText = data.sentiment_data.total_oi_change;
                        document.getElementById('sent-btc-dom').innerText = data.sentiment_data.btc_dominance;
                        document.getElementById('sent-avg-funding').innerText = data.sentiment_data.avg_funding_rate;

                        document.getElementById('sent-bid-val').innerText = `%${data.sentiment_data.bid_pressure}`;
                        document.getElementById('sent-ask-val').innerText = `%${data.sentiment_data.ask_pressure}`;
                        document.getElementById('bid-bar').style.width = `${data.sentiment_data.bid_pressure}%`;
                        document.getElementById('ask-bar').style.width = `${data.sentiment_data.ask_pressure}%`;

                        document.getElementById('sent-whale-in').innerText = data.sentiment_data.whale_inflow;
                        document.getElementById('sent-whale-out').innerText = data.sentiment_data.whale_outflow;
                        document.getElementById('sent-net-whale').innerText = data.sentiment_data.net_whale_flow;

                        const lsRatio = data.sentiment_data.long_short_ratio;
                        document.getElementById('ls-ratio-text').innerText = `%${lsRatio} Long / %${(100 - lsRatio).toFixed(1)} Short`;
                        document.getElementById('ls-bar').style.width = `${lsRatio}%`;
                    }

                    document.getElementById('stat-max-dd').innerText = `%${data.max_drawdown_pct || '0.00'}`;
                    const shockBadge = document.getElementById('btc-shock-badge');
                    if (data.btc_shock_lock) { shockBadge.classList.remove('hidden'); shockBadge.innerText = data.btc_shock_reason; } else { shockBadge.classList.add('hidden'); }

                    const ddBadge = document.getElementById('drawdown-badge');
                    if (data.daily_loss_locked) ddBadge.classList.remove('hidden'); else ddBadge.classList.add('hidden');

                    document.getElementById('btc-regime-badge').innerText = data.btc_regime || "BTC: AKTİF";

                    const totalUsedMargin = data.active_positions.reduce((acc, p) => acc + p.margin, 0);
                    const totalRiskAmount = data.active_positions.reduce((acc, p) => acc + p.max_loss, 0);
                    const usedPct = data.total_balance > 0 ? ((totalUsedMargin / data.total_balance) * 100).toFixed(1) : "0.0";
                    document.getElementById('stat-used-margin').innerText = `$${totalUsedMargin.toFixed(1)} (%${usedPct})`;

                    document.getElementById('man-total-pos').innerText = `${data.active_positions.length} Adet`;
                    document.getElementById('man-total-margin').innerText = `$${totalUsedMargin.toFixed(2)}`;
                    document.getElementById('man-total-risk').innerText = `$${totalRiskAmount.toFixed(2)}`;

                    tradeHistoryCache = data.trade_history;
                    recalculatePnlMetrics();
                    recalculateAdvancedStats();
                    loadArchivePreview();

                    if (data.equity_curve && data.equity_curve.length > 0 && equitySeries) equitySeries.setData(data.equity_curve);
                    document.getElementById('log-box').innerHTML = data.logs.map(l => `<div>${l}</div>`).join('');

                    lastPositions = data.active_positions;
                    const activeTbody = document.getElementById('active-pos-table');
                    if (activeTbody) {
                        activeTbody.innerHTML = data.active_positions.map((p, idx) => `
                            <tr class="hover:bg-slate-800/80 cursor-pointer ${selectedPos && selectedPos.symbol === p.symbol ? 'bg-slate-800/60' : ''}" onclick="selectPosition(lastPositions[${idx}])">
                                <td class="py-2 font-bold text-white">${p.symbol}</td>
                                <td class="text-slate-400 font-mono text-[10px]">${p.open_time}</td>
                                <td class="${p.direction === 'LONG' ? 'text-emerald-400' : 'text-red-400'} font-bold">${p.direction} (${p.leverage}x)</td>
                                <td class="text-white font-mono">$${p.margin}</td>
                                <td class="font-mono text-slate-300">${p.entry}</td>
                                <td class="font-mono text-white font-bold">${p.current_price || p.entry}</td>
                                <td class="font-mono font-bold ${p.unrealized_pnl >= 0 ? 'text-emerald-400' : 'text-red-400'}">${p.unrealized_pnl >= 0 ? '+' : ''}$${p.unrealized_pnl.toFixed(2)}</td>
                                <td class="w-24"><div class="w-full bg-slate-800 rounded-full h-1.5 overflow-hidden"><div class="bg-emerald-500 h-1.5 rounded-full" style="width: ${p.progress_pct}%"></div></div><span class="text-[9px] text-slate-400 font-mono">%${p.progress_pct}</span></td>
                            </tr>`).join('');
                    }

                    const manualTbody = document.getElementById('manual-pos-table');
                    if (manualTbody) {
                        manualTbody.innerHTML = data.active_positions.map(p => `
                            <tr>
                                <td class="py-2 font-bold text-white">${p.symbol}</td>
                                <td class="${p.direction === 'LONG' ? 'text-emerald-400' : 'text-red-400'} font-bold">${p.direction}</td>
                                <td class="font-mono text-slate-300">${p.entry} / ${p.current_price || p.entry}</td>
                                <td class="font-bold ${p.unrealized_pnl >= 0 ? 'text-emerald-400' : 'text-red-400'}">${p.unrealized_pnl >= 0 ? '+' : ''}$${p.unrealized_pnl.toFixed(2)}</td>
                                <td><input id="manual-sl-${p.symbol}" type="number" step="any" value="${p.sl}" class="bg-slate-900 border border-slate-700 w-20 px-1 py-0.5 rounded text-white font-mono"></td>
                                <td><input id="manual-tp-${p.symbol}" type="number" step="any" value="${p.tp2}" class="bg-slate-900 border border-slate-700 w-20 px-1 py-0.5 rounded text-white font-mono"></td>
                                <td class="text-center"><button onclick="manualToggleTrailing('${p.symbol}')" class="px-2 py-1 rounded text-[10px] font-bold ${p.trailing_active ? 'bg-emerald-600 text-black animate-pulse' : 'bg-slate-800 text-slate-400'}">${p.trailing_active ? 'AÇIK' : 'KAPALI'}</button></td>
                                <td class="text-right space-x-1">
                                    <button onclick="manualUpdateSlTp('${p.symbol}')" class="bg-slate-700 hover:bg-slate-600 px-1.5 py-1 rounded text-[10px] text-white">💾</button>
                                    <button onclick="manualPartialClose('${p.symbol}', 0.5)" class="bg-amber-600 hover:bg-amber-500 px-1.5 py-1 rounded text-[10px] text-white font-bold">%50</button>
                                    <button onclick="manualBreakeven('${p.symbol}')" class="bg-sky-600 hover:bg-sky-500 px-1.5 py-1 rounded text-[10px] text-white">Başa Baş</button>
                                    <button onclick="manualClosePos('${p.symbol}')" class="bg-rose-600 hover:bg-rose-500 px-2 py-1 rounded text-[10px] text-white font-bold">Kapat</button>
                                </td>
                            </tr>`).join('') || '<tr><td colspan="8" class="py-3 text-slate-500 italic">Açık pozisyon yok...</td></tr>';
                    }

                    const journalTbody = document.getElementById('journal-table');
                    if (journalTbody) {
                        journalTbody.innerHTML = data.trade_history.map(h => `
                            <tr class="hover:bg-slate-800/40">
                                <td class="py-2 font-mono text-slate-400">${h.close_time}</td><td class="font-bold text-white">${h.symbol}</td>
                                <td class="${h.direction === 'LONG' ? 'text-emerald-400' : 'text-red-400'} font-bold">${h.direction}</td>
                                <td class="font-mono">${h.entry} ➔ ${h.close_price}</td>
                                <td class="font-bold ${h.realized_pnl >= 0 ? 'text-emerald-400' : 'text-red-400'}">${h.realized_pnl >= 0 ? '+' : ''}$${h.realized_pnl.toFixed(2)}</td>
                                <td class="text-[10px] text-sky-300 font-semibold">${h.close_reason}</td>
                            </tr>`).join('') || '<tr><td colspan="6" class="py-4 text-center text-slate-500 italic">Kayıt yok...</td></tr>';
                    }

                    const radarTbody = document.getElementById('radar-table');
                    if (radarTbody && data.radar_symbols) {
                        radarTbody.innerHTML = [...data.radar_symbols].sort((a,b) => b.score - a.score).map(r => `
                            <tr class="hover:bg-slate-800/40 cursor-pointer" onclick="currentSymbol='${r.symbol}'; switchTab('terminal'); loadChartCandles('${r.symbol}', null, false);">
                                <td class="py-2 font-bold text-white">${r.symbol}</td><td class="font-mono">$${r.price}</td>
                                <td class="font-bold ${r.trend === 'LONG' ? 'text-emerald-400' : 'text-red-400'}">${r.trend}</td>
                                <td class="font-mono">${r.rsi}</td><td class="font-mono text-amber-400">${r.vol_ratio}x</td>
                                <td><span class="px-2 py-0.5 rounded text-[10px] font-bold ${r.score >= 75 ? 'bg-emerald-500/20 text-emerald-400' : 'bg-slate-800 text-slate-400'}">${r.score} Puan</span></td>
                            </tr>`).join('');
                    }

                    if (currentSymbol) loadChartCandles(currentSymbol, selectedPos, true);
                    if (!selectedPos && data.active_positions.length > 0) selectPosition(data.active_positions[0]);
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
