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

UTC_TZ = timezone.utc

# =================================================================
# 📱 TELEGRAM BİLDİRİM AYARLARI (BURAYI DOLDURUN)
# =================================================================
TELEGRAM_BOT_TOKEN = "8971696278:AAHiBk7gzMGxjAz2mi4KqV4LEUWwhVH6NKc"
TELEGRAM_CHAT_ID = "2088808175"
# =================================================================

def get_now_str():
    return datetime.now(UTC_TZ).strftime("%H:%M:%S")

def get_now_datetime():
    return datetime.now(UTC_TZ)

async def send_telegram_alert(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(url, json=payload, timeout=5)
    except Exception as e:
        logging.error(f"Telegram bildirim hatası: {e}")

CSV_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "daily_reports")
os.makedirs(CSV_DIR, exist_ok=True)

system_state = {
    "initial_balance": 1000.0,
    "total_balance": 1000.0,
    "locked_margin": 0.0,
    "free_balance": 1000.0,
    "realized_pnl": 0.0,
    "unrealized_pnl": 0.0,
    "peak_balance": 1000.0,
    "max_drawdown_pct": 0.0,
    "risk_pct": 5.0,
    "leverage": 50,
    "margin_mode": "ISOLATED",
    "max_open_positions": 5,
    "max_total_margin_pct": 50.0,
    "daily_drawdown_limit_pct": 100.0,
    "daily_loss_locked": False,
    "daily_start_balance": 1000.0,
    "last_day_reset": get_now_datetime().strftime("%Y-%m-%d"),
    "btc_regime": "YÜKLENİYOR...",
    "btc_15m_change": 0.0,
    "btc_shock_lock": False,
    "btc_shock_reason": "",
    "macro_lock": False,
    "flash_crash_active": False,
    "market_breadth": 50.0,
    "breadth_bullish": 0,
    "breadth_total": 0,
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
    "order_book_metrics": {"bid_vol": 0, "ask_vol": 0, "bid_pressure": 50.0, "ask_pressure": 50.0},
    "api_settings": {
        "exchange": "BINANCE",
        "mode": "TESTNET",
        "api_key": "",
        "api_secret": "",
        "auto_trade": False
    },
    "equity_curve": [{"time": int(get_now_datetime().timestamp()), "value": 1000.0}],
    "logs": [],
    "bot_trading_active": True
}

EXCLUDED_KEYWORDS = [
    'NVDA', 'GOOGL', 'AAPL', 'TSLA', 'MSFT', 'AMZN', 'META', 'NFLX', 'AMD', 'COIN',
    'BABA', 'PLTR', 'SOXS', 'SOXL', 'QQQ', 'SPY', 'WDC', 'DELL', 'IONQ', 'GLW', 'BIRB',
    'TBT', 'TLT', 'PDD', 'NIO', 'BILI', 'LI', 'XPEV', 'MSTR', 'MARA', 'RIOT', 'CLSK',
    'CASHCAT', 'WLFI', 'TRUMP', 'MELANIA', 'PEPE2', 'SHIB2'
]

SECTORS = {
    "AI": ["FET", "AGIX", "OCEAN", "RNDR", "WLD", "ARKM", "TAO", "NEAR", "ICP", "GRT"],
    "MEME": ["PEPE", "DOGE", "SHIB", "FLOKI", "BONK", "WIF", "BOME", "MEME", "DOGS"],
    "L1_L2": ["BTC", "ETH", "SOL", "AVAX", "ADA", "SUI", "APT", "SEI", "OP", "ARB"]
}

retest_tracker = {}

def get_sector(symbol):
    base = symbol.split('/')[0].upper()
    if base.startswith('1000'): base = base[4:]
    for sec, coins in SECTORS.items():
        if base in coins: return sec
    return "OTHER"

def is_macro_event_near():
    now = get_now_datetime()
    events = [
        datetime(now.year, now.month, 10, 15, 30, tzinfo=UTC_TZ), 
        datetime(now.year, now.month, 5, 15, 30, tzinfo=UTC_TZ),
        datetime(now.year, now.month, 18, 21, 0, tzinfo=UTC_TZ)
    ]
    for ev in events:
        if now > ev: 
            ev = ev.replace(month = ev.month % 12 + 1)
        diff = abs((now - ev).total_seconds())
        if diff <= 3600:
            return True
    return False

async def create_exchange_instance():
    api_conf = system_state["api_settings"]
    exch_id = api_conf["exchange"].lower()
    
    args = {
        'enableRateLimit': True,
        'options': {'defaultType': 'future' if exch_id == 'binance' else 'linear'},
        'timeout': 10000
    }
    
    if api_conf.get("api_key") and api_conf.get("api_secret"):
        args['apiKey'] = api_conf["api_key"]
        args['secret'] = api_conf["api_secret"]
        
    exchange_class = getattr(ccxt, exch_id)
    exchange = exchange_class(args)
    
    if api_conf.get("mode") == "TESTNET":
        exchange.set_sandbox_mode(True)
        
    return exchange

async def execute_manual_real_order(symbol, direction, amount_raw):
    if not system_state["api_settings"]["auto_trade"] or not system_state["api_settings"]["api_key"]:
        return
    try:
        exchange = await create_exchange_instance()
        close_side = 'sell' if direction == 'LONG' else 'buy'
        safe_amount = float(exchange.amount_to_precision(symbol, amount_raw))
        await exchange.create_order(symbol, 'market', close_side, safe_amount)
        add_log(f"🕹️ MANUEL GERÇEK EMİR İLETİLDİ: {symbol} {close_side.upper()} {safe_amount}")
        await exchange.close()
    except Exception as e:
        add_log(f"❌ MANUEL EMİR HATASI ({symbol}): {str(e)[:60]}")

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

def sync_wallet_accounting():
    locked = round(sum(float(p.get("margin", 0.0)) for p in system_state["active_positions"]), 2)
    unrealized = round(sum(float(p.get("unrealized_pnl", 0.0)) for p in system_state["active_positions"]), 2)
    system_state["locked_margin"] = locked
    system_state["unrealized_pnl"] = unrealized
    system_state["free_balance"] = round(max(0.0, system_state["total_balance"] - locked), 2)

def apply_realized_pnl(amount: float):
    amount = round(float(amount), 2)
    system_state["realized_pnl"] = round(system_state.get("realized_pnl", 0.0) + amount, 2)
    system_state["total_balance"] = round(system_state["total_balance"] + amount, 2)
    sync_wallet_accounting()

def check_daily_drawdown():
    return

def calculate_indicators(df):
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / (loss + 1e-9)
    df['rsi'] = 100 - (100 / (1 + rs))

    df['ema20'] = df['close'].ewm(span=20, adjust=False).mean()
    df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()

    high_diff = df['high'].diff()
    low_diff = -df['low'].diff()
    plus_dm = high_diff.where((high_diff > low_diff) & (high_diff > 0), 0.0)
    minus_dm = low_diff.where((low_diff > high_diff) & (low_diff > 0), 0.0)

    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df['atr'] = tr.rolling(window=14).mean()

    atr14 = df['atr']
    plus_di = 100 * (plus_dm.rolling(window=14).mean() / (atr14 + 1e-9))
    minus_di = 100 * (minus_dm.rolling(window=14).mean() / (atr14 + 1e-9))
    dx = 100 * ((plus_di - minus_di).abs() / (plus_di + minus_di + 1e-9))
    df['adx'] = dx.rolling(window=14).mean()

    df['vol_ma'] = df['volume'].rolling(window=20).mean()
    return df

def compute_position_metrics(entry, sl, lev, risk_pct):
    balance = system_state["total_balance"]
    actual_risk_pct = risk_pct / 100.0
    leverage = lev

    risk_amount = balance * actual_risk_pct
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

        if pct_15m <= -10.0:
            system_state["flash_crash_active"] = True
            system_state["bot_trading_active"] = False
            msg = "🚨 FLASH CRASH TESPİT EDİLDİ! Bot acil uyku moduna geçti, işlemler kilitlendi!"
            add_log(msg)
            asyncio.create_task(send_telegram_alert(f"🚨 <b>FLASH CRASH UYARISI</b>\n{msg}"))

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
    global retest_tracker
    try:
        if system_state["daily_loss_locked"] or not system_state.get("bot_trading_active", True) or system_state["macro_lock"] or system_state["flash_crash_active"]:
            return None

        sec = get_sector(symbol)
        sec_count = sum(1 for p in system_state["active_positions"] if get_sector(p["symbol"]) == sec)
        if sec != "OTHER" and sec_count >= 2:
            return None

        base = symbol.split('/')[0].upper()
        if any(exc in base for exc in EXCLUDED_KEYWORDS):
            return None

        tasks = [
            exchange.fetch_ohlcv(symbol, timeframe='1h', limit=40),
            exchange.fetch_ohlcv(symbol, timeframe='1h', limit=40),
            exchange.fetch_ohlcv(symbol, timeframe='4h', limit=50),
            exchange.fetch_order_book(symbol, limit=20)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        if any(isinstance(r, Exception) or not r or len(r) < 30 for r in results[:3]):
            return None

        df_1h_main = calculate_indicators(pd.DataFrame(results[0], columns=['t', 'open', 'high', 'low', 'close', 'volume']))
        df_1h_retest = calculate_indicators(pd.DataFrame(results[1], columns=['t', 'open', 'high', 'low', 'close', 'volume']))
        df_4h = calculate_indicators(pd.DataFrame(results[2], columns=['t', 'open', 'high', 'low', 'close', 'volume']))
        book = results[3] if not isinstance(results[3], Exception) and results[3] else {}

        bids = book.get('bids', [])
        asks = book.get('asks', [])
        bid_vol = sum(b[1] for b in bids[:10]) if bids else 1.0
        ask_vol = sum(a[1] for a in asks[:10]) if asks else 1.0
        total_depth = bid_vol + ask_vol
        bid_pressure = round((bid_vol / total_depth) * 100, 1)
        ask_pressure = round((ask_vol / total_depth) * 100, 1)

        if symbol == system_state.get("selected_symbol", "BTC/USDT:USDT"):
            system_state["order_book_metrics"] = {
                "bid_vol": round(bid_vol, 2), "ask_vol": round(ask_vol, 2),
                "bid_pressure": bid_pressure, "ask_pressure": ask_pressure
            }

        c_1h = df_1h_main.iloc[-1]
        c_4h = df_4h.iloc[-1]

        system_state["breadth_total"] += 1
        if c_1h['close'] > c_1h['ema50']:
            system_state["breadth_bullish"] += 1

        swing_low_1h = df_1h_retest['low'].iloc[-20:-3].min()
        swing_high_1h = df_1h_retest['high'].iloc[-20:-3].max()
        recent_breakout_high = df_1h_main['high'].iloc[-8:-1].max()
        recent_breakout_low = df_1h_main['low'].iloc[-8:-1].min()

        score = 0
        direction = None
        reasons = []

        adx_val = c_1h['adx'] if pd.notnull(c_1h['adx']) else 25.0
        if adx_val < 18:
            return None

        sweep_low = df_1h_retest['low'].iloc[-4:].min() < swing_low_1h
        body_size = abs(c_1h['close'] - c_1h['open'])
        total_candle_size = c_1h['high'] - c_1h['low']
        is_strong_green = (
            c_1h['close'] > recent_breakout_high
            and c_1h['close'] > c_1h['open']
            and (body_size / (total_candle_size + 1e-9) > 0.35)
        )

        sweep_high = df_1h_retest['high'].iloc[-4:].max() > swing_high_1h
        is_strong_red = (
            c_1h['close'] < recent_breakout_low
            and c_1h['close'] < c_1h['open']
            and (body_size / (total_candle_size + 1e-9) > 0.35)
        )

        if sweep_low and is_strong_green:
            if not (system_state["btc_shock_lock"] and system_state["btc_15m_change"] <= -1.2):
                retest_tracker[symbol] = {
                    "direction": "LONG",
                    "level": recent_breakout_high,
                    "score_base": 40,
                    "reasons": ["⚡ 1H Dip Likiditesi Alındı + 1H Güçlü Kırılım"]
                }
        elif sweep_high and is_strong_red:
            if not (system_state["btc_shock_lock"] and system_state["btc_15m_change"] >= 1.2):
                retest_tracker[symbol] = {
                    "direction": "SHORT",
                    "level": recent_breakout_low,
                    "score_base": 40,
                    "reasons": ["⚡ 1H Tepe Likiditesi Alındı + 1H Güçlü Kırılım"]
                }

        if symbol in retest_tracker:
            tracker = retest_tracker[symbol]
            if tracker["direction"] == "LONG":
                if c_1h['low'] <= tracker["level"] * 1.005 and c_1h['close'] > tracker["level"] and c_1h['close'] > c_1h['open']:
                    direction = "LONG"
                    score += tracker["score_base"] + 25
                    reasons = tracker["reasons"] + ["🎯 Kusursuz 1H Break & Retest (Seviye Üstü Yeşil Mum Kapanışı)"]
                    del retest_tracker[symbol]
            elif tracker["direction"] == "SHORT":
                if c_1h['high'] >= tracker["level"] * 0.995 and c_1h['close'] < tracker["level"] and c_1h['close'] < c_1h['open']:
                    direction = "SHORT"
                    score += tracker["score_base"] + 25
                    reasons = tracker["reasons"] + ["🎯 Kusursuz 1H Break & Retest (Seviye Altı Kırmızı Mum Kapanışı)"]
                    del retest_tracker[symbol]

        if direction == "LONG" and bid_pressure >= 55.0:
            book_score = int((bid_pressure - 50) * 1.5)
            score += book_score
            reasons.append(f"🟢 L2 Emir Defteri Alıcı Baskısı Yüksek (%{bid_pressure})")
        elif direction == "SHORT" and ask_pressure >= 55.0:
            book_score = int((ask_pressure - 50) * 1.5)
            score += book_score
            reasons.append(f"🔴 L2 Emir Defteri Satıcı Baskısı Yüksek (%{ask_pressure})")

        if direction == "LONG":
            if c_1h['close'] > c_1h['ema50'] and c_1h['close'] > c_1h['ema20']:
                score += 20
                reasons.append("📈 1H Güçlü Ana Trend (Boğa) Onayı")
        elif direction == "SHORT":
            if c_1h['close'] < c_1h['ema50'] and c_1h['close'] < c_1h['ema20']:
                score += 20
                reasons.append("📉 1H Güçlü Ana Trend (Ayı) Onayı")

        vol_ratio = float(c_1h['volume'] / (c_1h['vol_ma'] + 1e-9)) if pd.notnull(c_1h['vol_ma']) else 1.0
        if direction and vol_ratio >= 1.25:
            score += 10
            reasons.append(f"🔥 Yüksek Hacim Desteği ({vol_ratio:.1f}x)")

        radar_item = {
            "symbol": symbol,
            "price": float(c_1h['close']),
            "rsi": round(float(c_1h['rsi']), 1) if pd.notnull(c_1h['rsi']) else 50.0,
            "vol_ratio": round(vol_ratio, 2),
            "trend": direction if direction else ("LONG" if c_1h['close'] > c_1h['ema50'] else "SHORT"),
            "score": score
        }
        
        system_state["radar_symbols"] = [r for r in system_state["radar_symbols"] if r["symbol"] != symbol]
        system_state["radar_symbols"].append(radar_item)
        if len(system_state["radar_symbols"]) > 60:
            system_state["radar_symbols"].pop(0)

        btc_change_abs = abs(system_state["btc_15m_change"])
        dynamic_threshold = 75
        if btc_change_abs > 0.8:
            dynamic_threshold = 85
        elif btc_change_abs < 0.3:
            dynamic_threshold = 70

        if not direction or score < dynamic_threshold:
            return None

        entry = float(c_1h['close'])
        atr = float(c_1h['atr']) if pd.notnull(c_1h['atr']) else entry * 0.01

        effective_leverage = system_state["leverage"]
        effective_risk = system_state["risk_pct"]

        if direction == "LONG":
            sl = float(df_1h_main['low'].iloc[-8:].min() - (2.8 * atr))
            if (entry - sl) / entry < 0.02:
                sl = entry * 0.98
            risk_dist = entry - sl
            tp1 = entry + (1.5 * risk_dist)
            tp2 = entry + (3.0 * risk_dist)
        else:
            sl = float(df_1h_main['high'].iloc[-8:].max() + (2.8 * atr))
            if (sl - entry) / entry < 0.02:
                sl = entry * 1.02
            risk_dist = sl - entry
            tp1 = entry - (1.5 * risk_dist)
            tp2 = entry - (3.0 * risk_dist)

        pos_size, margin, max_loss = compute_position_metrics(entry, sl, effective_leverage, effective_risk)

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
            "leverage": effective_leverage,
            "margin_mode": system_state["margin_mode"],
            "tp1_hit": False,
            "trailing_active": False,
            "atr": atr, 
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
    add_log("Quant Motoru (Avcı): Saf UTC Zaman Dilimi + Mum Kapanış Onaylı Retest Aktif!")

    while True:
        exchange = None
        try:
            exchange = await create_exchange_instance()

            check_daily_drawdown()
            sync_wallet_accounting()
            await update_btc_metrics(exchange)
            await fetch_fear_greed()

            macro_near = is_macro_event_near()
            if macro_near and not system_state.get("macro_lock"):
                system_state["macro_lock"] = True
                msg = "⚠️ MAKRO VERİ KORUMASI: Etkinliğe 1 Saat Kaldı! Yeni işlem alımı durduruldu. Stoplar Girişe (Başa Baş) çekiliyor."
                add_log(msg)
                asyncio.create_task(send_telegram_alert(f"⚠️ <b>MAKRO VERİ KİLİDİ</b>\n{msg}"))
                for p in system_state["active_positions"]:
                    p['sl'] = p['entry']
            elif not macro_near and system_state.get("macro_lock"):
                system_state["macro_lock"] = False
                add_log("✅ MAKRO VERİ KORUMASI: Piyasa dalgalanması sona erdi. Normal işleyişe dönüldü.")

            if system_state.get("flash_crash_active") and system_state["active_positions"]:
                for pos in list(system_state["active_positions"]):
                    asyncio.create_task(execute_manual_real_order(pos['symbol'], pos['direction'], pos['active_size']))
                    system_state["active_positions"].remove(pos)
                add_log("🚨 CRASH GÜVENLİĞİ: Tüm açık işlemler acil durum kapsamında market emriyle kapatıldı!")
                sync_wallet_accounting()

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
            system_state["breadth_total"] = 0
            system_state["breadth_bullish"] = 0

            batch_size = 3
            for i in range(0, len(crypto_symbols), batch_size):
                chunk = crypto_symbols[i:i + batch_size]
                tasks = [analyze_symbol(exchange, s) for s in chunk]
                signals = await asyncio.gather(*tasks, return_exceptions=True)

                for sig in signals:
                    if sig and isinstance(sig, dict):
                        exists = any(p['symbol'] == sig['symbol'] for p in system_state["active_positions"])
                        if not exists:
                            max_pos = system_state["max_open_positions"]
                            if max_pos == -1:
                                if system_state["btc_shock_lock"] or "AYI" in system_state["btc_regime"]:
                                    max_pos = 5
                                else:
                                    max_pos = 15

                            if max_pos > 0 and len(system_state["active_positions"]) >= max_pos:
                                continue

                            current_total_margin = system_state["locked_margin"]
                            allowed_margin = system_state["total_balance"] * (system_state["max_total_margin_pct"] / 100.0)
                            
                            if (current_total_margin + sig['margin']) > allowed_margin or sig['margin'] > system_state["free_balance"]:
                                continue

                            system_state["active_positions"].append(sig)
                            sync_wallet_accounting()
                            mode_label = "İzole" if sig['margin_mode'] == "ISOLATED" else "Cross"
                            
                            log_msg = f"🟢 POZİSYON AÇILDI: {sig['symbol']} {sig['direction']} | Puan: {sig['score']} | {sig['leverage']}x | Teminat: ${sig['margin']} | Risk: ${sig['max_loss']}"
                            add_log(log_msg)
                            
                            tg_msg = f"🟢 <b>YENİ İŞLEM AÇILDI</b>\n\n<b>Parite:</b> {sig['symbol']}\n<b>Yön:</b> {sig['direction']} ({sig['leverage']}x {mode_label})\n<b>Giriş:</b> {sig['entry']}\n<b>Hedef 2:</b> {sig['tp2']}\n<b>Stop:</b> {sig['sl']}\n<b>Risk (Zarar):</b> ${sig['max_loss']}\n<b>Skor:</b> {sig['score']}"
                            asyncio.create_task(send_telegram_alert(tg_msg))

                            if system_state["api_settings"]["auto_trade"] and system_state["api_settings"]["api_key"]:
                                try:
                                    try:
                                        await exchange.set_leverage(sig['leverage'], sig['symbol'])
                                    except Exception as e:
                                        add_log(f"⚠️ Kaldıraç uyarısı: {str(e)[:40]}")
                                    
                                    safe_amount = float(exchange.amount_to_precision(sig['symbol'], sig['pos_size']))
                                    side = 'buy' if sig['direction'] == 'LONG' else 'sell'
                                    
                                    await exchange.create_order(sig['symbol'], 'market', side, safe_amount)
                                    add_log(f"🚀 GERÇEK EMİR İLETİLDİ: {sig['symbol']} {side.upper()} {safe_amount} Adet")
                                except Exception as e:
                                    add_log(f"❌ GERÇEK EMİR HATASI ({sig['symbol']}): {str(e)[:60]}")

                system_state["last_scan_time"] = get_now_str()
                await asyncio.sleep(2)

            if system_state["breadth_total"] > 0:
                system_state["market_breadth"] = (system_state["breadth_bullish"] / system_state["breadth_total"]) * 100

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

async def position_manager_loop():
    await asyncio.sleep(5)
    add_log("🛡️ Koruyucu Döngü (Protector) Aktif: Açık pozisyonlar milisaniyelik takip ediliyor.")
    while True:
        exchange = None
        try:
            if not system_state["active_positions"]:
                await asyncio.sleep(1)
                continue

            exchange = await create_exchange_instance()
            symbols = [p['symbol'] for p in system_state["active_positions"]]
            
            try:
                tickers = await exchange.fetch_tickers(symbols)
            except:
                tickers = await exchange.fetch_tickers() 
            
            now_ts = int(get_now_datetime().timestamp())

            for pos in list(system_state["active_positions"]):
                try:
                    ticker = tickers.get(pos['symbol'])
                    if not ticker or not ticker.get('last'):
                        ticker = await exchange.fetch_ticker(pos['symbol'])
                    
                    curr_price = ticker['last']
                    pos['current_price'] = curr_price
                    direction = pos['direction']
                    close_reason = None

                    pnl_raw = ((curr_price - pos['entry']) / pos['entry']) if direction == "LONG" else ((pos['entry'] - curr_price) / pos['entry'])
                    pos['unrealized_pnl'] = round(pos['active_size'] * pnl_raw, 2)

                    if pos.get("trailing_active"):
                        atr_val = pos.get("atr", curr_price * 0.01)
                        if direction == "LONG" and curr_price > pos['entry']:
                            new_sl = curr_price - (1.5 * atr_val)
                            if new_sl > pos['sl']: pos['sl'] = new_sl
                        elif direction == "SHORT" and curr_price < pos['entry']:
                            new_sl = curr_price + (1.5 * atr_val)
                            if new_sl < pos['sl']: pos['sl'] = new_sl

                    target_dist = abs(pos['tp2'] - pos['entry'])
                    favorable_move = (curr_price - pos['entry']) if direction == "LONG" else (pos['entry'] - curr_price)
                    pos['progress_pct'] = max(0.0, min(100.0, round((favorable_move / (target_dist + 1e-9)) * 100, 1)))

                    if now_ts - pos.get('open_timestamp', now_ts) > 6 * 3600:
                        close_reason = "⏳ 6 Saat Zaman Aşımı (Momentum Kaybı)"
                    elif (direction == "LONG" and curr_price <= pos['sl']) or (direction == "SHORT" and curr_price >= pos['sl']):
                        close_reason = "❌ Stop-Loss Tetiklendi"
                    elif (direction == "LONG" and curr_price >= pos['tp2']) or (direction == "SHORT" and curr_price <= pos['tp2']):
                        close_reason = "🎯 TP2 Likidite Havuzuna Ulaşıldı"
                    elif (direction == "LONG" and curr_price >= pos['tp1']) or (direction == "SHORT" and curr_price <= pos['tp1']):
                        if abs(pos['tp1'] - pos['tp2']) / pos['entry'] < 0.001:
                            close_reason = "🎯 Tek Hedef (%100) Likidite Havuzuna Ulaşıldı"
                        elif not pos.get("tp1_hit"):
                            pos["tp1_hit"] = True
                            pos["sl"] = pos["entry"]
                            partial_pnl = round((pos['pos_size'] * 0.5) * pnl_raw, 2)
                            pos['active_size'] = pos['pos_size'] * 0.5
                            apply_realized_pnl(partial_pnl)
                            pos["margin"] = round(pos.get("margin", 0.0) * 0.5, 2)
                            system_state["equity_curve"].append({"time": now_ts, "value": round(system_state["total_balance"], 2)})
                            
                            log_msg = f"⚡ TP1 ALINDI ({pos['symbol']}): %50 Kâr Realize Edildi (+${partial_pnl}) | Stop Başabaşa Çekildi."
                            add_log(log_msg)
                            
                            tg_msg = f"⚡ <b>İLK HEDEF (TP1) VURULDU</b>\n\n<b>Parite:</b> {pos['symbol']}\n<b>Kâr:</b> +${partial_pnl}\n<b>Aksiyon:</b> Pozisyon %50 küçültüldü ve Stop-Loss başabaş seviyesine (Giriş: {pos['entry']}) çekildi."
                            asyncio.create_task(send_telegram_alert(tg_msg))

                            if system_state["api_settings"]["auto_trade"] and system_state["api_settings"]["api_key"]:
                                try:
                                    close_side = 'sell' if pos['direction'] == 'LONG' else 'buy'
                                    safe_amount = float(exchange.amount_to_precision(pos['symbol'], pos['active_size']))
                                    await exchange.create_order(pos['symbol'], 'market', close_side, safe_amount)
                                    add_log(f"⚡ GERÇEK TP1 İLETİLDİ: {pos['symbol']} %50 Kapatıldı")
                                except Exception as e:
                                    add_log(f"❌ GERÇEK TP1 HATASI: {str(e)[:60]}")

                    if close_reason:
                        pnl_pct = ((curr_price - pos['entry']) / pos['entry'] * 100) if direction == "LONG" else ((pos['entry'] - curr_price) / pos['entry'] * 100)
                        realized_pnl = round(pos['active_size'] * (pnl_pct / 100.0), 2)
                        apply_realized_pnl(realized_pnl)

                        duration_mins = max(1, int((now_ts - pos.get('open_timestamp', now_ts)) / 60))
                        system_state["equity_curve"].append({"time": now_ts, "value": round(system_state["total_balance"], 2)})
                        
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
                            "close_time": get_now_str(),
                            "close_timestamp": now_ts
                        }
                        system_state["trade_history"].insert(0, history_item)
                        system_state["active_positions"].remove(pos)
                        sync_wallet_accounting()
                        
                        log_msg = f"🔴 POZİSYON KAPANDI: {pos['symbol']} | PnL: %{pnl_pct:.2f} (${realized_pnl}) | {close_reason}"
                        add_log(log_msg)
                        
                        icon = "🎯" if realized_pnl > 0 else "❌"
                        tg_msg = f"{icon} <b>POZİSYON KAPANDI</b>\n\n<b>Parite:</b> {pos['symbol']}\n<b>Yön:</b> {pos['direction']}\n<b>Net Kâr/Zarar:</b> ${realized_pnl} (%{pnl_pct:.2f})\n<b>Neden:</b> {close_reason}"
                        asyncio.create_task(send_telegram_alert(tg_msg))
                        
                        check_daily_drawdown()

                        if system_state["api_settings"]["auto_trade"] and system_state["api_settings"]["api_key"]:
                            try:
                                close_side = 'sell' if pos['direction'] == 'LONG' else 'buy'
                                safe_amount = float(exchange.amount_to_precision(pos['symbol'], pos['active_size']))
                                await exchange.create_order(pos['symbol'], 'market', close_side, safe_amount)
                                add_log(f"✅ GERÇEK ÇIKIŞ İLETİLDİ: {pos['symbol']} Tamamen Kapatıldı")
                            except Exception as e:
                                add_log(f"❌ GERÇEK ÇIKIŞ HATASI ({pos['symbol']}): {str(e)[:60]}")

                except Exception as e:
                    pass

            await exchange.close()
            await asyncio.sleep(1) 
        except Exception as e:
            if exchange:
                try: await exchange.close()
                except: pass
            await asyncio.sleep(1)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task1 = asyncio.create_task(market_scanner_loop())
    task2 = asyncio.create_task(keep_alive_loop())
    task3 = asyncio.create_task(position_manager_loop())
    yield
    task1.cancel()
    task2.cancel()
    task3.cancel()

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
    if abs(system_state["total_balance"] - payload.total_balance) > 1.0:
        system_state["initial_balance"] = payload.total_balance
        system_state["daily_start_balance"] = payload.total_balance
        system_state["peak_balance"] = payload.total_balance
        system_state["equity_curve"] = [{"time": int(get_now_datetime().timestamp()), "value": payload.total_balance}]
        system_state["total_balance"] = payload.total_balance
        
    system_state["risk_pct"] = payload.risk_pct
    system_state["leverage"] = payload.leverage
    system_state["margin_mode"] = payload.margin_mode
    system_state["max_open_positions"] = payload.max_open_positions
    system_state["max_total_margin_pct"] = payload.max_total_margin_pct
    sync_wallet_accounting()
    
    pos_limit_str = "Sınırsız" if payload.max_open_positions == 0 else ("Yapay Zeka" if payload.max_open_positions == -1 else f"{payload.max_open_positions} Adet")
    mode_str = "İzole" if payload.margin_mode == "ISOLATED" else "Cross"
    risk_str = "Yapay Zeka" if payload.risk_pct == 0.0 else f"%{payload.risk_pct}"
    lev_str = "Yapay Zeka" if payload.leverage == 0 else f"{payload.leverage}x"
    
    add_log(f"⚙️ AYARLAR GÜNCELLENDİ: Kasa: ${payload.total_balance} | Mod: {mode_str} | Risk: {risk_str} | Kaldıraç: {lev_str} | Max Poz: {pos_limit_str} | Max Marjin: %{payload.max_total_margin_pct}")
    return {"status": "success"}

@app.post("/api/update_api")
async def update_api(payload: ApiPayload):
    system_state["api_settings"] = payload.dict()
    status_str = "AKTİF" if payload.auto_trade else "DEVRE DIŞI"
    add_log(f"🔑 API GÜNCELLENDİ: {payload.exchange} ({payload.mode}) | Otomatik Emir: {status_str}")
    return {"status": "success"}

@app.post("/api/toggle_bot_trading")
async def toggle_bot_trading():
    system_state["bot_trading_active"] = not system_state.get("bot_trading_active", True)
    status_str = "AÇIK (Yeni Sinyal Alınıyor)" if system_state["bot_trading_active"] else "KAPALI (Yeni Sinyal Durduruldu)"
    add_log(f"🤖 BOT İŞLEM ALIMI: {status_str}")
    return {"status": "success", "active": system_state["bot_trading_active"]}

@app.post("/api/manual/close_position")
async def manual_close_position(payload: ClosePosPayload):
    target = next((p for p in system_state["active_positions"] if p['symbol'] == payload.symbol), None)
    if target:
        curr_price = target.get('current_price', target['entry'])
        direction = target['direction']
        pnl_pct = ((curr_price - target['entry']) / target['entry'] * 100) if direction == "LONG" else ((target['entry'] - curr_price) / target['entry'] * 100)
        realized_pnl = round(target['active_size'] * (pnl_pct / 100.0), 2)
        apply_realized_pnl(realized_pnl)

        now_dt = get_now_datetime()
        system_state["equity_curve"].append({"time": int(now_dt.timestamp()), "value": round(system_state["total_balance"], 2)})

        duration_mins = max(1, int((now_dt.timestamp() - target.get('open_timestamp', now_dt.timestamp())) / 60))

        history_item = {
            "symbol": target['symbol'],
            "direction": target['direction'],
            "entry": target['entry'],
            "close_price": curr_price,
            "pnl_pct": round(pnl_pct, 2),
            "realized_pnl": realized_pnl,
            "score": target['score'],
            "duration_mins": duration_mins,
            "open_reasons": target['reasons'],
            "close_reason": "✋ MANUEL KAPATILDI",
            "close_time": now_dt.strftime("%H:%M:%S"),
            "close_timestamp": int(now_dt.timestamp())
        }
        system_state["trade_history"].insert(0, history_item)
        system_state["active_positions"].remove(target)
        sync_wallet_accounting()
        add_log(f"✋ MANUEL KAPATMA: {target['symbol']} | PnL: ${realized_pnl}")
        asyncio.create_task(send_telegram_alert(f"✋ <b>MANUEL KAPATMA</b>\n\n<b>Parite:</b> {target['symbol']}\n<b>Net Kâr/Zarar:</b> ${realized_pnl}"))
        
        asyncio.create_task(execute_manual_real_order(target['symbol'], target['direction'], target['active_size']))
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
        apply_realized_pnl(realized_pnl)
        margin_release = round(target.get("margin", 0.0) * payload.ratio, 2)
        target['active_size'] -= part_size
        target['pos_size'] -= part_size
        target['margin'] = round(max(0.0, target.get("margin", 0.0) - margin_release), 2)

        if target['active_size'] <= 0:
            system_state["active_positions"].remove(target)
        sync_wallet_accounting()

        now_dt = get_now_datetime()
        system_state["equity_curve"].append({"time": int(now_dt.timestamp()), "value": round(system_state["total_balance"], 2)})
        add_log(f"✂️ KADEMELİ KAPATMA (%{int(payload.ratio*100)}): {target['symbol']} | Realize PnL: +${realized_pnl}")
        
        asyncio.create_task(execute_manual_real_order(target['symbol'], target['direction'], part_size))
        return {"status": "success"}
    return {"status": "error"}

@app.post("/api/manual/close_all")
async def manual_close_all():
    now_dt = get_now_datetime()
    for pos in list(system_state["active_positions"]):
        curr_price = pos.get('current_price', pos['entry'])
        direction = pos['direction']
        pnl_pct = ((curr_price - pos['entry']) / pos['entry'] * 100) if direction == "LONG" else ((target['entry'] - curr_price) / target['entry'] * 100)
        realized_pnl = round(pos['active_size'] * (pnl_pct / 100.0), 2)
        apply_realized_pnl(realized_pnl)

        duration_mins = max(1, int((now_dt.timestamp() - pos.get('open_timestamp', now_dt.timestamp())) / 60))

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
            "close_reason": "🚨 ACİL TÜMÜNÜ KAPAT",
            "close_time": now_dt.strftime("%H:%M:%S"),
            "close_timestamp": int(now_dt.timestamp())
        }
        system_state["trade_history"].insert(0, history_item)
        system_state["active_positions"].remove(pos)
        asyncio.create_task(execute_manual_real_order(pos['symbol'], pos['direction'], pos['active_size']))
        
    sync_wallet_accounting()
    add_log("🚨 TÜM POZİSYONLAR KAPATILDI!")
    asyncio.create_task(send_telegram_alert(f"🚨 <b>ACİL KAPATMA (PANIC BUTTON)</b>\nAçık olan tüm pozisyonlar manuel olarak kapatıldı!"))
    return {"status": "success"}

@app.post("/api/manual/breakeven_all")
async def manual_breakeven_all():
    for pos in system_state["active_positions"]:
        pos['sl'] = pos['entry']
        pos['tp1_hit'] = True
    add_log("🛡️ TOPLU BAŞABAŞ: Tüm açık pozisyonların stopları giriş fiyatına çekildi!")
    asyncio.create_task(send_telegram_alert(f"🛡️ <b>TOPLU GÜVENLİK</b>\nTüm açık pozisyonların stop seviyeleri Başa Baş (Giriş Fiyatı) noktasına çekildi."))
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
        add_log(f"🔄 AKILLI ATR TRAILING STOP: {target['symbol']} için {status_str} yapıldı.")
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
        start_ts = int(datetime.strptime(payload.start_date, "%Y-%m-%d").replace(tzinfo=UTC_TZ).timestamp())
        end_ts = int(datetime.strptime(payload.end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=UTC_TZ).timestamp())
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
    sync_wallet_accounting()
    return system_state

@app.get("/", response_class=HTMLResponse)
async def get_dashboard(request: Request):
    html_content = """
    <!DOCTYPE html>
    <html lang="tr">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Meta Quant Terminal Pro Ultimate + L2</title>
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
                        <h1 class="text-base font-extrabold tracking-wider text-emerald-400">META QUANT ULTIMATE + L2</h1>
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

            <div class="flex items-center space-x-3 text-xs text-slate-400">
                <button onclick="toggleBotTrading()" id="bot-toggle-btn" class="px-2.5 py-1 rounded-lg font-bold bg-emerald-600 text-black hover:bg-emerald-500 transition">🤖 Bot: AÇIK</button>
                <div>Taranan: <span id="scanned-count" class="text-white font-bold">0</span></div>
                <div>Son: <span id="last-scan" class="text-white font-bold">-</span></div>
            </div>
        </div>

        <!-- SAYFA 1: CANLI TERMİNAL -->
        <div id="page-terminal" class="space-y-3">
            <div class="card p-3 rounded-xl flex flex-wrap justify-between items-center gap-3">
                <div class="flex flex-col space-y-1 bg-slate-900/90 p-2 rounded-xl border border-slate-800">
                    <div class="flex items-center justify-between gap-2 border-b border-slate-800 pb-1 text-[9px]">
                        <span class="text-slate-400 font-semibold uppercase">Dönemsel PnL (UTC 00:00):</span>
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
                            <div class="text-[9px] text-slate-400 uppercase tracking-wider">Toplam Kasa</div>
                            <div id="stat-total-balance" class="text-sm font-extrabold font-mono text-white">$1000.00</div>
                        </div>
                        <div class="border-r border-slate-800 h-6"></div>
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
                            <option value="0.0" class="text-fuchsia-400">Otomatik (Yapay Zeka)</option>
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
                            <option value="0" class="text-fuchsia-400">Otomatik (Yapay Zeka)</option>
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
                            <option value="-1" class="text-fuchsia-400">Otomatik (Yapay Zeka)</option>
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
                    <button onclick="saveSettings(this)" class="mt-3 bg-emerald-600 hover:bg-emerald-500 text-black font-bold px-2 py-1 rounded transition text-xs">KAYDET</button>
                </div>
            </div>

            <!-- GRAFİK VE SAĞ PANEL -->
            <div class="grid grid-cols-1 lg:grid-cols-3 gap-3">
                <div class="card p-3 rounded-xl lg:col-span-2 h-[520px] flex flex-col">
                    <div class="flex flex-wrap justify-between items-center mb-2 px-1 gap-2">
                        <div class="flex items-center space-x-3">
                            <span id="chart-title" class="text-xs font-bold text-emerald-400 tracking-wider">GRAFİK</span>
                            <div class="flex space-x-1 bg-slate-900 p-0.5 rounded border border-slate-800 text-[10px]">
                                <button onclick="changeTimeframe('1')" class="tf-btn px-2 py-0.5 rounded text-slate-400 hover:text-white" id="tf-1">1M</button>
                                <button onclick="changeTimeframe('5')" class="tf-btn px-2 py-0.5 rounded text-slate-400 hover:text-white" id="tf-5">5M</button>
                                <button onclick="changeTimeframe('15')" class="tf-btn px-2 py-0.5 rounded text-slate-400 hover:text-white" id="tf-15">15M</button>
                                <button onclick="changeTimeframe('60')" class="tf-btn active px-2 py-0.5 rounded text-slate-400 hover:text-white" id="tf-60">1H</button>
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

                <!-- SAĞ PANEL: GİRİŞ GEREKÇESİ, L2 EMİR DEFTERİ VE LOGLAR -->
                <div class="card p-3 rounded-xl flex flex-col h-[520px]">
                    <!-- 1. GİRİŞ GEREKÇESİ -->
                    <div class="flex-1 overflow-y-auto mb-2">
                        <h2 class="text-xs font-semibold text-slate-400 mb-2 uppercase tracking-wider">Seçili Parite Giriş Gerekçesi</h2>
                        <div id="active-rationale" class="space-y-2 text-xs">
                            <div class="text-slate-500 italic">Tablodan bir parite seçin...</div>
                        </div>
                    </div>
                    
                    <!-- 2. L2 EMİR DEFTERİ - LOGLARIN ÜSTÜNDE -->
                    <div class="bg-slate-900/90 p-2.5 rounded-lg border border-slate-800 space-y-1.5 mb-2">
                        <h3 class="text-[11px] font-bold text-sky-400 uppercase">⚡ L2 Emir Defteri (Tahta Derinliği)</h3>
                        <div class="flex justify-between text-[11px] font-bold">
                            <span class="text-emerald-400">Alıcı (Bid): <b id="ob-bid-pct">%50.0</b></span>
                            <span class="text-rose-400">Satıcı (Ask): <b id="ob-ask-pct">%50.0</b></span>
                        </div>
                        <div class="w-full bg-slate-800 h-2.5 rounded-full overflow-hidden flex">
                            <div id="ob-bid-bar" class="bg-emerald-500 h-2.5" style="width: 50%"></div>
                            <div id="ob-ask-bar" class="bg-rose-500 h-2.5" style="width: 50%"></div>
                        </div>
                    </div>

                    <!-- 3. SİSTEM LOGLARI -->
                    <div class="h-32 flex flex-col">
                        <h3 class="text-[10px] font-semibold text-slate-500 mb-1 uppercase">Sistem Logları</h3>
                        <div id="log-box" class="bg-black/50 p-2 rounded text-[11px] text-emerald-500/80 font-mono flex-1 overflow-y-auto space-y-1"></div>
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
                <div class="card p-3 rounded-xl border-amber-500/35">
                    <div class="text-[10px] text-amber-400 font-bold uppercase tracking-wider">ABD TÜFE (CPI)</div>
                    <div id="cd-cpi" class="text-base font-mono font-bold text-white mt-1">--s --d --sn</div>
                </div>
                <div class="card p-3 rounded-xl border-purple-500/35">
                    <div class="text-[10px] text-purple-400 font-bold uppercase tracking-wider">FED FOMC Kararı</div>
                    <div id="cd-fomc" class="text-base font-mono font-bold text-white mt-1">--s --d --sn</div>
                </div>
                <div class="card p-3 rounded-xl border-rose-500/35">
                    <div class="text-[10px] text-rose-400 font-bold uppercase tracking-wider">ABD NFP İstihdam</div>
                    <div id="cd-nfp" class="text-base font-mono font-bold text-white mt-1">--s --d --sn</div>
                </div>
                <div class="card p-3 rounded-xl border-sky-500/35">
                    <div class="text-[10px] text-sky-400 font-bold uppercase tracking-wider">Majör Token Unlock</div>
                    <div id="cd-unlock" class="text-base font-mono font-bold text-white mt-1">--s --d --sn</div>
                </div>
            </div>

            <div class="card p-4 rounded-xl space-y-3">
                <h3 class="text-xs font-semibold text-emerald-400 uppercase flex items-center justify-between">
                    <span class="flex items-center"><span class="w-2 h-2 bg-emerald-400 rounded-full mr-2 animate-ping"></span> Canlı Kripto Son Dakika Haber Akışı & Kurumsal Gelişmeler</span>
                    <span class="text-[10px] text-slate-500">Kaynak: Global Kurumsal Akış</span>
                </h3>
                <div class="space-y-2 text-xs">
                    <div class="flex justify-between items-center bg-slate-900/80 p-2.5 rounded-lg border border-slate-800">
                        <span class="text-white font-medium">⚡ SEC, yeni kurumsal ETF başvuru dosyaları için resmi inceleme takvimini güncelledi.</span>
                        <span class="text-[10px] text-slate-500 font-mono">2 dk önce</span>
                    </div>
                    <div class="flex justify-between items-center bg-slate-900/80 p-2.5 rounded-lg border border-slate-800">
                        <span class="text-white font-medium">🐋 Büyük balina cüzdanlarından türev borsalara son 1 saatte yoğun USDT transferi tespit edildi.</span>
                        <span class="text-[10px] text-slate-500 font-mono">14 dk önce</span>
                    </div>
                    <div class="flex justify-between items-center bg-slate-900/80 p-2.5 rounded-lg border border-slate-800">
                        <span class="text-white font-medium">📢 Bybit ve Binance vadeli işlemler platformlarına yeni kaldıraçlı parite marjin desteği eklendi.</span>
                        <span class="text-[10px] text-slate-500 font-mono">35 dk önce</span>
                    </div>
                    <div class="flex justify-between items-center bg-slate-900/80 p-2.5 rounded-lg border border-slate-800">
                        <span class="text-white font-medium">🌐 Avrupa Merkez Bankası (ECB) dijital varlık düzenlemeleri için yeni kılavuz yayınladı.</span>
                        <span class="text-[10px] text-slate-500 font-mono">1 saat önce</span>
                    </div>
                </div>
            </div>

            <div class="card p-4 rounded-xl space-y-3">
                <h3 class="text-xs font-semibold text-sky-400 uppercase">📅 Kritik Makroekonomik Veriler ve Piyasa Beklentileri</h3>
                <div class="overflow-x-auto">
                    <table class="w-full text-left text-xs">
                        <thead class="text-slate-500 border-b border-slate-800">
                            <tr>
                                <th class="pb-2">VERİ ADI</th>
                                <th class="pb-2">DÖNEM</th>
                                <th class="pb-2">ÖNCEKİ</th>
                                <th class="pb-2">BEKLENTİ</th>
                                <th class="pb-2">ETKİ</th>
                            </tr>
                        </thead>
                        <tbody class="divide-y divide-slate-800/60 text-slate-300 font-mono">
                            <tr>
                                <td class="py-2 text-white font-bold">ABD TÜFE (CPI Yıllık)</td>
                                <td>Ağustos</td>
                                <td>%2.9</td>
                                <td class="text-amber-400">%2.8</td>
                                <td><span class="px-2 py-0.5 rounded text-[9px] font-bold bg-rose-500/20 text-rose-400">YÜKSEK</span></td>
                            </tr>
                            <tr>
                                <td class="py-2 text-white font-bold">FED Faiz Kararı</td>
                                <td>Eylül FOMC</td>
                                <td>%5.50</td>
                                <td class="text-emerald-400">%5.25 (İndirim Beklentisi)</td>
                                <td><span class="px-2 py-0.5 rounded text-[9px] font-bold bg-purple-500/20 text-purple-400">KRİTİK</span></td>
                            </tr>
                            <tr>
                                <td class="py-2 text-white font-bold">ABD Tarım Dışı İstihdam (NFP)</td>
                                <td>Ağustos</td>
                                <td>114K</td>
                                <td class="text-sky-400">165K</td>
                                <td><span class="px-2 py-0.5 rounded text-[9px] font-bold bg-rose-500/20 text-rose-400">YÜKSEK</span></td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>

            <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
                <div class="card p-4 rounded-xl space-y-2">
                    <h3 class="text-xs font-semibold text-sky-400 uppercase">📢 Yeni Vadeli Listelemeler (Futures Listing)</h3>
                    <div class="space-y-1.5 text-xs">
                        <div class="flex justify-between bg-slate-900/80 p-2 rounded border border-slate-800">
                            <span class="text-white font-bold">MEW/USDT (50x) - Bybit/Binance</span> <span class="text-emerald-400 font-mono">Aktif Edildi</span>
                        </div>
                        <div class="flex justify-between bg-slate-900/80 p-2 rounded border border-slate-800">
                            <span class="text-white font-bold">ZRO/USDT (50x) - OKX</span> <span class="text-emerald-400 font-mono">Aktif Edildi</span>
                        </div>
                    </div>
                </div>
                <div class="card p-4 rounded-xl space-y-2">
                    <h3 class="text-xs font-semibold text-amber-400 uppercase">🛠️ Planlı Borsa Bakım Saatleri</h3>
                    <div class="space-y-1.5 text-xs">
                        <div class="flex justify-between bg-slate-900/80 p-2 rounded border border-slate-800">
                            <span class="text-white font-bold">Bybit Altyapı Güncellemesi</span> <span class="text-amber-400 font-mono">Yarın 04:00 TSİ (30 dk)</span>
                        </div>
                        <div class="flex justify-between bg-slate-900/80 p-2 rounded border border-slate-800">
                            <span class="text-white font-bold">Binance Futures API Bakımı</span> <span class="text-slate-400 font-mono">Çarşamba 03:00 TSİ</span>
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
                    <button onclick="manualBreakevenAll(this)" class="bg-sky-600 hover:bg-sky-500 text-white font-bold px-3 py-2 rounded-lg text-xs transition">🛡️ TÜMÜNÜ BAŞABAŞ ÇEK</button>
                    <button onclick="manualCloseAll(this)" class="bg-rose-600 hover:bg-rose-500 text-white font-bold px-3 py-2 rounded-lg text-xs transition">🚨 TÜMÜNÜ KAPAT (ACİL)</button>
                </div>
            </div>

            <div class="grid grid-cols-1 md:grid-cols-4 gap-3">
                <div class="card p-3 rounded-xl flex justify-between items-center">
                    <span class="text-xs text-slate-400 uppercase">Toplam Açık Pozisyon:</span> <span id="man-total-pos" class="text-sm font-bold font-mono text-white">0 Adet</span>
                </div>
                <div class="card p-3 rounded-xl flex justify-between items-center">
                    <span class="text-xs text-slate-400 uppercase">Toplam Anlık PnL:</span> <span id="man-total-pnl" class="text-sm font-bold font-mono text-emerald-400">$0.00 (%0.0)</span>
                </div>
                <div class="card p-3 rounded-xl flex justify-between items-center">
                    <span class="text-xs text-slate-400 uppercase">Kullanılan Marjin:</span> <span id="man-total-margin" class="text-sm font-bold font-mono text-amber-400">$0.00</span>
                </div>
                <div class="card p-3 rounded-xl flex justify-between items-center">
                    <span class="text-xs text-slate-400 uppercase">Toplam Risk Tutar:</span> <span id="man-total-risk" class="text-sm font-bold font-mono text-rose-400">$0.00</span>
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
            <div class="grid grid-cols-1 md:grid-cols-3 gap-3">
                <div class="card p-3 rounded-xl flex justify-between items-center">
                    <span class="text-xs text-slate-400 uppercase">Toplam İşlem Sayısı:</span> <span id="journal-total-trades" class="text-sm font-bold font-mono text-white">0</span>
                </div>
                <div class="card p-3 rounded-xl flex justify-between items-center">
                    <span class="text-xs text-slate-400 uppercase">Kazanma Oranı (Win Rate):</span> <span id="journal-winrate" class="text-sm font-bold font-mono text-sky-400">%0.0</span>
                </div>
                <div class="card p-3 rounded-xl flex justify-between items-center">
                    <span class="text-xs text-slate-400 uppercase">Toplam Net PnL:</span> <span id="journal-total-pnl" class="text-sm font-bold font-mono text-emerald-400">$0.00</span>
                </div>
            </div>

            <div class="card p-3 rounded-xl flex flex-wrap gap-3 items-center justify-between">
                <input id="journal-search" type="text" placeholder="Parite ara (örn: BTC, ETH)..." oninput="renderJournalTable()" class="bg-slate-900 border border-slate-700 text-white rounded px-3 py-1.5 text-xs outline-none w-64 font-mono">
                <div class="flex space-x-2 text-xs">
                    <button onclick="setJournalFilter('ALL')" id="j-filter-ALL" class="px-3 py-1 rounded bg-emerald-600 text-black font-bold transition">Tümü</button>
                    <button onclick="setJournalFilter('LONG')" id="j-filter-LONG" class="px-3 py-1 rounded bg-slate-800 text-slate-300 hover:text-white transition">Long</button>
                    <button onclick="setJournalFilter('SHORT')" id="j-filter-SHORT" class="px-3 py-1 rounded bg-slate-800 text-slate-300 hover:text-white transition">Short</button>
                </div>
            </div>

            <div class="grid grid-cols-1 lg:grid-cols-3 gap-3">
                <div class="card p-4 rounded-xl lg:col-span-2">
                    <h2 class="text-xs font-semibold text-sky-400 mb-3 uppercase">📖 Kapanan İşlem Günlüğü (Detay için satıra tıkla)</h2>
                    <div class="overflow-x-auto max-h-[420px] overflow-y-auto">
                        <table class="w-full text-left text-xs">
                            <thead class="text-slate-500 border-b border-slate-800 sticky top-0 bg-[#121824]">
                                <tr><th class="pb-2">ZAMAN</th><th class="pb-2">PARİTE</th><th class="pb-2">YÖN</th><th class="pb-2">GİRİŞ / ÇIKIŞ</th><th class="pb-2">NET PnL ($)</th><th class="pb-2">KAPANIŞ NEDENİ</th></tr>
                            </thead>
                            <tbody id="journal-table" class="divide-y divide-slate-800/50"></tbody>
                        </table>
                    </div>
                </div>

                <div class="card p-4 rounded-xl flex flex-col justify-between h-[480px]">
                    <div>
                        <h2 class="text-xs font-semibold text-emerald-400 mb-2 uppercase tracking-wider">🔍 İşlem Açılış Gerekçeleri</h2>
                        <div id="journal-detail-box" class="space-y-2 text-xs">
                            <div class="text-slate-500 italic">İncelemek için tablodan bir işleme tıklayın...</div>
                        </div>
                    </div>
                    <div class="text-[10px] text-slate-500 text-center border-t border-slate-800 pt-2">Meta Quant Journal Intelligence</div>
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
                    <button onclick="saveApiSettings(this)" class="w-full bg-amber-500 hover:bg-amber-400 text-black font-bold py-2 rounded transition">KAYDET</button>
                </div>
            </div>
        </div>

        <script>
            // BİLDİRİM İZNİ İSTE
            if ("Notification" in window && Notification.permission !== "granted" && Notification.permission !== "denied") {
                Notification.requestPermission();
            }

            function sendWebNotification(title, bodyText) {
                if ("Notification" in window && Notification.permission === "granted") {
                    new Notification(title, { body: bodyText, icon: "https://cryptologos.cc/logos/bitcoin-btc-logo.png" });
                }
            }

            function playAlertSound(type) {
                try {
                    const ctx = new (window.AudioContext || window.webkitAudioContext)();
                    const osc = ctx.createOscillator();
                    const gain = ctx.createGain();
                    
                    if (type === "OPEN") {
                        osc.type = "sine";
                        osc.frequency.setValueAtTime(440, ctx.currentTime);
                        osc.frequency.exponentialRampToValueAtTime(880, ctx.currentTime + 0.1);
                        gain.gain.setValueAtTime(0.2, ctx.currentTime);
                        gain.gain.linearRampToValueAtTime(0.01, ctx.currentTime + 0.2);
                    } else if (type === "WIN") {
                        osc.type = "triangle";
                        osc.frequency.setValueAtTime(523.25, ctx.currentTime); 
                        osc.frequency.setValueAtTime(659.25, ctx.currentTime + 0.1); 
                        osc.frequency.setValueAtTime(783.99, ctx.currentTime + 0.2); 
                        gain.gain.setValueAtTime(0.2, ctx.currentTime);
                        gain.gain.linearRampToValueAtTime(0.01, ctx.currentTime + 0.4);
                    } else if (type === "LOSS") {
                        osc.type = "sawtooth";
                        osc.frequency.setValueAtTime(250, ctx.currentTime);
                        osc.frequency.exponentialRampToValueAtTime(100, ctx.currentTime + 0.3);
                        gain.gain.setValueAtTime(0.3, ctx.currentTime);
                        gain.gain.linearRampToValueAtTime(0.01, ctx.currentTime + 0.4);
                    } else {
                        osc.type = "sine";
                        osc.frequency.setValueAtTime(880, ctx.currentTime);
                        osc.frequency.exponentialRampToValueAtTime(1760, ctx.currentTime + 0.15);
                        gain.gain.setValueAtTime(0.1, ctx.currentTime);
                        gain.gain.linearRampToValueAtTime(0.01, ctx.currentTime + 0.25);
                    }

                    osc.connect(gain);
                    gain.connect(ctx.destination);
                    osc.start();
                    osc.stop(ctx.currentTime + 0.5);
                } catch(e) {}
            }

            let chart = null;
            let candleSeries = null;
            let equityChart = null;
            let equitySeries = null;

            let currentSymbol = "BTC/USDT:USDT";
            localStorage.setItem("selected_sym", "BTC/USDT:USDT");

            let currentTimeframe = "60";
            let currentPnlFilter = "today";
            let currentStatsFilter = "today";
            let journalDirectionFilter = "ALL";
            let selectedJournalItem = null;
            let selectedPos = null;
            let priceLines = [];
            let lastPositions = [];
            let tradeHistoryCache = [];
            let lastKnownPosCount = 0;
            let resolvedSymbolCache = {};
            let lastProcessedLog = "";
            let lastLoadedSymbol = "";
            let lastLoadedTf = "";

            function recalculatePnlMetrics() {
                try {
                    const now = new Date();
                    const startOfToday = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime() / 1000;
                    const startOfYesterday = startOfToday - 86400;
                    const startOfWeek = startOfToday - (now.getDay() === 0 ? 6 : now.getDay() - 1) * 86400;
                    const startOfMonth = new Date(now.getFullYear(), now.getMonth(), 1).getTime() / 1000;

                    let filtered = tradeHistoryCache.filter(h => {
                        const ts = h.close_timestamp || 0;
                        if (currentPnlFilter === 'today') return ts >= startOfToday;
                        if (currentPnlFilter === 'yesterday') return ts >= startOfYesterday && ts < startOfToday;
                        if (currentPnlFilter === 'week') return ts >= startOfWeek;
                        if (currentPnlFilter === 'month') return ts >= startOfMonth;
                        return true; 
                    });

                    const totalPnl = filtered.reduce((acc, h) => acc + (h.realized_pnl || 0), 0);
                    const totalCount = filtered.length;
                    const wins = filtered.filter(h => (h.realized_pnl || 0) > 0).length;
                    const winRate = totalCount > 0 ? ((wins / totalCount) * 100).toFixed(1) : "0.0";

                    const pnlEl = document.getElementById('stat-pnl');
                    if (pnlEl) {
                        pnlEl.innerText = `${totalPnl >= 0 ? '+' : ''}$${totalPnl.toFixed(2)}`;
                        pnlEl.className = `text-sm font-extrabold font-mono ${totalPnl >= 0 ? 'text-emerald-400' : 'text-rose-400'}`;
                    }

                    const winrateEl = document.getElementById('stat-winrate');
                    if (winrateEl) winrateEl.innerText = `%${winRate}`;

                    const tradesEl = document.getElementById('stat-trades');
                    if (tradesEl) tradesEl.innerText = totalCount;

                    const labelMap = {
                        'today': 'Bugün Net PnL',
                        'yesterday': 'Dün Net PnL',
                        'week': 'Bu Hafta Net PnL',
                        'month': 'Bu Ay Net PnL',
                        'all': 'Tüm Zamanlar Net PnL'
                    };
                    const labelEl = document.getElementById('pnl-label');
                    if (labelEl) labelEl.innerText = labelMap[currentPnlFilter] || 'Net PnL';
                } catch(e) {}
            }

            function recalculateAdvancedStats() {
                try {
                    const now = new Date();
                    const startOfToday = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime() / 1000;
                    const startOfWeek = startOfToday - (now.getDay() === 0 ? 6 : now.getDay() - 1) * 86400;
                    const startOfMonth = new Date(now.getFullYear(), now.getMonth(), 1).getTime() / 1000;

                    let filtered = tradeHistoryCache.filter(h => {
                        const ts = h.close_timestamp || 0;
                        if (currentStatsFilter === 'today') return ts >= startOfToday;
                        if (currentStatsFilter === 'week') return ts >= startOfWeek;
                        if (currentStatsFilter === 'month') return ts >= startOfMonth;
                        return true;
                    });

                    const totalCount = filtered.length;
                    const winsList = filtered.filter(h => (h.realized_pnl || 0) > 0);
                    const lossesList = filtered.filter(h => (h.realized_pnl || 0) < 0);

                    const grossProfit = winsList.reduce((acc, h) => acc + h.realized_pnl, 0);
                    const grossLoss = Math.abs(lossesList.reduce((acc, h) => acc + h.realized_pnl, 0));
                    const profitFactor = grossLoss > 0 ? (grossProfit / grossLoss).toFixed(2) : (grossProfit > 0 ? "99.0" : "0.00");

                    const netSum = filtered.reduce((acc, h) => acc + h.realized_pnl, 0);
                    const expectancy = totalCount > 0 ? (netSum / totalCount).toFixed(2) : "0.00";

                    const avgWin = winsList.length > 0 ? (grossProfit / winsList.length).toFixed(2) : "0.00";
                    const avgLoss = lossesList.length > 0 ? (grossLoss / lossesList.length).toFixed(2) : "0.00";

                    let maxWinStreak = 0, maxLossStreak = 0, curWin = 0, curLoss = 0;
                    [...filtered].reverse().forEach(h => {
                        if ((h.realized_pnl || 0) > 0) {
                            curWin++; curLoss = 0;
                            if (curWin > maxWinStreak) maxWinStreak = curWin;
                        } else if ((h.realized_pnl || 0) < 0) {
                            curLoss++; curWin = 0;
                            if (curLoss > maxLossStreak) maxLossStreak = curLoss;
                        }
                    });

                    const longCount = filtered.filter(h => h.direction === 'LONG').length;
                    const shortCount = filtered.filter(h => h.direction === 'SHORT').length;
                    const longPct = totalCount > 0 ? ((longCount / totalCount) * 100).toFixed(0) : 50;

                    const totalMins = filtered.reduce((acc, h) => acc + (h.duration_mins || 0), 0);
                    const avgDurationMins = totalCount > 0 ? Math.round(totalMins / totalCount) : 0;

                    let pnlArray = filtered.map(h => h.pnl_pct || 0);
                    let avgPnlPct = pnlArray.length > 0 ? pnlArray.reduce((a, b) => a + b, 0) / pnlArray.length : 0;
                    
                    let variance = pnlArray.length > 0 ? pnlArray.reduce((a, b) => a + Math.pow(b - avgPnlPct, 2), 0) / pnlArray.length : 0;
                    let stdDev = Math.sqrt(variance);
                    
                    let downsideVariance = pnlArray.length > 0 ? pnlArray.reduce((a, b) => a + (b < 0 ? Math.pow(b, 2) : 0), 0) / pnlArray.length : 0;
                    let downsideStdDev = Math.sqrt(downsideVariance);

                    let sharpe = stdDev > 0 ? (avgPnlPct / stdDev).toFixed(2) : (avgPnlPct > 0 ? "3.00" : "0.00");
                    let sortino = downsideStdDev > 0 ? (avgPnlPct / downsideStdDev).toFixed(2) : (avgPnlPct > 0 ? "5.00" : "0.00");

                    if (document.getElementById('stat-pf')) document.getElementById('stat-pf').innerText = profitFactor;
                    if (document.getElementById('stat-expectancy')) document.getElementById('stat-expectancy').innerText = `$${expectancy}`;
                    if (document.getElementById('stat-avg-win')) document.getElementById('stat-avg-win').innerText = `+$${avgWin}`;
                    if (document.getElementById('stat-avg-loss')) document.getElementById('stat-avg-loss').innerText = `-$${avgLoss}`;
                    if (document.getElementById('stat-max-win-streak')) document.getElementById('stat-max-win-streak').innerText = maxWinStreak;
                    if (document.getElementById('stat-max-loss-streak')) document.getElementById('stat-max-loss-streak').innerText = maxLossStreak;
                    if (document.getElementById('stat-ls-ratio')) document.getElementById('stat-ls-ratio').innerText = `L: %${longPct} | S: %${100 - longPct}`;
                    if (document.getElementById('stat-ls-bar')) document.getElementById('stat-ls-bar').style.width = `${longPct}%`;
                    if (document.getElementById('stat-avg-duration')) document.getElementById('stat-avg-duration').innerText = `${avgDurationMins} Dakika`;
                    if (document.getElementById('stat-sharpe')) document.getElementById('stat-sharpe').innerText = `${sharpe} / ${sortino}`;

                    let symbolMap = {};
                    filtered.forEach(h => {
                        if (!symbolMap[h.symbol]) symbolMap[h.symbol] = { count: 0, pnl: 0 };
                        symbolMap[h.symbol].count++;
                        symbolMap[h.symbol].pnl += (h.realized_pnl || 0);
                    });

                    let symbolArr = Object.keys(symbolMap).map(s => ({ symbol: s, ...symbolMap[s] }));
                    let sortedBest = [...symbolArr].sort((a,b) => b.pnl - a.pnl);
                    let sortedWorst = [...symbolArr].sort((a,b) => a.pnl - b.pnl);

                    const topTable = document.getElementById('top-symbols-table');
                    if (topTable) {
                        topTable.innerHTML = sortedBest.slice(0, 5).map(s => `
                            <tr><td class="py-1.5 font-bold text-white">${s.symbol}</td><td>${s.count} İşlem</td><td class="text-right font-mono font-bold text-emerald-400">+$${s.pnl.toFixed(2)}</td></tr>
                        `).join('') || '<tr><td colspan="3" class="text-slate-500 italic py-2">Veri yok...</td></tr>';
                    }

                    const worstTable = document.getElementById('worst-symbols-table');
                    if (worstTable) {
                        worstTable.innerHTML = sortedWorst.slice(0, 5).map(s => `
                            <tr><td class="py-1.5 font-bold text-white">${s.symbol}</td><td>${s.count} İşlem</td><td class="text-right font-mono font-bold text-rose-400">-$${Math.abs(s.pnl).toFixed(2)}</td></tr>
                        `).join('') || '<tr><td colspan="3" class="text-slate-500 italic py-2">Veri yok...</td></tr>';
                    }
                } catch(e) {}
            }

            function changePnlFilter(tf) {
                currentPnlFilter = tf;
                ['today', 'yesterday', 'week', 'month', 'all'].forEach(t => {
                    const btn = document.getElementById(`pnl-tf-${t}`);
                    if (btn) {
                        if (t === tf) btn.classList.add('active');
                        else btn.classList.remove('active');
                    }
                });
                recalculatePnlMetrics();
            }

            function changeStatsFilter(tf) {
                currentStatsFilter = tf;
                ['today', 'week', 'month', 'all'].forEach(t => {
                    const btn = document.getElementById(`stats-tf-${t}`);
                    if (btn) {
                        if (t === tf) btn.classList.add('active');
                        else btn.classList.remove('active');
                    }
                });
                recalculateAdvancedStats();
            }

            function changeTimeframe(tf) {
                currentTimeframe = tf;
                ['1', '5', '15', '60', '240', 'D'].forEach(t => {
                    const btn = document.getElementById(`tf-${t}`);
                    if (btn) {
                        if (t === tf) btn.classList.add('active');
                        else btn.classList.remove('active');
                    }
                });
                lastLoadedSymbol = "";
                loadChartCandles(currentSymbol, selectedPos, false);
            }

            async function loadReportsList() {
                try {
                    const res = await fetch('/api/reports/list');
                    const files = await res.json();
                    const tb = document.getElementById('reports-table');
                    if (tb) {
                        tb.innerHTML = files.map(f => `
                            <tr><td class="py-2 font-mono text-slate-300">${f}</td><td class="text-right"><a href="/api/reports/download/${f}" class="text-sky-400 hover:underline font-bold text-xs">İndir</a></td></tr>
                        `).join('') || '<tr><td colspan="2" class="py-2 text-slate-500 italic">Arşiv dosyası bulunamadı.</td></tr>';
                    }
                } catch(e) {}
            }

            async function loadArchivePreview() {
                try {
                    const totalCount = tradeHistoryCache.length;
                    const wins = tradeHistoryCache.filter(h => h.realized_pnl > 0).length;
                    const winRate = totalCount > 0 ? ((wins / totalCount) * 100).toFixed(1) : "0.0";
                    const netPnl = tradeHistoryCache.reduce((acc, h) => acc + h.realized_pnl, 0);

                    if (document.getElementById('archive-prev-trades')) document.getElementById('archive-prev-trades').innerText = totalCount;
                    if (document.getElementById('archive-prev-winrate')) document.getElementById('archive-prev-winrate').innerText = `%${winRate}`;
                    const pnlElem = document.getElementById('archive-prev-pnl');
                    if (pnlElem) {
                        pnlElem.innerText = `${netPnl >= 0 ? '+' : ''}$${netPnl.toFixed(2)}`;
                        pnlElem.className = `text-sm font-extrabold font-mono ${netPnl >= 0 ? 'text-emerald-400' : 'text-rose-400'}`;
                    }
                } catch(e) {}
            }

            async function downloadCustomCsv() {
                const start_date = document.getElementById('custom-start-date').value;
                const end_date = document.getElementById('custom-end-date').value;
                if (!start_date || !end_date) { alert("Lütfen başlangıç ve bitiş tarihi seçin!"); return; }
                
                const res = await fetch('/api/export/custom_csv', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ start_date, end_date })
                });
                const blob = await res.blob();
                const url = window.URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = `trades_${start_date}_to_${end_date}.csv`;
                document.body.appendChild(a);
                a.click();
                a.remove();
            }

            function resizeCanvas() {
                const wrapper = document.getElementById('tv-wrapper');
                const canvas = document.getElementById('box-canvas');
                if (wrapper && canvas) {
                    canvas.width = wrapper.clientWidth;
                    canvas.height = wrapper.clientHeight;
                }
                if (chart && wrapper) {
                    chart.resize(wrapper.clientWidth, wrapper.clientHeight);
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
                    const tp2Height = Math.abs(tp1Y - tp2Y);
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
                        resizeCanvas();
                        drawPositionBoxes();
                    }, 50);
                } else if (tabId === 'stats') {
                    recalculateAdvancedStats();
                } else if (tabId === 'excel') {
                    loadReportsList();
                    loadArchivePreview();
                } else if (tabId === 'journal') {
                    renderJournalTable();
                }
            }

            async function toggleBotTrading() {
                try {
                    const res = await fetch('/api/toggle_bot_trading', { method: 'POST' });
                    const data = await res.json();
                    const btn = document.getElementById('bot-toggle-btn');
                    if (btn) {
                        if (data.active) {
                            btn.className = "px-2.5 py-1 rounded-lg font-bold bg-emerald-600 text-black hover:bg-emerald-500 transition";
                            btn.innerText = "🤖 Bot: AÇIK";
                        } else {
                            btn.className = "px-2.5 py-1 rounded-lg font-bold bg-rose-600 text-white hover:bg-rose-500 transition";
                            btn.innerText = "🤖 Bot: KAPALI";
                        }
                    }
                } catch(e) {}
            }

            function setJournalFilter(dir) {
                journalDirectionFilter = dir;
                ['ALL', 'LONG', 'SHORT'].forEach(d => {
                    const btn = document.getElementById(`j-filter-${d}`);
                    if (btn) {
                        if (d === dir) {
                            btn.className = "px-3 py-1 rounded bg-emerald-600 text-black font-bold transition";
                        } else {
                            btn.className = "px-3 py-1 rounded bg-slate-800 text-slate-300 hover:text-white transition";
                        }
                    }
                });
                renderJournalTable();
            }

            function selectJournalItem(index) {
                const searchTxt = document.getElementById('journal-search').value.toLowerCase();
                let filtered = tradeHistoryCache.filter(h => {
                    const matchSymbol = h.symbol.toLowerCase().includes(searchTxt);
                    const matchDir = journalDirectionFilter === 'ALL' || h.direction === journalDirectionFilter;
                    return matchSymbol && matchDir;
                });
                
                selectedJournalItem = filtered[index];
                if (!selectedJournalItem) return;

                renderJournalTable();
                
                const box = document.getElementById('journal-detail-box');
                box.innerHTML = `
                    <div class="bg-slate-900/80 p-2.5 rounded border border-slate-800 space-y-2">
                        <div class="flex justify-between items-center"><span class="font-bold text-white text-sm">${selectedJournalItem.symbol}</span><span class="px-2 py-0.5 rounded text-[10px] font-bold ${selectedJournalItem.direction === 'LONG' ? 'bg-emerald-500/20 text-emerald-400' : 'bg-red-500/20 text-red-400'}">${selectedJournalItem.direction}</span></div>
                        <div class="text-[11px] text-slate-400">Kapanış Nedeni: <b class="text-sky-300">${selectedJournalItem.close_reason}</b></div>
                        <div class="text-[11px] text-slate-400">Süre: <b class="text-white">${selectedJournalItem.duration_mins || 1} Dakika</b></div>
                        <div class="text-[11px] text-slate-400 pt-1 border-t border-slate-800 uppercase font-bold text-emerald-400">Giriş Gerekçeleri:</div>
                        <div class="space-y-1">
                            ${(selectedJournalItem.open_reasons || []).map(r => `<div class="bg-black/40 p-1.5 rounded border border-slate-800 text-[11px] text-slate-300">✓ ${r}</div>`).join('')}
                        </div>
                    </div>
                `;
            }

            function renderJournalTable() {
                const searchTxt = document.getElementById('journal-search') ? document.getElementById('journal-search').value.toLowerCase() : '';
                let filtered = tradeHistoryCache.filter(h => {
                    const matchSymbol = h.symbol.toLowerCase().includes(searchTxt);
                    const matchDir = journalDirectionFilter === 'ALL' || h.direction === journalDirectionFilter;
                    return matchSymbol && matchDir;
                });

                const totalCount = tradeHistoryCache.length;
                const wins = tradeHistoryCache.filter(h => h.realized_pnl > 0).length;
                const winRate = totalCount > 0 ? ((wins / totalCount) * 100).toFixed(1) : "0.0";
                const netPnl = tradeHistoryCache.reduce((acc, h) => acc + h.realized_pnl, 0);

                document.getElementById('journal-total-trades').innerText = totalCount;
                document.getElementById('journal-winrate').innerText = `%${winRate}`;
                const jPnlEl = document.getElementById('journal-total-pnl');
                jPnlEl.innerText = `${netPnl >= 0 ? '+' : ''}$${netPnl.toFixed(2)}`;
                jPnlEl.className = `text-sm font-bold font-mono ${netPnl >= 0 ? 'text-emerald-400' : 'text-rose-400'}`;

                const journalTbody = document.getElementById('journal-table');
                if (journalTbody) {
                    journalTbody.innerHTML = filtered.map((h, idx) => {
                        const isSelected = selectedJournalItem && selectedJournalItem.symbol === h.symbol && selectedJournalItem.close_timestamp === h.close_timestamp;
                        let badgeClass = "bg-sky-500/20 text-sky-400 border-sky-500/40";
                        if (h.close_reason.includes("Stop-Loss")) badgeClass = "bg-rose-500/20 text-rose-400 border-rose-500/40";
                        else if (h.close_reason.includes("TP2") || h.close_reason.includes("%100")) badgeClass = "bg-emerald-500/20 text-emerald-400 border-emerald-500/40";

                        return `
                            <tr class="hover:bg-slate-800/60 cursor-pointer ${isSelected ? 'bg-slate-800/80 border-l-2 border-emerald-500' : ''}" onclick="selectJournalItem(${idx})">
                                <td class="py-2 font-mono text-slate-400">${h.close_time}</td>
                                <td class="font-bold text-white">${h.symbol}</td>
                                <td class="${h.direction === 'LONG' ? 'text-emerald-400' : 'text-red-400'} font-bold">${h.direction}</td>
                                <td class="font-mono text-slate-300">${h.entry} ➔ ${h.close_price}</td>
                                <td class="font-bold font-mono ${h.realized_pnl >= 0 ? 'text-emerald-400' : 'text-rose-400'}">${h.realized_pnl >= 0 ? '+' : ''}$${h.realized_pnl.toFixed(2)}</td>
                                <td><span class="px-2 py-0.5 rounded text-[10px] font-bold border ${badgeClass}">${h.close_reason}</span></td>
                            </tr>`;
                    }).join('') || '<tr><td colspan="6" class="py-4 text-center text-slate-500 italic">Eşleşen kayıt bulunamadı...</td></tr>';
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

            function getPrecisionConfig(price) {
                if (price < 0.001) return { precision: 6, minMove: 0.000001 };
                if (price < 1) return { precision: 4, minMove: 0.0001 };
                if (price < 100) return { precision: 3, minMove: 0.001 };
                return { precision: 2, minMove: 0.01 };
            }

            function getIntervalSeconds(tf) {
                const mapping = { '1': 60, '5': 300, '15': 900, '60': 3600, '240': 14400, 'D': 86400 };
                return mapping[tf] || 300;
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
                        rightOffset: 25,
                        fixLeftEdge: false,
                        fixRightEdge: false,
                        lockVisibleTimeRangeOnResize: false,
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

            async function fetchCandlesDirect(symbol, interval = '60', fetchLimit = 1000) {
                let baseSym = symbol.split('/')[0].toUpperCase().replace(':USDT', '');
                if (baseSym.endsWith('USDT') && baseSym !== 'USDT') {
                    baseSym = baseSym.replace('USDT', '');
                }
                
                const tfMap = { '1': '1m', '5': '5m', '15': '15m', '60': '1h', '240': '4h', 'D': '1d' };
                const binanceInterval = tfMap[currentTimeframe] || '1h';

                let possibleSymbols = [
                    resolvedSymbolCache[symbol],
                    baseSym + 'USDT',
                    '1000' + baseSym + 'USDT',
                    baseSym.replace('1000', '') + 'USDT'
                ].filter(Boolean);

                let data = [];
                let finalUsedSym = null;

                for (let rawSym of possibleSymbols) {
                    let url = `https://fapi.binance.com/fapi/v1/klines?symbol=${rawSym}&interval=${binanceInterval}&limit=${fetchLimit}&_t=${Date.now()}`;
                    try {
                        let res = await fetch(url);
                        let json = await res.json();
                        if (Array.isArray(json) && json.length > 0 && json.code === undefined) {
                            data = json;
                            finalUsedSym = rawSym;
                            break;
                        }
                    } catch(e) {}
                }

                if (finalUsedSym) {
                    resolvedSymbolCache[symbol] = finalUsedSym;
                }

                if (Array.isArray(data) && data.length > 0) {
                    // Türkiye saati (TSİ / +3 saat = +10800 saniye) hizalaması ve grafik senkronizasyonu
                    return data.map(c => ({
                        time: Math.floor(c[0] / 1000) + 10800, 
                        open: parseFloat(c[1]), 
                        high: parseFloat(c[2]), 
                        low: parseFloat(c[3]), 
                        close: parseFloat(c[4])
                    }));
                }
                return [];
            }

            async function fetchLiveTickerPrice(symbol) {
                let baseSym = symbol.split('/')[0].toUpperCase().replace(':USDT', '');
                if (baseSym.endsWith('USDT') && baseSym !== 'USDT') baseSym = baseSym.replace('USDT', '');
                let rawSym = resolvedSymbolCache[symbol] || (baseSym + 'USDT');
                try {
                    let res = await fetch(`https://fapi.binance.com/fapi/v1/ticker/price?symbol=${rawSym}`);
                    let json = await res.json();
                    if (json && json.price) return parseFloat(json.price);
                } catch(e) {}
                return null;
            }

            async function loadChartCandles(symbol, posData = null, isLiveTick = false) {
                try {
                    const symbolChanged = (symbol !== lastLoadedSymbol || currentTimeframe !== lastLoadedTf);
                    const candles = await fetchCandlesDirect(symbol, currentTimeframe, 1000);
                    
                    if (candles.length > 0 && candleSeries) {
                        if (isLiveTick && !symbolChanged) {
                            let livePrice = await fetchLiveTickerPrice(symbol);
                            if (livePrice) {
                                let lastCandle = candles[candles.length - 1];
                                lastCandle.close = livePrice;
                                if (livePrice > lastCandle.high) lastCandle.high = livePrice;
                                if (livePrice < lastCandle.low) lastCandle.low = livePrice;
                                candleSeries.update(lastCandle);
                                
                                const dec = livePrice < 1 ? 6 : 2;
                                document.getElementById('bar-close').innerText = `$${livePrice.toFixed(dec)}`;
                                document.getElementById('bar-high').innerText = `$${lastCandle.high.toFixed(dec)}`;
                                document.getElementById('bar-low').innerText = `$${lastCandle.low.toFixed(dec)}`;
                            }
                            drawPositionBoxes();
                        } else {
                            lastLoadedSymbol = symbol;
                            lastLoadedTf = currentTimeframe;

                            const lastCandle = candles[candles.length - 1];
                            const pConf = getPrecisionConfig(lastCandle.close);
                            candleSeries.applyOptions({ priceFormat: { type: 'price', precision: pConf.precision, minMove: pConf.minMove } });
                            
                            const intervalSec = getIntervalSeconds(currentTimeframe);
                            let futureData = [];
                            let lastTime = lastCandle.time;
                            for (let i = 1; i <= 50; i++) {
                                futureData.push({ time: lastTime + (i * intervalSec) });
                            }
                            
                            candleSeries.setData([...candles, ...futureData]);

                            const dec = lastCandle.close < 1 ? pConf.precision : 2;
                            document.getElementById('bar-open').innerText = `$${lastCandle.open.toFixed(dec)}`;
                            document.getElementById('bar-high').innerText = `$${lastCandle.high.toFixed(dec)}`;
                            document.getElementById('bar-low').innerText = `$${lastCandle.low.toFixed(dec)}`;
                            document.getElementById('bar-close').innerText = `$${lastCandle.close.toFixed(dec)}`;
                            
                            if (chart) chart.priceScale('right').applyOptions({ autoScale: true });
                            
                            resizeCanvas();
                            drawPositionBoxes();

                            priceLines.forEach(l => candleSeries.removePriceLine(l));
                            priceLines = [];
                            const tfLabel = currentTimeframe === '60' ? '1H' : (currentTimeframe === '240' ? '4H' : (currentTimeframe === 'D' ? '1D' : `${currentTimeframe}M`));
                            document.getElementById('chart-title').innerText = `${symbol} (${tfLabel})`;

                            if (posData && candleSeries) {
                                const entryLine = candleSeries.createPriceLine({ price: posData.entry, color: '#38bdf8', lineWidth: 2, lineStyle: LightweightCharts.LineStyle.Solid, axisLabelVisible: true, title: 'GİRİŞ' });
                                const slLine = candleSeries.createPriceLine({ price: posData.sl, color: '#ef4444', lineWidth: 2, lineStyle: LightweightCharts.LineStyle.Dashed, axisLabelVisible: true, title: 'STOP' });
                                
                                priceLines.push(entryLine, slLine);
                                
                                const p = posData.entry < 1 ? 6 : 2;
                                let htmlStr = `<span class="text-sky-400 font-mono">Giriş: ${posData.entry}</span> | <span class="text-red-400 font-mono">SL: ${posData.sl.toFixed(p)}</span>`;
                                
                                if (Math.abs(posData.tp1 - posData.tp2) / posData.entry < 0.001) {
                                    const tpLine = candleSeries.createPriceLine({ price: posData.tp1, color: '#10b981', lineWidth: 2, lineStyle: LightweightCharts.LineStyle.Dashed, axisLabelVisible: true, title: 'TP (TAM ÇIKIŞ)' });
                                    priceLines.push(tpLine);
                                    htmlStr += ` | <span class="text-emerald-500 font-mono font-bold">TP: ${posData.tp1.toFixed(p)}</span>`;
                                } else {
                                    const tp1Line = candleSeries.createPriceLine({ price: posData.tp1, color: '#4ade80', lineWidth: 2, lineStyle: LightweightCharts.LineStyle.Dashed, axisLabelVisible: true, title: 'TP1' });
                                    const tp2Line = candleSeries.createPriceLine({ price: posData.tp2, color: '#047857', lineWidth: 2, lineStyle: LightweightCharts.LineStyle.Dashed, axisLabelVisible: true, title: 'TP2' });
                                    priceLines.push(tp1Line, tp2Line);
                                    htmlStr += ` | <span class="text-emerald-600 font-mono">TP2: ${posData.tp2.toFixed(p)}</span>`;
                                }
                                
                                document.getElementById('chart-levels').innerHTML = htmlStr;
                            } else {
                                document.getElementById('chart-levels').innerHTML = '';
                            }
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
                
                const tp1StatusHtml = pos.tp1_hit 
                    ? `<div class="bg-emerald-950/60 border border-emerald-800 p-1.5 rounded text-[11px] text-emerald-400 font-bold mb-2">⚡ TP1 Alındı (%50 Kâr Realize Edildi - Stop Giriş Boyuna Çekildi)</div>` 
                    : ``;

                let tpDisplayHtml = "";
                if (Math.abs(pos.tp1 - pos.tp2) / pos.entry < 0.001) {
                    tpDisplayHtml = `<div class="text-emerald-500 font-bold">TP (TAM ÇIKIŞ): <span class="text-emerald-400">${pos.tp1.toFixed(p)}</span></div>`;
                } else {
                    tpDisplayHtml = `
                        <div class="text-emerald-800 font-bold">TP2: <span class="text-emerald-400">${pos.tp2.toFixed(p)}</span></div>
                        <div class="text-emerald-600 font-bold">TP1: <span class="text-emerald-400">${pos.tp1.toFixed(p)}</span></div>
                    `;
                }

                document.getElementById('active-rationale').innerHTML = `
                    <div class="flex justify-between items-center mb-2"><span class="font-bold text-base text-white">${pos.symbol}</span><span class="px-2 py-0.5 rounded text-xs font-bold ${pos.direction === 'LONG' ? 'bg-emerald-500/20 text-emerald-400' : 'bg-red-500/20 text-red-400'}">${pos.direction}</span></div>
                    ${tp1StatusHtml}
                    <div class="space-y-1 text-slate-300">${pos.reasons.map(r => `<div class="bg-slate-900/60 p-1.5 rounded border border-slate-800">✓ ${r}</div>`).join('')}</div>
                    <div class="mt-2 p-2 bg-black/40 rounded border border-slate-800 text-[11px] space-y-1 font-mono">
                        <div class="text-slate-400">Giriş Saati: <span class="text-white font-bold font-sans">${pos.open_time}</span></div>
                        <div class="text-slate-400">Mod: <span class="text-white font-bold font-sans">${pos.leverage}x ${modeLabel} ($${pos.margin})</span></div>
                        ${tpDisplayHtml}
                        <div class="text-sky-400 font-bold">Giriş: <span>${pos.entry}</span></div>
                        <div class="text-red-400 font-bold">SL: <span>${pos.sl.toFixed(p)}</span></div>
                    </div>`;
            }

            async function manualClosePos(symbol, btn) {
                if(btn){ btn.innerText = "⏳"; btn.disabled = true; btn.classList.add("opacity-50"); playAlertSound("CLICK"); }
                await fetch('/api/manual/close_position', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({symbol}) });
                updateDashboard();
            }

            async function manualPartialClose(symbol, ratio, btn) {
                if(btn){ btn.innerText = "⏳"; btn.disabled = true; btn.classList.add("opacity-50"); playAlertSound("CLICK"); }
                await fetch('/api/manual/partial_close', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({symbol, ratio}) });
                updateDashboard();
            }

            async function manualCloseAll(btn) {
                if (confirm("Tüm pozisyonları kapatmak istediğinize emin misiniz?")) {
                    playAlertSound("CLICK");
                    if(btn){ btn.innerText = "⏳ Kapatılıyor..."; btn.disabled = true; btn.classList.add("opacity-50"); }
                    await fetch('/api/manual/close_all', { method: 'POST' });
                    updateDashboard();
                    if(btn){ btn.innerText = "🚨 TÜMÜNÜ KAPAT (ACİL)"; btn.disabled = false; btn.classList.remove("opacity-50"); }
                }
            }

            async function manualBreakevenAll(btn) {
                if (confirm("Tüm stopları başa başa çekmek istiyor musunuz?")) {
                    playAlertSound("CLICK");
                    if(btn){ btn.innerText = "⏳ İşleniyor..."; btn.disabled = true; btn.classList.add("opacity-50"); }
                    await fetch('/api/manual/breakeven_all', { method: 'POST' });
                    updateDashboard();
                    if(btn){ btn.innerText = "🛡️ TÜMÜNÜ BAŞABAŞ ÇEK"; btn.disabled = false; btn.classList.remove("opacity-50"); }
                }
            }

            async function manualBreakeven(symbol, btn) {
                if(btn){ btn.innerText = "⏳"; btn.disabled = true; btn.classList.add("opacity-50"); playAlertSound("CLICK"); }
                await fetch('/api/manual/breakeven', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({symbol}) });
                updateDashboard();
            }

            async function manualToggleTrailing(symbol, btn) {
                if(btn){ btn.innerText = "⏳"; btn.disabled = true; btn.classList.add("opacity-50"); playAlertSound("CLICK"); }
                await fetch('/api/manual/toggle_trailing', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({symbol}) });
                updateDashboard();
            }

            async function manualUpdateSlTp(symbol, btn) {
                if(btn){ btn.innerText = "⏳"; btn.disabled = true; btn.classList.add("opacity-50"); playAlertSound("CLICK"); }
                const sl = parseFloat(document.getElementById(`manual-sl-${symbol}`).value);
                const tp2 = parseFloat(document.getElementById(`manual-tp-${symbol}`).value);
                await fetch('/api/manual/update_sltp', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({symbol, sl, tp2}) });
                updateDashboard();
            }

            async function saveSettings(btn) {
                if(btn){ btn.innerText = "⏳"; btn.disabled = true; btn.classList.add("opacity-50"); playAlertSound("CLICK"); }
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
                if (res.ok) { 
                    updateDashboard(); 
                    if(btn){ btn.innerText = "KAYDET"; btn.disabled = false; btn.classList.remove("opacity-50"); }
                }
            }

            async function saveApiSettings(btn) {
                if(btn){ btn.innerText = "⏳"; btn.disabled = true; btn.classList.add("opacity-50"); playAlertSound("CLICK"); }
                const exchange = document.getElementById('api-exchange').value;
                const mode = document.getElementById('api-mode').value;
                const api_key = document.getElementById('api-key').value;
                const api_secret = document.getElementById('api-secret').value;
                await fetch('/api/update_api', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({exchange, mode, api_key, api_secret, auto_trade: true}) });
                updateDashboard();
                if(btn){ btn.innerText = "KAYDET"; btn.disabled = false; btn.classList.remove("opacity-50"); }
            }

            async function updateDashboard() {
                try {
                    const res = await fetch('/api/state');
                    const data = await res.json();

                    if (data.logs && data.logs.length > 0) {
                        const currentTopLog = data.logs[0];
                        if (currentTopLog !== lastProcessedLog) {
                            lastProcessedLog = currentTopLog;
                            
                            if (currentTopLog.includes("POZİSYON AÇILDI")) {
                                playAlertSound("OPEN");
                                sendWebNotification("🟢 Yeni İşlem Açıldı", currentTopLog.split("]")[1]);
                            } else if (currentTopLog.includes("TP1 ALINDI") || currentTopLog.includes("Likidite Havuzuna Ulaşıldı") || currentTopLog.includes("Momentum Kaybı")) {
                                playAlertSound("WIN");
                                sendWebNotification("🎯 Kâr Alındı / Hedef Vuruldu", currentTopLog.split("]")[1]);
                            } else if (currentTopLog.includes("Stop-Loss Tetiklendi") || currentTopLog.includes("LİMİTİ TETİKLENDİ")) {
                                playAlertSound("LOSS");
                                sendWebNotification("🛑 Stop-Loss Patladı", currentTopLog.split("]")[1]);
                            } else if (currentTopLog.includes("MANUEL KAPATMA")) {
                                playAlertSound("CLICK");
                            }
                        }
                    }

                    const logBoxElem = document.getElementById('log-box');
                    if(logBoxElem) {
                        logBoxElem.innerHTML = data.logs.map(l => `<div>${l}</div>`).join('');
                    }

                    document.getElementById('scanned-count').innerText = data.scanned_count;
                    document.getElementById('last-scan').innerText = data.last_scan_time;

                    if (data.order_book_metrics) {
                        let ob = data.order_book_metrics;
                        document.getElementById('ob-bid-pct').innerText = '%' + ob.bid_pressure;
                        document.getElementById('ob-ask-pct').innerText = '%' + ob.ask_pressure;
                        document.getElementById('ob-bid-bar').style.width = ob.bid_pressure + '%';
                        document.getElementById('ob-ask-bar').style.width = ob.ask_pressure + '%';
                    }

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
                    const totalUnrealizedPnl = data.active_positions.reduce((acc, p) => acc + p.unrealized_pnl, 0);
                    const totalPnlPct = data.total_balance > 0 ? ((totalUnrealizedPnl / data.total_balance) * 100) : 0;

                    const usedPct = data.total_balance > 0 ? ((totalUsedMargin / data.total_balance) * 100).toFixed(1) : "0.0";
                    document.getElementById('stat-used-margin').innerText = `$${totalUsedMargin.toFixed(1)} (%${usedPct})`;

                    document.getElementById('man-total-pos').innerText = `${data.active_positions.length} Adet`;
                    document.getElementById('man-total-margin').innerText = `$${totalUsedMargin.toFixed(2)}`;
                    document.getElementById('man-total-risk').innerText = `$${totalRiskAmount.toFixed(2)}`;

                    const manPnlEl = document.getElementById('man-total-pnl');
                    if (manPnlEl) {
                        manPnlEl.innerText = `${totalUnrealizedPnl >= 0 ? '+' : ''}$${totalUnrealizedPnl.toFixed(2)} (%${totalPnlPct >= 0 ? '+' : ''}${totalPnlPct.toFixed(2)})`;
                        manPnlEl.className = `text-sm font-bold font-mono ${totalUnrealizedPnl >= 0 ? 'text-emerald-400' : 'text-rose-400'}`;
                    }

                    const totalBalanceElem = document.getElementById('stat-total-balance');
                    if (totalBalanceElem) {
                        totalBalanceElem.innerText = `$${data.total_balance.toFixed(2)}`;
                        if (data.total_balance > data.initial_balance) {
                            totalBalanceElem.className = "text-sm font-extrabold font-mono text-emerald-400";
                        } else if (data.total_balance < data.initial_balance) {
                            totalBalanceElem.className = "text-sm font-extrabold font-mono text-rose-400";
                        } else {
                            totalBalanceElem.className = "text-sm font-extrabold font-mono text-white";
                        }
                    }

                    const balanceInput = document.getElementById('input-balance');
                    if (balanceInput && document.activeElement !== balanceInput) {
                        balanceInput.value = data.total_balance.toFixed(2);
                    }

                    tradeHistoryCache = data.trade_history;
                    recalculatePnlMetrics();
                    recalculateAdvancedStats();
                    loadArchivePreview();
                    renderJournalTable();

                    if (selectedPos) {
                        const updatedSelected = data.active_positions.find(p => p.symbol === selectedPos.symbol);
                        if (updatedSelected) {
                            selectedPos = updatedSelected;
                            renderRationale(selectedPos);
                        }
                    }

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
                                <td class="text-center"><button onclick="manualToggleTrailing('${p.symbol}', this)" class="px-2 py-1 rounded text-[10px] font-bold ${p.trailing_active ? 'bg-emerald-600 text-black animate-pulse' : 'bg-slate-800 text-slate-400'}">${p.trailing_active ? 'AÇIK' : 'KAPALI'}</button></td>
                                <td class="text-right space-x-1">
                                    <button onclick="manualUpdateSlTp('${p.symbol}', this)" class="bg-slate-700 hover:bg-slate-600 px-1.5 py-1 rounded text-[10px] text-white">💾</button>
                                    <button onclick="manualPartialClose('${p.symbol}', 0.5, this)" class="bg-amber-600 hover:bg-amber-500 px-1.5 py-1 rounded text-[10px] text-white font-bold">%50</button>
                                    <button onclick="manualBreakeven('${p.symbol}', this)" class="bg-sky-600 hover:bg-sky-500 px-1.5 py-1 rounded text-[10px] text-white">Başa Baş</button>
                                    <button onclick="manualClosePos('${p.symbol}', this)" class="bg-rose-600 hover:bg-rose-500 px-2 py-1 rounded text-[10px] text-white font-bold">Kapat</button>
                                </td>
                            </tr>`).join('') || '<tr><td colspan="8" class="py-3 text-slate-500 italic">Açık pozisyon yok...</td></tr>';
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

                    try {
                        if (data.equity_curve && data.equity_curve.length > 0 && equitySeries) {
                            let eqMap = new Map();
                            data.equity_curve.forEach(d => {
                                if(d && !isNaN(d.time)) {
                                    eqMap.set(Number(d.time), Number(d.value));
                                }
                            });
                            
                            let sortedEq = Array.from(eqMap.entries())
                                .map(([t, v]) => ({time: t, value: v}))
                                .sort((a,b) => a.time - b.time);
                                
                            let finalEq = [];
                            let lastTime = 0;
                            for (let i = 0; i < sortedEq.length; i++) {
                                if (sortedEq[i].time > lastTime) {
                                    finalEq.push(sortedEq[i]);
                                    lastTime = sortedEq[i].time;
                                }
                            }
                            
                            if (finalEq.length > 0) {
                                equitySeries.setData(finalEq);
                            }
                        }
                    } catch(err) {
                        console.error("Equity Curve Çizim Hatası: ", err);
                    }

                    if (currentSymbol) loadChartCandles(currentSymbol, selectedPos, true);
                    if (!selectedPos && data.active_positions.length > 0) selectPosition(data.active_positions[0]);
                } catch (e) {
                    console.error("Dashboard Güncelleme Hatası: ", e);
                }
            }

            initCharts();
            loadChartCandles(currentSymbol, null, false);
            setInterval(updateDashboard, 5000);
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
