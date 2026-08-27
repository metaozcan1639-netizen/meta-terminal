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

# =================================================================
# 📱 TELEGRAM BİLDİRİM AYARLARI
# =================================================================
TELEGRAM_BOT_TOKEN = "8971696278:AAHiBk7gzMGxjAz2mi4KqV4LEUWwhVH6NKc"
TELEGRAM_CHAT_ID = "2088808175"
# =================================================================

def get_now_str():
    return datetime.now(TURKEY_TZ).strftime("%H:%M:%S")

def get_now_datetime():
    return datetime.now(TURKEY_TZ)

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
        datetime(now.year, now.month, 10, 15, 30, tzinfo=TURKEY_TZ), 
        datetime(now.year, now.month, 5, 15, 30, tzinfo=TURKEY_TZ),
        datetime(now.year, now.month, 18, 21, 0, tzinfo=TURKEY_TZ)
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

        system_state["sentiment_data"]["btc_rsi"] = round(float(last_1h['rsi']), 1) if pd.notnull(last_1h['rsi']) else 52.2
        system_state["sentiment_data"]["btc_volume_24h"] = vol_str
        system_state["sentiment_data"]["market_bias"] = bias
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

        # 1H ZAMAN DİLİMİNE SABİTLENMİŞ YÜKSEK KALİTELİ VERİ ÇEKİMİ
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

        # L2 Emir Defteri (Order Book) Derinlik Baskısı Hesaplama
        bids = book.get('bids', [])
        asks = book.get('asks', [])
        bid_vol = sum(b[1] for b in bids[:10]) if bids else 1.0
        ask_vol = sum(a[1] for a in asks[:10]) if asks else 1.0
        total_depth = bid_vol + ask_vol
        bid_pressure = round((bid_vol / total_depth) * 100, 1)
        ask_pressure = round((ask_vol / total_depth) * 100, 1)

        # Eğer bu parite seçili parite ise terminal için state'e kaydet
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
                if c_1h['low'] <= tracker["level"] * 1.003 and c_1h['close'] > tracker["level"]:
                    direction = "LONG"
                    score += tracker["score_base"] + 25
                    reasons = tracker["reasons"] + ["🎯 Kusursuz 1H Break & Retest (Destek Onayı)"]
                    del retest_tracker[symbol]
            elif tracker["direction"] == "SHORT":
                if c_1h['high'] >= tracker["level"] * 0.997 and c_1h['close'] < tracker["level"]:
                    direction = "SHORT"
                    score += tracker["score_base"] + 25
                    reasons = tracker["reasons"] + ["🎯 Kusursuz 1H Break & Retest (Direnç Onayı)"]
                    del retest_tracker[symbol]

        # L2 Emir Defteri Baskı Filtresi (Sabit puan ekleme yok, tahta baskısına göre gerçekçi dinamik puan)
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

        if not direction or score < 75:
            return None

        entry = float(c_1h['close'])
        atr = float(c_1h['atr']) if pd.notnull(c_1h['atr']) else entry * 0.01

        effective_leverage = system_state["leverage"]
        effective_risk = system_state["risk_pct"]

        # KUSURSUZ VE GÜVENLİ STOP MESAFESİ (2.8 * ATR Sabitlendi)
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
    add_log("Quant Motoru (Avcı): 1H Break & Retest + L2 Emir Defteri Derinlik Analizi Aktif!")

    while True:
        exchange = None
        try:
            exchange = await create_exchange_instance()
            check_daily_drawdown()
            sync_wallet_accounting()
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

                system_state["last_scan_time"] = get_now_str()
                await asyncio.sleep(2)

            await exchange.close()
            await asyncio.sleep(2)
        except Exception as e:
            add_log(f"Döngü Uyarısı: {str(e)[:45]}")
            if exchange:
                try: await exchange.close()
                except: pass
            await asyncio.sleep(2)

async def position_manager_loop():
    await asyncio.sleep(5)
    while True:
        exchange = None
        try:
            if not system_state["active_positions"]:
                await asyncio.sleep(1)
                continue

            exchange = await create_exchange_instance()
            symbols = [p['symbol'] for p in system_state["active_positions"]]
            tickers = await exchange.fetch_tickers(symbols)
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
                            apply_realized_pnl(partial_pnl)
                            pos["margin"] = round(pos.get("margin", 0.0) * 0.5, 2)
                            
                            log_msg = f"⚡ TP1 ALINDI ({pos['symbol']}): %50 Kâr Realize Edildi (+${partial_pnl}) | Stop Başabaşa Çekildi."
                            add_log(log_msg)
                            asyncio.create_task(send_telegram_alert(f"⚡ <b>İLK HEDEF (TP1) VURULDU</b>\nParite: {pos['symbol']}\nKâr: +${partial_pnl}"))

                    if close_reason:
                        pnl_pct = ((curr_price - pos['entry']) / pos['entry'] * 100) if direction == "LONG" else ((pos['entry'] - curr_price) / pos['entry'] * 100)
                        realized_pnl = round(pos['active_size'] * (pnl_pct / 100.0), 2)
                        apply_realized_pnl(realized_pnl)

                        duration_mins = max(1, int((now_ts - pos.get('open_timestamp', now_ts)) / 60))
                        system_state["trade_history"].insert(0, {
                            "symbol": pos['symbol'], "direction": pos['direction'], "entry": pos['entry'],
                            "close_price": curr_price, "pnl_pct": round(pnl_pct, 2), "realized_pnl": realized_pnl,
                            "score": pos['score'], "duration_mins": duration_mins, "open_reasons": pos['reasons'],
                            "close_reason": close_reason, "close_time": get_now_str(), "close_timestamp": now_ts
                        })
                        system_state["active_positions"].remove(pos)
                        sync_wallet_accounting()
                        add_log(f"🔴 POZİSYON KAPANDI: {pos['symbol']} | PnL: %{pnl_pct:.2f} (${realized_pnl}) | {close_reason}")
                        asyncio.create_task(send_telegram_alert(f"🔴 <b>POZİSYON KAPANDI</b>\nParite: {pos['symbol']}\nNet PnL: ${realized_pnl}"))

                except Exception:
                    pass

            await exchange.close()
            await asyncio.sleep(1) 
        except Exception:
            if exchange:
                try: await exchange.close()
                except: pass
            await asyncio.sleep(1)

@asynccontextmanager
async def lifespan(app: FastAPI):
    t1 = asyncio.create_task(market_scanner_loop())
    t2 = asyncio.create_task(keep_alive_loop())
    t3 = asyncio.create_task(position_manager_loop())
    yield
    t1.cancel(); t2.cancel(); t3.cancel()

app = FastAPI(title="Meta Quant Terminal Pro Ultimate L2", lifespan=lifespan)

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

class ClosePosPayload(BaseModel):
    symbol: str

@app.post("/api/update_settings")
async def update_settings(payload: SettingsPayload):
    system_state["total_balance"] = payload.total_balance
    system_state["risk_pct"] = payload.risk_pct
    system_state["leverage"] = payload.leverage
    system_state["margin_mode"] = payload.margin_mode
    system_state["max_open_positions"] = payload.max_open_positions
    system_state["max_total_margin_pct"] = payload.max_total_margin_pct
    sync_wallet_accounting()
    return {"status": "success"}

@app.post("/api/toggle_bot_trading")
async def toggle_bot_trading():
    system_state["bot_trading_active"] = not system_state.get("bot_trading_active", True)
    return {"status": "success", "active": system_state["bot_trading_active"]}

@app.post("/api/manual/close_position")
async def manual_close_position(payload: ClosePosPayload):
    target = next((p for p in system_state["active_positions"] if p['symbol'] == payload.symbol), None)
    if target:
        system_state["active_positions"].remove(target)
        sync_wallet_accounting()
        return {"status": "success"}
    return {"status": "error"}

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
        <title>Meta Quant Terminal Pro - L2 Order Book Engine</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <script src="https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>
        <style>
            html, body { min-height: 100%; background-color: #0b0e14; color: #e2e8f0; font-family: 'Inter', monospace; }
            .card { background-color: #121824; border: 1px solid #1e293b; }
            .nav-tab.active { background-color: #10b981; color: #000; font-weight: bold; }
        </style>
    </head>
    <body class="p-3 space-y-3 pb-16">
        <div class="card p-3 rounded-xl flex flex-wrap justify-between items-center gap-3 border-emerald-500/30">
            <div class="flex items-center space-x-3">
                <div class="w-3 h-3 bg-emerald-500 rounded-full animate-ping"></div>
                <h1 class="text-base font-extrabold text-emerald-400">META QUANT ULTIMATE + L2 ORDER BOOK</h1>
            </div>
            <div class="flex items-center space-x-2 text-xs">
                <button onclick="toggleBot()" id="bot-btn" class="px-3 py-1 rounded-lg font-bold bg-emerald-600 text-black">🤖 Bot: AÇIK</button>
                <div>Taranan: <span id="scanned" class="text-white font-bold">0</span></div>
            </div>
        </div>

        <!-- ANA TERMİNAL VE L2 EMİR DEFTERİ GÖSTERGESİ -->
        <div class="grid grid-cols-1 lg:grid-cols-3 gap-3">
            <div class="card p-4 rounded-xl lg:col-span-2 space-y-4">
                <h2 class="text-xs font-bold text-emerald-400 uppercase">📊 Canlı Açık Pozisyonlar</h2>
                <div class="overflow-x-auto">
                    <table class="w-full text-left text-xs">
                        <thead class="text-slate-500 border-b border-slate-800">
                            <tr><th>PARİTE</th><th>YÖN</th><th>GİRİŞ</th><th>CANLI FİYAT</th><th>MARJİN</th><th>PnL</th><th>EYLEM</th></tr>
                        </thead>
                        <tbody id="active-pos-table" class="divide-y divide-slate-800/50"></tbody>
                    </table>
                </div>
            </div>

            <!-- TERMINAL L2 EMİR DEFTERİ (ORDER BOOK) PANELİ -->
            <div class="card p-4 rounded-xl space-y-3">
                <h2 class="text-xs font-bold text-sky-400 uppercase">⚡ L2 Emir Defteri (Tahta Derinliği)</h2>
                <p class="text-[11px] text-slate-400">Seçilen veya taranan paritenin anlık alıcı/satıcı baskısı:</p>
                <div class="space-y-2 text-xs">
                    <div class="flex justify-between font-bold">
                        <span class="text-emerald-400">Alıcı (Bid): <b id="ob-bid-pct">%50.0</b></span>
                        <span class="text-rose-400">Satıcı (Ask): <b id="ob-ask-pct">%50.0</b></span>
                    </div>
                    <div class="w-full bg-slate-800 h-3 rounded-full overflow-hidden flex">
                        <div id="ob-bid-bar" class="bg-emerald-500 h-3" style="width: 50%"></div>
                        <div id="ob-ask-bar" class="bg-rose-500 h-3" style="width: 50%"></div>
                    </div>
                    <div class="text-[10px] text-slate-400 pt-1 font-mono">
                        <div>Alıcı Hacim: <span id="ob-bid-vol" class="text-white">-</span></div>
                        <div>Satıcı Hacim: <span id="ob-ask-vol" class="text-white">-</span></div>
                    </div>
                </div>
                <div class="border-t border-slate-800 pt-3">
                    <h3 class="text-[10px] font-bold text-slate-400 uppercase mb-1">Sistem Logları</h3>
                    <div id="log-box" class="bg-black/50 p-2 rounded text-[11px] text-emerald-400 font-mono h-32 overflow-y-auto space-y-1"></div>
                </div>
            </div>
        </div>

        <script>
            async function toggleBot() {
                let res = await fetch('/api/toggle_bot_trading', {method: 'POST'});
                let data = await res.json();
                document.getElementById('bot-btn').innerText = data.active ? "🤖 Bot: AÇIK" : "🤖 Bot: KAPALI";
            }

            async function closePos(symbol) {
                await fetch('/api/manual/close_position', {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({symbol})
                });
                update();
            }

            async function update() {
                try {
                    let res = await fetch('/api/state');
                    let data = await res.json();
                    
                    document.getElementById('scanned').innerText = data.scanned_count;
                    document.getElementById('log-box').innerHTML = data.logs.map(l => `<div>${l}</div>`).join('');

                    // L2 Order Book Panel Güncellemesi
                    let ob = data.order_book_metrics || {bid_pressure: 50, ask_pressure: 50, bid_vol: 0, ask_vol: 0};
                    document.getElementById('ob-bid-pct').innerText = '%' + ob.bid_pressure;
                    document.getElementById('ob-ask-pct').innerText = '%' + ob.ask_pressure;
                    document.getElementById('ob-bid-bar').style.width = ob.bid_pressure + '%';
                    document.getElementById('ob-ask-bar').style.width = ob.ask_pressure + '%';
                    document.getElementById('ob-bid-vol').innerText = ob.bid_vol;
                    document.getElementById('ob-ask-vol').innerText = ob.ask_vol;

                    document.getElementById('active-pos-table').innerHTML = data.active_positions.map(p => `
                        <tr>
                            <td class="py-2 font-bold text-white">${p.symbol}</td>
                            <td class="${p.direction === 'LONG' ? 'text-emerald-400' : 'text-rose-400'} font-bold">${p.direction} (${p.leverage}x)</td>
                            <td class="font-mono text-slate-300">${p.entry}</td>
                            <td class="font-mono text-white font-bold">${p.current_price || p.entry}</td>
                            <td class="font-mono text-amber-400">$${p.margin}</td>
                            <td class="font-mono font-bold ${p.unrealized_pnl >= 0 ? 'text-emerald-400' : 'text-rose-400'}">${p.unrealized_pnl >= 0 ? '+' : ''}$${p.unrealized_pnl.toFixed(2)}</td>
                            <td><button onclick="closePos('${p.symbol}')" class="bg-rose-600 px-2 py-1 rounded text-[10px] text-white font-bold">Kapat</button></td>
                        </tr>
                    `).join('') || '<tr><td colspan="7" class="py-4 text-center text-slate-500 italic">Aktif pozisyon bulunmuyor...</td></tr>';
                } catch(e) {}
            }
            setInterval(update, 4000);
            update();
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
