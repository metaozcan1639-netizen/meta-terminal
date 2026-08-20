import os
import asyncio
import logging
from datetime import datetime
import pandas as pd
import numpy as np
import ccxt.async_support as ccxt
import aiohttp
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import uvicorn

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

app = FastAPI(title="Meta Quant Terminal")

system_state = {
    "total_balance": 1000.0,
    "risk_pct": 1.0,
    "leverage": 10,
    "margin_mode": "ISOLATED",
    "scanned_count": 0,
    "last_scan_time": "-",
    "active_positions": [],
    "trade_history": [],
    "logs": []
}

# Geleneksel hisse senetleri ve sentetik kontratları filtreleme
EXCLUDED_KEYWORDS = [
    'NVDA', 'GOOGL', 'AAPL', 'TSLA', 'MSFT', 'AMZN', 'META', 'NFLX', 'AMD', 'COIN',
    'BABA', 'PLTR', 'SOXS', 'SOXL', 'QQQ', 'SPY', 'WDC', 'DELL', 'IONQ', 'GLW', 'BIRB'
]

def add_log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    system_state["logs"].insert(0, f"[{ts}] {msg}")
    if len(system_state["logs"]) > 50:
        system_state["logs"].pop()

def calculate_indicators(df):
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / (loss + 1e-9)
    df['rsi'] = 100 - (100 / (1 + rs))

    df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()
    df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()

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

async def analyze_symbol(exchange, symbol):
    try:
        base = symbol.split('/')[0].upper()
        if any(exc in base for exc in EXCLUDED_KEYWORDS):
            return None

        tfs = ['5m', '15m', '1h', '4h']
        tasks = [exchange.fetch_ohlcv(symbol, timeframe=tf, limit=40) for tf in tfs]
        results = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=4.0)

        if any(isinstance(r, Exception) or not r or len(r) < 25 for r in results):
            return None

        dfs = {tf: calculate_indicators(pd.DataFrame(results[i], columns=['t', 'open', 'high', 'low', 'close', 'volume']))
               for i, tf in enumerate(tfs)}

        df_5m, df_15m, df_1h, df_4h = dfs['5m'], dfs['15m'], dfs['1h'], dfs['4h']
        c_5m, p_5m = df_5m.iloc[-1], df_5m.iloc[-2]
        c_1h, c_4h = df_1h.iloc[-1], df_4h.iloc[-1]

        score = 0
        direction = None
        reasons = []

        # Makro Trend Uyumu (4H ve 1H)
        macro_bull = c_4h['close'] > c_4h['ema50'] and c_1h['close'] > c_1h['ema50']
        macro_bear = c_4h['close'] < c_4h['ema50'] and c_1h['close'] < c_1h['ema50']

        recent_low = df_5m['low'].iloc[-15:-2].min()
        recent_high = df_5m['high'].iloc[-15:-2].max()

        # Likidite Avı & Dönüş Teyidi
        if p_5m['low'] < recent_low and c_5m['close'] > recent_low and macro_bull:
            direction = "LONG"
            score += 30
            reasons.append("⚡ 5M Dip Likiditesi Süpürüldü (Stop-Hunt)")
        elif p_5m['high'] > recent_high and c_5m['close'] < recent_high and macro_bear:
            direction = "SHORT"
            score += 30
            reasons.append("⚡ 5M Tepe Likiditesi Süpürüldü (Stop-Hunt)")

        if not direction:
            return None

        score += 25
        reasons.append("📈 4H ve 1H EMA50 Üstü Güçlü Trend Uyumu")

        vol_ratio = c_5m['volume'] / (c_5m['vol_ma'] + 1e-9)
        if vol_ratio >= 1.5:
            score += 25
            reasons.append(f"🔥 Kurumsal Hacim Patlaması ({vol_ratio:.1f}x MA20)")

        if (direction == "LONG" and c_5m['rsi'] <= 40) or (direction == "SHORT" and c_5m['rsi'] >= 60):
            score += 20
            reasons.append(f"🎯 5M RSI Aşırı Uç Bölgede ({c_5m['rsi']:.1f})")

        if score >= 70:
            entry = float(c_5m['close'])
            atr = float(c_5m['atr']) if pd.notnull(c_5m['atr']) else entry * 0.005

            # Dinamik ve Güvenli Stop Marjı (1.2 * ATR Emniyet Payı)
            if direction == "LONG":
                sl = float(min(p_5m['low'], c_5m['low']) - (1.2 * atr))
                if (entry - sl) / entry < 0.006:
                    sl = entry * 0.994
                tp1 = float(entry + (1.6 * (entry - sl)))
                tp2 = float(entry + (3.2 * (entry - sl)))
            else:
                sl = float(max(p_5m['high'], c_5m['high']) + (1.2 * atr))
                if (sl - entry) / entry < 0.006:
                    sl = entry * 1.006
                tp1 = float(entry - (1.6 * (sl - entry)))
                tp2 = float(entry - (3.2 * (sl - entry)))

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
                "reasons": reasons,
                "open_time": datetime.now().strftime("%H:%M:%S")
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
                    await session.get(f"{url}/api/state")
            except Exception:
                pass

async def market_scanner_loop():
    exchange = ccxt.bybit({
        'options': {'defaultType': 'linear'},
        'enableRateLimit': True
    })
    add_log("Quant Motoru Aktif: Likit Kripto Vadeli Pariteler Taranıyor...")

    while True:
        try:
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
                signals = await asyncio.gather(*tasks)

                for sig in signals:
                    if sig:
                        exists = any(p['symbol'] == sig['symbol'] for p in system_state["active_positions"])
                        if not exists:
                            system_state["active_positions"].append(sig)
                            add_log(f"🟢 POZİSYON AÇILDI: {sig['symbol']} {sig['direction']} | {sig['leverage']}x İzole | Teminat: ${sig['margin']} | Maks Risk: ${sig['max_loss']}")

                system_state["last_scan_time"] = datetime.now().strftime("%H:%M:%S")
                await asyncio.sleep(0.2)

            for pos in list(system_state["active_positions"]):
                try:
                    ticker = await exchange.fetch_ticker(pos['symbol'])
                    curr_price = ticker['last']
                    direction = pos['direction']
                    close_reason = None

                    if (direction == "LONG" and curr_price <= pos['sl']) or (direction == "SHORT" and curr_price >= pos['sl']):
                        close_reason = "❌ Stop-Loss Tetiklendi"
                    elif (direction == "LONG" and curr_price >= pos['tp2']) or (direction == "SHORT" and curr_price <= pos['tp2']):
                        close_reason = "🎯 TP2 Majör Hedefe Ulaşıldı"
                    elif (direction == "LONG" and curr_price >= pos['tp1']) or (direction == "SHORT" and curr_price <= pos['tp1']):
                        if not pos.get("tp1_hit"):
                            pos["tp1_hit"] = True
                            pos["sl"] = pos["entry"]
                            add_log(f"⚡ TP1 Alındı ({pos['symbol']}): Stop Girişe Çekildi.")

                    if close_reason:
                        pnl_pct = ((curr_price - pos['entry']) / pos['entry'] * 100) if direction == "LONG" else ((pos['entry'] - curr_price) / pos['entry'] * 100)
                        realized_pnl = round(pos['pos_size'] * (pnl_pct / 100.0), 2)
                        system_state["total_balance"] += realized_pnl

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
                            "close_time": datetime.now().strftime("%H:%M:%S")
                        }
                        system_state["trade_history"].insert(0, history_item)
                        system_state["active_positions"].remove(pos)
                        add_log(f"🔴 POZİSYON KAPANDI: {pos['symbol']} | PnL: %{pnl_pct:.2f} (${realized_pnl}) | {close_reason}")
                except Exception:
                    pass

            await asyncio.sleep(1)
        except Exception as e:
            add_log(f"Döngü Uyarısı: {str(e)[:70]}")
            await asyncio.sleep(5)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(market_scanner_loop())
    asyncio.create_task(keep_alive_loop())

@app.get("/api/candles/{symbol:path}")
async def get_candles(symbol: str):
    exchange = ccxt.bybit({'options': {'defaultType': 'linear'}, 'enableRateLimit': True})
    try:
        raw_candles = await exchange.fetch_ohlcv(symbol, timeframe='5m', limit=100)
        await exchange.close()
        return [{
            "time": int(c[0] / 1000),
            "open": float(c[1]),
            "high": float(c[2]),
            "low": float(c[3]),
            "close": float(c[4])
        } for c in raw_candles]
    except Exception:
        await exchange.close()
        return []

class SettingsPayload(BaseModel):
    total_balance: float
    risk_pct: float
    leverage: int

@app.post("/api/update_settings")
async def update_settings(payload: SettingsPayload):
    system_state["total_balance"] = payload.total_balance
    system_state["risk_pct"] = payload.risk_pct
    system_state["leverage"] = payload.leverage
    add_log(f"⚙️ AYARLAR GÜNCELLENDİ: Kasa: ${payload.total_balance} | Risk: %{payload.risk_pct} | Kaldıraç: {payload.leverage}x İzole")
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
        <title>Meta Quant Terminal</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <script src="https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>
        <style>
            body { background-color: #0b0e14; color: #e2e8f0; font-family: 'Inter', monospace; }
            .card { background-color: #121824; border: 1px solid #1e293b; }
        </style>
    </head>
    <body class="p-4 space-y-4">
        <div class="card p-4 rounded-xl flex flex-wrap justify-between items-center gap-4 border-emerald-500/30">
            <div class="flex items-center space-x-3">
                <div class="w-3 h-3 bg-emerald-500 rounded-full animate-ping"></div>
                <div>
                    <h1 class="text-lg font-bold tracking-wider text-emerald-400">META QUANT PRO TERMINAL</h1>
                    <div class="text-[11px] text-slate-400">Kripto Vadeli Pariteler 7/24 Kesintisiz Canlı Motor</div>
                </div>
            </div>

            <div class="flex items-center space-x-3 bg-slate-900/80 p-2 rounded-lg border border-slate-800 text-xs">
                <div>
                    <label class="text-slate-400 block text-[10px]">KASA ($)</label>
                    <input id="input-balance" type="number" value="1000" class="bg-slate-800 text-white font-bold w-20 px-2 py-1 rounded outline-none border border-slate-700">
                </div>
                <div>
                    <label class="text-slate-400 block text-[10px]">RİSK (%)</label>
                    <select id="input-risk" class="bg-slate-800 text-white font-bold px-2 py-1 rounded outline-none border border-slate-700">
                        <option value="0.5">%0.5</option>
                        <option value="1.0" selected>%1.0</option>
                        <option value="2.0">%2.0</option>
                        <option value="3.0">%3.0</option>
                        <option value="5.0">%5.0</option>
                    </select>
                </div>
                <div>
                    <label class="text-slate-400 block text-[10px]">KALDIRAÇ</label>
                    <select id="input-leverage" class="bg-slate-800 text-emerald-400 font-bold px-2 py-1 rounded outline-none border border-slate-700">
                        <option value="5">5x</option>
                        <option value="10" selected>10x</option>
                        <option value="20">20x</option>
                        <option value="50">50x</option>
                        <option value="75">75x</option>
                    </select>
                </div>
                <button onclick="saveSettings()" class="mt-3 bg-emerald-600 hover:bg-emerald-500 text-black font-bold px-3 py-1 rounded transition">KAYDET</button>
            </div>

            <div class="flex space-x-4 text-xs text-slate-400">
                <div>Taranan Parite: <span id="scanned-count" class="text-white font-bold">0</span></div>
                <div>Son Tarama: <span id="last-scan" class="text-white font-bold">-</span></div>
            </div>
        </div>

        <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
            <div class="card p-3 rounded-xl lg:col-span-2 h-[460px] flex flex-col">
                <div class="flex justify-between items-center mb-2 px-1">
                    <span id="chart-title" class="text-xs font-bold text-emerald-400 tracking-wider">CANLI GRAFİK (5M)</span>
                    <span id="chart-levels" class="text-[11px] text-slate-400 space-x-2"></span>
                </div>
                <div id="tv-container" class="w-full flex-1 rounded overflow-hidden"></div>
            </div>

            <div class="card p-4 rounded-xl flex flex-col justify-between h-[460px]">
                <div>
                    <h2 class="text-xs font-semibold text-slate-400 mb-2 uppercase tracking-wider">Seçili Parite Giriş Gerekçesi</h2>
                    <div id="active-rationale" class="space-y-2 text-xs">
                        <div class="text-slate-500 italic">Tablodan bir parite seçin...</div>
                    </div>
                </div>
                <div class="mt-4">
                    <h3 class="text-[10px] font-semibold text-slate-500 mb-1 uppercase">Sistem Logları</h3>
                    <div id="log-box" class="bg-black/50 p-2 rounded text-[11px] text-emerald-500/80 font-mono h-28 overflow-y-auto space-y-1"></div>
                </div>
            </div>
        </div>

        <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <div class="card p-4 rounded-xl">
                <h2 class="text-xs font-semibold text-emerald-400 mb-3 flex items-center">
                    <span class="w-2 h-2 bg-emerald-400 rounded-full mr-2"></span> AKTİF POZİSYONLAR (Grafik için Tıkla)
                </h2>
                <div class="overflow-x-auto max-h-60 overflow-y-auto">
                    <table class="w-full text-left text-[11px]">
                        <thead class="text-slate-500 border-b border-slate-800">
                            <tr>
                                <th class="pb-2">PARİTE</th>
                                <th class="pb-2">YÖN/KALDIRAÇ</th>
                                <th class="pb-2">TEMİNAT (M.)</th>
                                <th class="pb-2">GİRİŞ</th>
                                <th class="pb-2">STOP (SL)</th>
                                <th class="pb-2">TP1 / TP2</th>
                            </tr>
                        </thead>
                        <tbody id="active-pos-table" class="divide-y divide-slate-800/50"></tbody>
                    </table>
                </div>
            </div>

            <div class="card p-4 rounded-xl">
                <h2 class="text-xs font-semibold text-sky-400 mb-3 flex items-center">
                    <span class="w-2 h-2 bg-sky-400 rounded-full mr-2"></span> KAPANAN İŞLEMLER RAPORU
                </h2>
                <div class="overflow-x-auto max-h-60 overflow-y-auto">
                    <table class="w-full text-left text-[11px]">
                        <thead class="text-slate-500 border-b border-slate-800">
                            <tr>
                                <th class="pb-2">PARİTE</th>
                                <th class="pb-2">PNL ($)</th>
                                <th class="pb-2">NEDEN AÇILDI?</th>
                                <th class="pb-2">NEDEN KAPANDI?</th>
                            </tr>
                        </thead>
                        <tbody id="history-table" class="divide-y divide-slate-800/50"></tbody>
                    </table>
                </div>
            </div>
        </div>

        <script>
            let chart = null;
            let candleSeries = null;
            let currentSymbol = localStorage.getItem("selected_sym") || "BTC/USDT:USDT";
            let selectedPos = null;
            let priceLines = [];
            let lastPositions = [];

            function initChart() {
                const container = document.getElementById('tv-container');
                container.innerHTML = '';
                chart = LightweightCharts.createChart(container, {
                    layout: { background: { color: '#121824' }, textColor: '#94a3b8' },
                    grid: { vertLines: { color: '#1e293b' }, horzLines: { color: '#1e293b' } },
                    crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
                    timeScale: { timeVisible: true, secondsVisible: false }
                });
                candleSeries = chart.addCandlestickSeries({
                    upColor: '#10b981', downColor: '#ef4444',
                    borderUpColor: '#10b981', borderDownColor: '#ef4444',
                    wickUpColor: '#10b981', wickDownColor: '#ef4444'
                });
            }

            async function loadChartCandles(symbol, posData = null, isLiveTick = false) {
                try {
                    const res = await fetch(`/api/candles/${encodeURIComponent(symbol)}`);
                    const candles = await res.json();
                    if (candles.length > 0) {
                        candleSeries.setData(candles);
                        if (!isLiveTick) {
                            chart.timeScale().fitContent();
                        }
                    }

                    if (!isLiveTick) {
                        priceLines.forEach(l => candleSeries.removePriceLine(l));
                        priceLines = [];

                        document.getElementById('chart-title').innerText = `${symbol} (5M)`;

                        if (posData) {
                            const entryLine = candleSeries.createPriceLine({
                                price: posData.entry,
                                color: '#38bdf8',
                                lineWidth: 2,
                                lineStyle: LightweightCharts.LineStyle.Solid,
                                axisLabelVisible: true,
                                title: 'GİRİŞ',
                            });
                            const slLine = candleSeries.createPriceLine({
                                price: posData.sl,
                                color: '#ef4444',
                                lineWidth: 2,
                                lineStyle: LightweightCharts.LineStyle.Dashed,
                                axisLabelVisible: true,
                                title: 'STOP (SL)',
                            });
                            const tp1Line = candleSeries.createPriceLine({
                                price: posData.tp1,
                                color: '#10b981',
                                lineWidth: 2,
                                lineStyle: LightweightCharts.LineStyle.Dashed,
                                axisLabelVisible: true,
                                title: 'TP1',
                            });
                            const tp2Line = candleSeries.createPriceLine({
                                price: posData.tp2,
                                color: '#059669',
                                lineWidth: 2,
                                lineStyle: LightweightCharts.LineStyle.Dashed,
                                axisLabelVisible: true,
                                title: 'TP2',
                            });
                            priceLines.push(entryLine, slLine, tp1Line, tp2Line);

                            document.getElementById('chart-levels').innerHTML = `
                                <span class="text-sky-400 font-mono">Giriş: ${posData.entry}</span> | 
                                <span class="text-red-400 font-mono">SL: ${posData.sl.toFixed(4)}</span> | 
                                <span class="text-emerald-400 font-mono">TP1: ${posData.tp1.toFixed(4)}</span> | 
                                <span class="text-emerald-500 font-mono">TP2: ${posData.tp2.toFixed(4)}</span>
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
                loadChartCandles(pos.symbol, pos, false);
                renderRationale(pos);
            }

            function renderRationale(pos) {
                if (!pos) return;
                document.getElementById('active-rationale').innerHTML = `
                    <div class="flex justify-between items-center mb-2">
                        <span class="font-bold text-base text-white">${pos.symbol}</span>
                        <span class="px-2 py-0.5 rounded text-xs font-bold ${pos.direction === 'LONG' ? 'bg-emerald-500/20 text-emerald-400' : 'bg-red-500/20 text-red-400'}">${pos.direction} (${pos.score} Puan)</span>
                    </div>
                    <div class="space-y-1 text-slate-300">
                        ${pos.reasons.map(r => `<div class="bg-slate-900/60 p-1.5 rounded border border-slate-800">✓ ${r}</div>`).join('')}
                    </div>
                    <div class="mt-2 p-2 bg-black/40 rounded border border-slate-800 text-[11px] space-y-1">
                        <div class="text-slate-400">Kaldıraç & Teminat: <span class="text-white font-bold">${pos.leverage}x İzole ($${pos.margin})</span></div>
                        <div class="text-red-400">Stop-Loss: <span class="font-mono">${pos.sl.toFixed(4)}</span> (Maks Kayıp: $${pos.max_loss})</div>
                        <div class="text-emerald-400">TP1 / TP2: <span class="font-mono">${pos.tp1.toFixed(4)} / ${pos.tp2.toFixed(4)}</span></div>
                    </div>
                `;
            }

            async function saveSettings() {
                const total_balance = parseFloat(document.getElementById('input-balance').value);
                const risk_pct = parseFloat(document.getElementById('input-risk').value);
                const leverage = parseInt(document.getElementById('input-leverage').value);

                await fetch('/api/update_settings', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({total_balance, risk_pct, leverage})
                });
            }

            async function updateDashboard() {
                try {
                    const res = await fetch('/api/state');
                    const data = await res.json();

                    document.getElementById('scanned-count').innerText = data.scanned_count;
                    document.getElementById('last-scan').innerText = data.last_scan_time;

                    const logBox = document.getElementById('log-box');
                    logBox.innerHTML = data.logs.map(l => `<div>${l}</div>`).join('');

                    lastPositions = data.active_positions;
                    const activeTbody = document.getElementById('active-pos-table');
                    activeTbody.innerHTML = data.active_positions.map((p, idx) => `
                        <tr class="hover:bg-slate-800/80 cursor-pointer ${selectedPos && selectedPos.symbol === p.symbol ? 'bg-slate-800/60' : ''}" onclick="selectPosition(lastPositions[${idx}])">
                            <td class="py-2 font-bold text-white">${p.symbol}</td>
                            <td class="${p.direction === 'LONG' ? 'text-emerald-400' : 'text-red-400'} font-bold">${p.direction} (${p.leverage}x)</td>
                            <td class="text-white font-mono">$${p.margin}</td>
                            <td class="font-mono">${p.entry}</td>
                            <td class="text-red-400 font-mono">${p.sl.toFixed(4)}</td>
                            <td class="text-emerald-400 font-mono">${p.tp1.toFixed(4)} / ${p.tp2.toFixed(4)}</td>
                        </tr>
                    `).join('');

                    const histTbody = document.getElementById('history-table');
                    histTbody.innerHTML = data.trade_history.map(h => `
                        <tr class="hover:bg-slate-800/30">
                            <td class="py-2 font-bold">${h.symbol}<br><span class="text-[9px] text-slate-500">${h.close_time}</span></td>
                            <td class="font-bold ${h.realized_pnl >= 0 ? 'text-emerald-400' : 'text-red-400'}">
                                ${h.realized_pnl >= 0 ? '+' : ''}$${h.realized_pnl}<br><span class="text-[9px]">%${h.pnl_pct}</span>
                            </td>
                            <td class="text-slate-300 text-[10px]">${h.open_reasons.join('<br>')}</td>
                            <td class="text-sky-300 text-[10px] font-semibold">${h.close_reason}</td>
                        </tr>
                    `).join('');

                    // Canlı grafik tick güncellemesi
                    if (currentSymbol) {
                        loadChartCandles(currentSymbol, selectedPos, true);
                    }

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

            initChart();
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
