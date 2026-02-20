import requests
import pandas as pd
import numpy as np
from datetime import datetime
import time
import warnings
import json

warnings.filterwarnings('ignore')

BASE_URL = "https://data912.com"

TICKERS_ARG = [
    'ALUA', 'BBAR', 'BMA', 'BYMA', 'CEPU', 'COME', 'CRES', 'CVH',
    'EDN', 'GGAL', 'LOMA', 'MIRG', 'PAMP', 'SUPV', 'TECO2', 'TGNO4',
    'TGSU2', 'TRAN', 'TXAR', 'VALO', 'YPFD', 'AGRO', 'BHIP', 'BOLT',
    'BPAT', 'CGPA2', 'CTIO', 'DGCE', 'FERR', 'HARG', 'INVJ', 'LEDE',
    'LONG', 'METR', 'MOLA', 'MOLI', 'MORI', 'OEST', 'RICH', 'SAMI'
]

def fetch_ticker(ticker, max_retries=3):
    url = f"{BASE_URL}/historical/stocks/{ticker}"
    for attempt in range(max_retries):
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            data = r.json()
            if not data:
                return None
            df = pd.DataFrame(data)
            df['ticker'] = ticker
            df['date'] = pd.to_datetime(df['date'])
            df = df.rename(columns={
                'o': 'open', 'h': 'high', 'l': 'low',
                'c': 'close', 'v': 'volume',
                'dr': 'daily_return'
            })
            return df.sort_values('date').reset_index(drop=True)
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(1)
    return None

def load_all_tickers(tickers, delay=0.4):
    frames = []
    for tk in tickers:
        df = fetch_ticker(tk)
        if df is not None and not df.empty:
            frames.append(df)
        time.sleep(delay)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

df_raw = load_all_tickers(TICKERS_ARG)

if df_raw.empty:
    raise ValueError("No se pudieron descargar datos")

metrics_rows = []
for tk, g in df_raw.groupby('ticker'):
    g = g.sort_values('date').copy()
    close = g['close'].values
    vol = g['volume'].values
    if len(close) < 2:
        continue
    close_last = close[-1]
    close_prev = close[-2]
    daily_ret = ((close_last / close_prev) - 1) * 100
    date_last = g['date'].iloc[-1]
    vol_last = vol[-1]
    vol_avg20 = np.mean(vol[-21:-1]) if len(vol) > 21 else np.mean(vol[:-1])
    vol_rel20 = (vol_last / vol_avg20) if vol_avg20 > 0 else np.nan
    metrics_rows.append({
        'ticker': tk,
        'close_last': round(close_last, 2),
        'date_last': date_last,
        'daily_ret': round(daily_ret, 2),
        'volume_last': int(vol_last),
        'vol_rel20': round(vol_rel20, 2),
    })

metrics = pd.DataFrame(metrics_rows).dropna(subset=['daily_ret'])

total_stocks = len(metrics)
advances = len(metrics[metrics['daily_ret'] > 0])
declines = len(metrics[metrics['daily_ret'] < 0])
unchanged = total_stocks - advances - declines
ad_ratio = advances / declines if declines > 0 else float('inf')
avg_change = metrics['daily_ret'].mean()
median_change = metrics['daily_ret'].median()
std_change = metrics['daily_ret'].std()

if ad_ratio >= 2.0 and avg_change >= 0.5:
    market_sentiment = "Alcista amplio"
    sentiment_color = "#22c55e"
elif ad_ratio >= 1.2 and avg_change >= 0:
    market_sentiment = "Alcista moderado"
    sentiment_color = "#86efac"
elif ad_ratio <= 0.5 and avg_change <= -0.5:
    market_sentiment = "Bajista amplio"
    sentiment_color = "#ef4444"
elif ad_ratio <= 0.8 and avg_change <= 0:
    market_sentiment = "Bajista moderado"
    sentiment_color = "#fca5a5"
else:
    market_sentiment = "Mixto / Sin tendencia"
    sentiment_color = "#94a3b8"

def build_summary():
    lines = []
    lines.append(
        f"Panel General de acciones argentinas: {total_stocks} instrumentos analizados, "
        f"{advances} alcistas / {declines} bajistas (ratio A/D {ad_ratio:.2f}), "
        f"cambio promedio {avg_change:+.2f}%."
    )
    if ad_ratio >= 1.5:
        lines.append("Amplitud positiva: más de la mitad del panel en verde.")
    elif ad_ratio <= 0.67:
        lines.append("Amplitud negativa: deterioro generalizado en el panel.")
    else:
        lines.append("Amplitud mixta: movimiento sin dirección dominante.")
    top_gain = metrics.loc[metrics['daily_ret'].idxmax()]
    top_loss = metrics.loc[metrics['daily_ret'].idxmin()]
    lines.append(
        f"Mayor ganador: {top_gain['ticker']} ({top_gain['daily_ret']:+.2f}%). "
        f"Mayor perdedor: {top_loss['ticker']} ({top_loss['daily_ret']:+.2f}%)."
    )
    return " ".join(lines)

now_str = datetime.now().strftime('%Y-%m-%d  %H:%M')

data_export = {
    "generated_at": now_str,
    "market_summary": {
        "total_stocks": total_stocks,
        "advances": advances,
        "declines": declines,
        "unchanged": unchanged,
        "ad_ratio": round(ad_ratio, 3),
        "avg_change": round(avg_change, 3),
        "median_change": round(median_change, 3),
        "std_change": round(std_change, 3),
        "sentiment": market_sentiment,
        "sentiment_color": sentiment_color,
        "executive_summary": build_summary(),
    },
    "tickers": []
}

for tk, g in df_raw.groupby('ticker'):
    g = g.sort_values('date').copy()
    history = []
    for _, row in g.iterrows():
        history.append({
            "date": row['date'].strftime('%Y-%m-%d'),
            "open": round(float(row['open']), 2),
            "high": round(float(row['high']), 2),
            "low": round(float(row['low']), 2),
            "close": round(float(row['close']), 2),
            "volume": int(row['volume']),
        })

    ticker_metrics = metrics[metrics['ticker'] == tk]
    if ticker_metrics.empty:
        continue
    m = ticker_metrics.iloc[0]

    data_export["tickers"].append({
        "ticker": tk,
        "close_last": float(m['close_last']),
        "daily_ret": float(m['daily_ret']),
        "volume_last": int(m['volume_last']),
        "vol_rel20": float(m['vol_rel20']),
        "history": history
    })

with open('data.json', 'w', encoding='utf-8') as f:
    json.dump(data_export, f, ensure_ascii=False)

print(f"data.json generado: {now_str}")
