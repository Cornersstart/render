"""
TradeSniper Bot — V8 FULL SQUAD + GOLDEN RECOVERY DOCTRINE
Doutrina : ONE TARGET, ONE KILL  |  $20 PROFIT LOCK = LAW
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🏆 GOLDEN DOCTRINE (backtest 103 dias — actualizada Abr/2026):
  🥇 POL — E09 ICHIMOKU 1H            (97.4% hit, +$1.222 líq.)  HOLD
  🌊 SOL — E06 SUPERTREND 15m         (95.0% hit, +$515 líq.)    HOLD
  🎯 XRP — E07 RSI DIV + VWAP 15m     (PF 2.38, alta convicção)  STRICT
LEGACY SQUAD (mantido como rede):
  💧 ETH — VWAP KISS M15                                          HOLD
  🔥 SOL — ENGOLFO M15  (extra trigger)                           HOLD
  🛡️ ADA — ORDER BLOCK 1H                                         STRICT
  🛡️ XRP — ORDER BLOCK 1H                                         STRICT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
HOLD pairs (POL/ETH/SOL): sem SL apertado, só circuit breaker -7%
STRICT pairs (ADA/XRP/DOGE): SL fixo 1.5% — backtest mostra ruína se segurar
GLOBAL CIRCUIT BREAKER: -7% drawdown = fecho imediato (protege banca $900)
Trailing +0.8% cb 1% | 5× Isolated ALL-IN | OKX Perpetual SWAP (hedge mode)
"""

import base64
import hashlib
import hmac
import http.server
import json
import logging
import os
import socketserver
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pandas_ta as ta
import requests

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("TradeSniper")

# ── Credenciais ────────────────────────────────────────────────────────────────
OKX_API_KEY    = os.environ.get("OKX_API_KEY", "").strip()
OKX_SECRET_KEY = os.environ.get("OKX_SECRET_KEY", "").strip()
OKX_PASSPHRASE = os.environ.get("OKX_PASSPHRASE", "").strip()
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN",
                  os.environ.get("TELEGRAM_BOT_TOKEN", "")).strip()
TELEGRAM_CHAT  = os.environ.get("CHAT_ID",
                  os.environ.get("TELEGRAM_CHAT_ID", "")).strip()

OKX_BASE  = "https://www.okx.com/api/v5"
LEVERAGE  = 5
RISK_FRAC = 1.0    # ALL-IN

# ── DUO DE ELITE ───────────────────────────────────────────────────────────────
DUO_SL_PCT          = 1.2    # SL inicial (protecção antes do trailing activar)
DUO_COOLDOWN        = 3600   # 60 min cooldown após trade
TRAIL_ACTIVATE_PCT  = 0.8    # trailing activa quando lucro ≥ +0.8%
TRAIL_CALLBACK      = 0.01   # distância trailing = 1.0%

DUO_ETH    = "ETH-USDT-SWAP"
DUO_SOL    = "SOL-USDT-SWAP"
SHIELD_ADA = "ADA-USDT-SWAP"
SHIELD_XRP = "XRP-USDT-SWAP"
GOLD_POL   = "POL-USDT-SWAP"      # 🥇 Golden pair — Ichimoku 1H exclusivo
GOLD_DOGE  = "DOGE-USDT-SWAP"     # incluído na lista STRICT (regra de hold)
ALL_SYMS   = [DUO_ETH, DUO_SOL, SHIELD_ADA, SHIELD_XRP, GOLD_POL, GOLD_DOGE]

# ── ORDER BLOCK DEFENSE (ADA / XRP — 1H) ──────────────────────────────────────
OB_LOOKBACK    = 20    # velas 1H para procurar blocos de ordem
OB_VOL_MULT    = 2.0   # volume do expansion candle ≥ 2× média
OB_BODY_MULT   = 1.5   # corpo do expansion candle ≥ 1.5× média
OB_TOL_PCT     = 0.4   # tolerância ±0.4% para toque no midpoint
OB_SL_PCT      = 1.0   # SL da estratégia OB (diferente do DUO)

# ── $20 PROFIT LOCK ────────────────────────────────────────────────────────────
PROFIT_LOCK_USD = 20.0  # move o SL para garantir lucro mínimo de $20

# ══════════════════════════════════════════════════════════════════════════════
# GOLDEN RECOVERY DOCTRINE — regras de hold por par (Abr/2026)
# ══════════════════════════════════════════════════════════════════════════════
HOLD_PAIRS    = {GOLD_POL, DUO_ETH, DUO_SOL}             # sem SL apertado
STRICT_PAIRS  = {SHIELD_ADA, SHIELD_XRP, GOLD_DOGE}      # SL fixo 1.5%
STRICT_SL_PCT = 1.5
# HOLD: SL na corretora é REDE DE SEGURANÇA (caso o bot/monitor caia).
# O controlo primário é o CIRCUIT_BREAKER no monitor (7.0%) — dispara primeiro.
# Folga de 1pp evita corrida dupla CB-vs-exchange-SL no mesmo tick.
HOLD_SL_PCT   = 8.0
CIRCUIT_BREAKER_PCT = 7.0   # global — monitor fecha SEMPRE a -7% em preço

# ── E06 SUPERTREND (SOL 15m) ──────────────────────────────────────────────────
ST_PERIOD = 10
ST_MULT   = 3.0

# ── E07 RSI DIVERGENCE + VWAP (XRP 15m) ───────────────────────────────────────
RSI_DIV_LOOKBACK = 30      # janela para detectar divergência
RSI_DIV_MIN_GAP  = 5       # diferença mínima entre topos/fundos do RSI

# ── E09 ICHIMOKU CLOUD (POL 1H) ──────────────────────────────────────────────
ICHI_TENKAN  = 9
ICHI_KIJUN   = 26
ICHI_SENKOU  = 52

# ── DISCIPLINA DE SNIPER ──────────────────────────────────────────────────────
LOCKDOWN_SECS    = 900   # 15 min de silêncio total após qualquer tentativa de ordem
VWAP_BODY_MIN    = 0.55  # corpo/range mínimo para confirmar VWAP KISS (era 0.40)
VWAP_DIST_PCT    = 0.15  # distância mínima da VWAP após cross (em %)

# ── Estado global ─────────────────────────────────────────────────────────────
_duo_in_trade:       bool  = False
_duo_cooldown_until: float = 0.0
_lockdown_until:     float = 0.0   # bloqueio total de novos sinais (anti ping-pong)
_duo_lock                  = threading.Lock()

_bot_authorized: bool = True
_auth_lock             = threading.Lock()

STATE_FILE = Path(__file__).parent / "bot_state.json"

# ── Persistência ─────────────────────────────────────────────────────────────
def _save_state(authorized: bool) -> None:
    try:
        STATE_FILE.write_text(json.dumps({"authorized": authorized,
            "updatedAt": datetime.now(timezone.utc).isoformat()}, indent=2))
    except Exception as e:
        log.debug("save_state: %s", e)

def _load_state() -> bool:
    try:
        if STATE_FILE.exists():
            return bool(json.loads(STATE_FILE.read_text()).get("authorized", True))
    except Exception:
        pass
    return True

# ── OKX — assinatura ──────────────────────────────────────────────────────────
def _sign(ts: str, method: str, path: str, body: str = "") -> str:
    msg = ts + method + path + body
    return base64.b64encode(
        hmac.new(OKX_SECRET_KEY.encode(), msg.encode(), hashlib.sha256).digest()
    ).decode()

def _headers(method: str, path: str, body: str = "") -> dict:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return {
        "Content-Type":         "application/json",
        "OK-ACCESS-KEY":        OKX_API_KEY,
        "OK-ACCESS-SIGN":       _sign(ts, method, path, body),
        "OK-ACCESS-TIMESTAMP":  ts,
        "OK-ACCESS-PASSPHRASE": OKX_PASSPHRASE,
    }

def _has_creds() -> bool:
    return bool(OKX_API_KEY and OKX_SECRET_KEY and OKX_PASSPHRASE)

# ── OKX — candles ─────────────────────────────────────────────────────────────
def okx_candles(inst_id: str, bar: str = "15m", limit: int = 300) -> pd.DataFrame:
    r    = requests.get(f"{OKX_BASE}/market/candles?instId={inst_id}&bar={bar}&limit={limit}", timeout=12)
    data = r.json()
    if data.get("code") != "0":
        raise RuntimeError(f"candles {inst_id}: {data.get('msg')}")
    df = pd.DataFrame(data["data"],
         columns=["ts","open","high","low","close","vol","volCcy","volCcyQuote","confirm"])
    df = df[["ts","open","high","low","close","vol"]].copy()
    for c in ["open","high","low","close","vol"]:
        df[c] = pd.to_numeric(df[c])
    df["ts"] = pd.to_datetime(df["ts"].astype("int64"), unit="ms", utc=True)
    df.sort_values("ts", inplace=True)
    df.set_index("ts", inplace=True)
    return df

# ── OKX — saldo ───────────────────────────────────────────────────────────────
def okx_balance() -> float | None:
    """Saldo USDT DISPONÍVEL (free margin) — usado para calcular qty da próxima ordem."""
    if not _has_creds(): return None
    path = "/api/v5/account/balance?ccy=USDT"
    try:
        r = requests.get(f"https://www.okx.com{path}", headers=_headers("GET", path), timeout=8)
        d = r.json()
        if d.get("code") != "0": return None
        details = d["data"][0].get("details", [])
        usdt = next((x for x in details if x["ccy"] == "USDT"), None)
        return float(usdt["availBal"]) if usdt else 0.0
    except Exception as e:
        log.warning("balance: %s", e)
        return None

def okx_balance_full() -> tuple[float, float] | None:
    """Retorna (total_equity, available) em USDT — para mostrar 'em uso' no status."""
    if not _has_creds(): return None
    path = "/api/v5/account/balance?ccy=USDT"
    try:
        r = requests.get(f"https://www.okx.com{path}", headers=_headers("GET", path), timeout=8)
        d = r.json()
        if d.get("code") != "0": return None
        details = d["data"][0].get("details", [])
        usdt = next((x for x in details if x["ccy"] == "USDT"), None)
        if not usdt: return (0.0, 0.0)
        return (float(usdt.get("eq", 0) or 0), float(usdt.get("availBal", 0) or 0))
    except Exception as e:
        log.warning("balance_full: %s", e)
        return None

# ── OKX — posição ─────────────────────────────────────────────────────────────
def okx_has_position(inst_id: str, pos_side: str | None = None) -> bool:
    if not _has_creds(): return False
    path = f"/api/v5/account/positions?instType=SWAP&instId={inst_id}"
    try:
        r = requests.get(f"https://www.okx.com{path}", headers=_headers("GET", path), timeout=8)
        for pos in r.json().get("data", []):
            if float(pos.get("pos", 0) or 0) != 0:
                if pos_side is None or pos.get("posSide") == pos_side:
                    return True
    except Exception as e:
        log.warning("has_position %s: %s", inst_id, e)
    return False

def okx_any_position_open(syms: list[str]) -> tuple[str, str] | None:
    """Verifica se EXISTE qualquer posição aberta em qualquer um dos símbolos
    (ambos os lados, long e short). Retorna (sym, posSide) ou None.

    Usado pela ONE TARGET DOCTRINE — se já existe trade aberta em qualquer par,
    nenhum novo sinal é executado, mesmo se o lock interno estiver desactivado
    (ex.: posição manual ou ordem fantasma).
    """
    if not _has_creds(): return None
    for sym in syms:
        for ps in ("long", "short"):
            if okx_has_position(sym, ps):
                return (sym, ps)
    return None

def okx_get_position(inst_id: str, pos_side: str) -> dict | None:
    if not _has_creds(): return None
    path = f"/api/v5/account/positions?instType=SWAP&instId={inst_id}"
    try:
        r = requests.get(f"https://www.okx.com{path}", headers=_headers("GET", path), timeout=8)
        for pos in r.json().get("data", []):
            if pos.get("instId") == inst_id and pos.get("posSide") == pos_side and float(pos.get("pos", 0) or 0) != 0:
                return pos
    except Exception as e:
        log.warning("get_position %s: %s", inst_id, e)
    return None

# ── OKX — leverage ────────────────────────────────────────────────────────────
def okx_set_leverage(inst_id: str) -> None:
    if not _has_creds(): return
    path = "/api/v5/account/set-leverage"
    for ps in ("long", "short"):
        payload = {"instId": inst_id, "lever": str(LEVERAGE), "mgnMode": "isolated", "posSide": ps}
        body = json.dumps(payload)
        try:
            r = requests.post(f"https://www.okx.com{path}", headers=_headers("POST", path, body), data=body, timeout=8)
            d = r.json()
            if d.get("code") == "0":
                log.info("leverage %s %s: %dx ✓", inst_id, ps, LEVERAGE)
            else:
                log.warning("leverage %s %s: %s", inst_id, ps, d.get("msg"))
        except Exception as e:
            log.warning("leverage %s %s: %s", inst_id, ps, e)

# ── OKX — ticker ──────────────────────────────────────────────────────────────
def okx_ticker(inst_id: str) -> float:
    r = requests.get(f"{OKX_BASE}/market/ticker?instId={inst_id}", timeout=8)
    d = r.json()
    if d.get("code") != "0":
        raise RuntimeError(f"ticker {inst_id}: {d.get('msg')}")
    return float(d["data"][0]["last"])

# ── OKX — lot size ────────────────────────────────────────────────────────────
def okx_lot_size(inst_id: str) -> float:
    try:
        r = requests.get(f"{OKX_BASE}/public/instruments?instType=SWAP&instId={inst_id}", timeout=8)
        d = r.json()
        if d.get("code") == "0" and d.get("data"):
            return float(d["data"][0].get("ctVal", 1))
    except Exception:
        pass
    return 1.0

def calc_qty(inst_id: str, price: float, balance: float) -> int:
    ct_val = okx_lot_size(inst_id)
    return max(1, int(balance * RISK_FRAC * LEVERAGE / (price * ct_val)))

# ── OKX — market order ────────────────────────────────────────────────────────
_SIDE_PS = {"buy": "long", "sell": "short"}

def okx_order(inst_id: str, side: str, qty: int) -> dict:
    if not _has_creds(): raise RuntimeError("Sem credenciais OKX.")
    path = "/api/v5/trade/order"
    body = json.dumps({"instId": inst_id, "tdMode": "isolated", "side": side,
                       "posSide": _SIDE_PS[side], "ordType": "market", "sz": str(qty)})
    r = requests.post(f"https://www.okx.com{path}", headers=_headers("POST", path, body), data=body, timeout=10)
    d = r.json()
    # Tenta extrair o sCode/sMsg detalhado primeiro (a OKX põe o erro real aqui)
    item = (d.get("data") or [{}])[0]
    sCode = str(item.get("sCode", ""))
    sMsg  = item.get("sMsg", "")
    if sCode and sCode not in ("0",):
        raise RuntimeError(f"order {inst_id} side={side} qty={qty}: sCode={sCode} sMsg='{sMsg}' | top msg='{d.get('msg')}'")
    if d.get("code") != "0":
        raise RuntimeError(f"order {inst_id} side={side} qty={qty}: code={d.get('code')} msg='{d.get('msg')}' raw={d}")
    return d

# ── OKX — fecho de posição a mercado (Profit Lock) ────────────────────────────
def okx_close_market(inst_id: str, pos_side: str, sz: int) -> dict:
    """Fecha uma posição existente a mercado (hedge mode OKX).

    pos_side: 'long' ou 'short'  — a posição a fechar (NÃO o lado a abrir)
    sz      : número de contratos a fechar (= tamanho total da posição)
    """
    if not _has_creds(): raise RuntimeError("Sem credenciais OKX.")
    path       = "/api/v5/trade/order"
    close_side = "sell" if pos_side == "long" else "buy"
    body = json.dumps({"instId": inst_id, "tdMode": "isolated",
                       "side": close_side, "posSide": pos_side,
                       "ordType": "market", "sz": str(sz)})
    r = requests.post(f"https://www.okx.com{path}",
                      headers=_headers("POST", path, body), data=body, timeout=10)
    d = r.json()
    if d.get("code") != "0":
        raise RuntimeError(f"close_market {inst_id}: {d.get('msg')}")
    item = d["data"][0]
    if item.get("sCode") not in ("0", 0):
        raise RuntimeError(f"close_market {inst_id}: sCode={item.get('sCode')} {item.get('sMsg')}")
    return d

# ── OKX — cancela todos os algos pendentes de um instrumento/posSide ──────────
def okx_cancel_all_algos(inst_id: str, pos_side: str) -> None:
    """Cancela SL/Trailing pendentes para inst_id+pos_side antes de colocar novo SL."""
    if not _has_creds(): return
    qpath  = "/api/v5/trade/orders-algo-pending"
    params = f"?instType=SWAP&instId={inst_id}&ordType=conditional,move_order_stop"
    try:
        r = requests.get(f"https://www.okx.com{qpath}{params}",
                         headers=_headers("GET", f"{qpath}{params}"), timeout=8)
        algos = r.json().get("data", [])
        to_cancel = [{"algoId": o["algoId"], "instId": inst_id}
                     for o in algos if o.get("posSide") == pos_side]
        if not to_cancel:
            return
        cpath = "/api/v5/trade/cancel-algos"
        cbody = json.dumps(to_cancel)
        requests.post(f"https://www.okx.com{cpath}",
                      headers=_headers("POST", cpath, cbody), data=cbody, timeout=8)
        log.info("🗑️ Algos cancelados: %d ordens [%s %s]", len(to_cancel), inst_id, pos_side)
    except Exception as e:
        log.error("cancel_all_algos %s: %s", inst_id, e)

# ── OKX — SL inicial (protecção antes do trailing activar) ────────────────────
def okx_initial_sl(inst_id: str, pos_side: str, sz: int, sl_px: float) -> str | None:
    if not _has_creds(): return None
    close_side = "sell" if pos_side == "long" else "buy"
    path = "/api/v5/trade/order-algo"
    body = json.dumps({"instId": inst_id, "tdMode": "isolated", "side": close_side,
                       "posSide": pos_side, "ordType": "conditional", "sz": str(sz),
                       "slTriggerPx": f"{sl_px:.6f}", "slOrdPx": "-1"})
    try:
        r = requests.post(f"https://www.okx.com{path}", headers=_headers("POST", path, body), data=body, timeout=10)
        d = r.json()
        if d.get("code") == "0":
            algo_id = d["data"][0].get("algoId", "?")
            log.info("🛡️ SL inicial algoId=%s SL=%.5f", algo_id, sl_px)
            return algo_id
        log.error("SL inicial recusado: %s", d)
    except Exception as e:
        log.error("SL inicial erro: %s", e)
    return None

# ── OKX — Trailing Stop (move_order_stop) ─────────────────────────────────────
def okx_trailing_stop(inst_id: str, pos_side: str, sz: int,
                      activate_px: float, callback: float = TRAIL_CALLBACK) -> str | None:
    if not _has_creds(): return None
    close_side = "sell" if pos_side == "long" else "buy"
    path = "/api/v5/trade/order-algo"
    body = json.dumps({"instId": inst_id, "tdMode": "isolated", "side": close_side,
                       "posSide": pos_side, "ordType": "move_order_stop", "sz": str(sz),
                       "activePx": f"{activate_px:.6f}",
                       "callbackRatio": f"{callback:.4f}",
                       "ordPx": "-1"})
    try:
        r = requests.post(f"https://www.okx.com{path}", headers=_headers("POST", path, body), data=body, timeout=10)
        d = r.json()
        if d.get("code") == "0":
            algo_id = d["data"][0].get("algoId", "?")
            log.info("📡 TRAILING STOP algoId=%s activePx=%.5f callback=%.1f%%",
                     algo_id, activate_px, callback * 100)
            return algo_id
        log.error("Trailing stop recusado: %s", d)
    except Exception as e:
        log.error("Trailing stop erro: %s", e)
    return None

# ── Telegram ──────────────────────────────────────────────────────────────────
def tg(msg: str, chat_id: str | int | None = None) -> None:
    token = TELEGRAM_TOKEN
    cid   = chat_id or TELEGRAM_CHAT
    if not token or not cid: return
    try:
        requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                      json={"chat_id": cid, "text": msg, "parse_mode": "HTML"}, timeout=10)
    except Exception as e:
        log.warning("tg: %s", e)

# ══════════════════════════════════════════════════════════════════════════════
# SINAIS
# ══════════════════════════════════════════════════════════════════════════════

def vwap_kiss_signal(df: pd.DataFrame) -> str | None:
    """VWAP KISS — ETH M15 (DISCIPLINA REFORÇADA — anti ping-pong).

    Confirmações exigidas (todas obrigatórias):
      1. Cross da VWAP diária na vela anterior (cur cruza, prv estava do outro lado)
      2. Vela com corpo >= VWAP_BODY_MIN (55%) do range total
      3. Distância mínima de VWAP_DIST_PCT (0.15%) entre close e VWAP após o cross
      4. PRV-2 (vela anterior à do cruce) também estava do mesmo lado da VWAP que prv
         → evita reversões de uma única vela ("limpa" volatilidade rápida)
      5. EMA200 alinhada com a direcção (LONG só acima, SHORT só abaixo)
      6. RSI mais apertado: LONG 45–65 / SHORT 35–55
    """
    if len(df) < 210: return None
    df = df.copy()
    df["ema200"] = ta.ema(df["close"], length=200)
    df["date"]   = df.index.date
    df["tp"]     = (df["high"] + df["low"] + df["close"]) / 3
    df["tpvol"]  = df["tp"] * df["vol"]
    df["cvtpv"]  = df.groupby("date")["tpvol"].cumsum()
    df["cvol"]   = df.groupby("date")["vol"].cumsum()
    df["vwap"]   = df["cvtpv"] / df["cvol"]
    df["rsi"]    = ta.rsi(df["close"], length=14)

    cur, prv, prv2 = df.iloc[-2], df.iloc[-3], df.iloc[-4]
    if any(pd.isna(x) for x in [cur["vwap"], cur["rsi"], cur["ema200"], prv2["vwap"]]):
        return None

    ema200 = cur["ema200"]
    price  = cur["close"]
    vwap   = cur["vwap"]
    rng    = cur["high"] - cur["low"]
    body   = abs(cur["close"] - cur["open"])
    rsi    = cur["rsi"]

    # 1) Corpo forte (≥55%)
    if rng <= 0 or body / rng < VWAP_BODY_MIN:
        return None

    # 2) Distância mínima de VWAP após cross (em %)
    dist_pct = abs(price - vwap) / vwap * 100
    if dist_pct < VWAP_DIST_PCT:
        return None

    # 3) Cross com confirmação prv-2 (3 velas: prv2/prv abaixo + cur acima = LONG, e vice-versa)
    up   = prv2["close"] < prv2["vwap"] and prv["close"] < prv["vwap"] and cur["close"] > cur["vwap"]
    down = prv2["close"] > prv2["vwap"] and prv["close"] > prv["vwap"] and cur["close"] < cur["vwap"]

    # 4) Direccionalidade da vela + EMA200 + RSI apertado
    if up   and cur["close"] > cur["open"] and 45 <= rsi <= 65 and price > ema200: return "buy"
    if down and cur["close"] < cur["open"] and 35 <= rsi <= 55 and price < ema200: return "sell"
    return None

def engolfo_signal(df: pd.DataFrame) -> str | None:
    """ENGOLFO — SOL M15: engolfo corpo + volume 1.3× + RSI + corpo mínimo + EMA200."""
    if len(df) < 210: return None
    df = df.copy()
    # ── Filtro de tendência macro: EMA200 ──────────────────────────────────────
    df["ema200"] = ta.ema(df["close"], length=200)
    df["rsi"]   = ta.rsi(df["close"], length=14)
    cur, prv = df.iloc[-2], df.iloc[-3]
    if pd.isna(cur["ema200"]) or pd.isna(cur["rsi"]): return None
    ema200 = cur["ema200"]
    price  = cur["close"]
    rsi    = cur["rsi"]
    # ── Filtro de corpo mínimo: ≥ 0.25% do preço (candle com força real) ──────
    body = abs(cur["close"] - cur["open"])
    if body < price * 0.0025: return None
    # ── Volume ≥ 1.3× média 20 velas ──────────────────────────────────────────
    vol_avg = df["vol"].iloc[-22:-2].mean()
    if not (vol_avg > 0 and cur["vol"] >= vol_avg * 1.3): return None
    # ── Padrão de engolfo (corpo a corpo) ─────────────────────────────────────
    pb_top = max(prv["open"], prv["close"]); pb_bot = min(prv["open"], prv["close"])
    rb_top = max(cur["open"], cur["close"]); rb_bot = min(cur["open"], cur["close"])
    bull = prv["close"] < prv["open"] and cur["close"] > cur["open"] and rb_bot <= pb_bot and rb_top >= pb_top
    bear = prv["close"] > prv["open"] and cur["close"] < cur["open"] and rb_bot <= pb_bot and rb_top >= pb_top
    # LONG só acima EMA200 + RSI 40-65 | SHORT só abaixo EMA200 + RSI 35-60
    if bull and price > ema200 and 40 <= rsi <= 65: return "buy"
    if bear and price < ema200 and 35 <= rsi <= 60: return "sell"
    return None

# ══════════════════════════════════════════════════════════════════════════════
# GOLDEN DOCTRINE STRATEGIES — E06 / E07 / E09
# ══════════════════════════════════════════════════════════════════════════════

def supertrend_signal(df: pd.DataFrame) -> str | None:
    """🌊 E06 SUPERTREND — SOL 15m  (95% hit rate / hold the hand).

    Dispara no FLIP do Supertrend (longo→curto vira para a outra direcção).
    Filtros: EMA200 alinhada com a direcção; RSI 35–65 (evita extremos).
    """
    if len(df) < 210: return None
    df = df.copy()
    st = ta.supertrend(df["high"], df["low"], df["close"],
                       length=ST_PERIOD, multiplier=ST_MULT)
    if st is None or st.empty: return None
    dir_col = next((c for c in st.columns if c.startswith("SUPERTd_")), None)
    if dir_col is None: return None
    df["st_dir"] = st[dir_col]
    df["ema200"] = ta.ema(df["close"], length=200)
    df["rsi"]    = ta.rsi(df["close"], length=14)

    cur, prv = df.iloc[-2], df.iloc[-3]
    if any(pd.isna(x) for x in [cur["st_dir"], prv["st_dir"], cur["ema200"], cur["rsi"]]):
        return None
    price, ema200, rsi = cur["close"], cur["ema200"], cur["rsi"]
    if not (35 <= rsi <= 65): return None

    # Flip de baixa→alta com EMA200 confirmando alta
    if prv["st_dir"] == -1 and cur["st_dir"] == 1 and price > ema200:
        return "buy"
    # Flip de alta→baixa com EMA200 confirmando baixa
    if prv["st_dir"] == 1 and cur["st_dir"] == -1 and price < ema200:
        return "sell"
    return None

def rsi_div_vwap_signal(df: pd.DataFrame) -> str | None:
    """🎯 E07 RSI DIVERGENCE + VWAP — XRP 15m  (PF 2.38 — alta convicção).

    LONG : preço faz fundo MAIS BAIXO + RSI faz fundo MAIS ALTO  (bullish div)
            + close acima da VWAP diária + RSI a sair de sobrevenda (<40 → >40).
    SHORT: simétrico — preço topo mais alto + RSI topo mais baixo + close < VWAP.
    """
    if len(df) < max(RSI_DIV_LOOKBACK + 5, 50): return None
    df = df.copy()
    df["rsi"]   = ta.rsi(df["close"], length=14)
    df["date"]  = df.index.date
    df["tp"]    = (df["high"] + df["low"] + df["close"]) / 3
    df["tpvol"] = df["tp"] * df["vol"]
    df["vwap"]  = (df.groupby("date")["tpvol"].cumsum()
                   / df.groupby("date")["vol"].cumsum())

    cur, prv = df.iloc[-2], df.iloc[-3]
    if any(pd.isna(x) for x in [cur["rsi"], cur["vwap"], prv["rsi"]]):
        return None

    window = df.iloc[-(RSI_DIV_LOOKBACK + 2):-2]
    if window.empty: return None

    # Bullish divergence: novo mínimo em preço, mínimo de RSI MAIS ALTO
    p_low_idx  = window["low"].idxmin()
    r_at_plow  = df.loc[p_low_idx, "rsi"]
    if (cur["low"] < window["low"].min() * 0.999
        and cur["rsi"] > r_at_plow + RSI_DIV_MIN_GAP
        and cur["close"] > cur["vwap"]
        and prv["rsi"] < 40 and cur["rsi"] >= 40):
        return "buy"

    # Bearish divergence: novo máximo em preço, máximo de RSI MAIS BAIXO
    p_hi_idx   = window["high"].idxmax()
    r_at_phi   = df.loc[p_hi_idx, "rsi"]
    if (cur["high"] > window["high"].max() * 1.001
        and cur["rsi"] < r_at_phi - RSI_DIV_MIN_GAP
        and cur["close"] < cur["vwap"]
        and prv["rsi"] > 60 and cur["rsi"] <= 60):
        return "sell"
    return None

def ichimoku_signal(df: pd.DataFrame) -> str | None:
    """🥇 E09 ICHIMOKU CLOUD — POL 1H  (97.4% hit / +$1.222 backtest).

    Dispara na quebra do Kumo (nuvem) confirmada por TK cross + Chikou livre.
    LONG : close acima do Kumo + Tenkan > Kijun + Chikou > preço de 26 períodos atrás.
    SHORT: simétrico abaixo.
    """
    n = max(ICHI_SENKOU + ICHI_KIJUN + 5, 90)
    if len(df) < n: return None
    df = df.copy()
    high, low, close = df["high"], df["low"], df["close"]

    df["tenkan"] = (high.rolling(ICHI_TENKAN).max() + low.rolling(ICHI_TENKAN).min()) / 2
    df["kijun"]  = (high.rolling(ICHI_KIJUN).max()  + low.rolling(ICHI_KIJUN).min())  / 2
    df["span_a"] = ((df["tenkan"] + df["kijun"]) / 2).shift(ICHI_KIJUN)
    df["span_b"] = ((high.rolling(ICHI_SENKOU).max()
                     + low.rolling(ICHI_SENKOU).min()) / 2).shift(ICHI_KIJUN)

    cur, prv = df.iloc[-2], df.iloc[-3]
    if any(pd.isna(x) for x in [cur["tenkan"], cur["kijun"],
                                cur["span_a"], cur["span_b"],
                                prv["tenkan"], prv["kijun"]]):
        return None

    kumo_top = max(cur["span_a"], cur["span_b"])
    kumo_bot = min(cur["span_a"], cur["span_b"])
    chikou_ref = df["close"].iloc[-2 - ICHI_KIJUN] if len(df) > ICHI_KIJUN + 3 else None

    long_break  = (cur["close"] > kumo_top
                   and cur["tenkan"] > cur["kijun"]
                   and prv["tenkan"] <= prv["kijun"]               # cross fresh
                   and (chikou_ref is None or cur["close"] > chikou_ref))
    short_break = (cur["close"] < kumo_bot
                   and cur["tenkan"] < cur["kijun"]
                   and prv["tenkan"] >= prv["kijun"]
                   and (chikou_ref is None or cur["close"] < chikou_ref))

    if long_break:  return "buy"
    if short_break: return "sell"
    return None

def order_block_signal(df: pd.DataFrame) -> str | None:
    """ORDER BLOCK DEFENSE — ADA / XRP (1H candles).

    Lógica:
      1. Varre os últimos OB_LOOKBACK candles à procura de expansion candles
         (volume ≥ OB_VOL_MULT × média20, corpo ≥ OB_BODY_MULT × média20).
      2. Calcula o midpoint (50%) do corpo de cada OB encontrado.
      3. Se o preço actual toca o midpoint (±OB_TOL_PCT%), emite sinal
         na direcção da expansão original.
      4. Filtro EMA200: LONG só acima, SHORT só abaixo.
      5. Filtro RSI 35–65: evita entradas em extremos absolutos.
    """
    if len(df) < 50:
        return None
    df = df.copy()
    df["ema200"] = ta.ema(df["close"], length=200)
    df["rsi"]    = ta.rsi(df["close"], length=14)
    df["body"]   = abs(df["close"] - df["open"])
    df["vol_ma"] = df["vol"].rolling(20).mean()
    df["bdy_ma"] = df["body"].rolling(20).mean()

    cur_price = df["close"].iloc[-1]
    ema200    = df["ema200"].iloc[-1]
    rsi_now   = df["rsi"].iloc[-1]
    if any(pd.isna(x) for x in [ema200, rsi_now]):
        return None
    if not (35 <= rsi_now <= 65):
        return None

    # Procura blocos de ordem nas últimas OB_LOOKBACK velas (excluindo a actual)
    scan_start = max(0, len(df) - OB_LOOKBACK - 1)
    for i in range(scan_start, len(df) - 1):
        vol_i  = df["vol"].iloc[i];  vma = df["vol_ma"].iloc[i]
        body_i = df["body"].iloc[i]; bma = df["bdy_ma"].iloc[i]
        if pd.isna(vma) or pd.isna(bma) or vma == 0 or bma == 0:
            continue
        if vol_i < vma * OB_VOL_MULT or body_i < bma * OB_BODY_MULT:
            continue  # não é expansion candle

        ob_open  = df["open"].iloc[i]
        ob_close = df["close"].iloc[i]
        ob_mid   = (ob_open + ob_close) / 2
        tol      = ob_mid * OB_TOL_PCT / 100
        bullish_ob = ob_close > ob_open   # vela de expansão de alta
        bearish_ob = ob_close < ob_open   # vela de expansão de baixa

        price_at_mid = abs(cur_price - ob_mid) <= tol

        if price_at_mid:
            if bullish_ob and cur_price > ema200:
                # Retorno ao OB bullish acima da EMA200 → LONG
                return "buy"
            if bearish_ob and cur_price < ema200:
                # Retorno ao OB bearish abaixo da EMA200 → SHORT
                return "sell"
    return None

# ══════════════════════════════════════════════════════════════════════════════
# MONITOR — aguarda fecho de posição em thread separada
# ══════════════════════════════════════════════════════════════════════════════

def _monitor(inst_id: str, pos_side: str, side: str,
             entry: float, sl_px: float, activate_px: float,
             sym: str, dir_txt: str, bal: float, qty: int,
             tag: str = "DUO ELITE") -> None:
    global _duo_in_trade, _duo_cooldown_until
    log.info("📡 SENTINELA [%s] %s %s | SL=%.5f | Trailing activa a %.5f | 💰 Lock $%.0f",
             tag, sym, dir_txt, sl_px, activate_px, PROFIT_LOCK_USD)
    _none_streak       = 0
    _profit_lock_moved = False   # SL já foi movido para garantir $20 — só uma vez
    while True:
        time.sleep(20)
        try:
            pos = okx_get_position(inst_id, pos_side)

            # ── 🚨 GLOBAL CIRCUIT BREAKER -7% — fecho imediato, protege banca ──
            if pos is not None:
                mark_px_cb = float(pos.get("markPx", 0) or 0)
                avg_px_cb  = float(pos.get("avgPx",  entry) or entry)
                pos_sz_cb  = int(float(pos.get("pos", qty) or qty))
                if mark_px_cb > 0 and avg_px_cb > 0:
                    if side == "buy":
                        adverse_pct = (avg_px_cb - mark_px_cb) / avg_px_cb * 100
                    else:
                        adverse_pct = (mark_px_cb - avg_px_cb) / avg_px_cb * 100
                    if adverse_pct >= CIRCUIT_BREAKER_PCT:
                        log.warning("🚨 CIRCUIT BREAKER %s: -%.2f%% preço — fechando!",
                                    sym, adverse_pct)
                        try:
                            okx_cancel_all_algos(inst_id, pos_side); time.sleep(0.5)
                            okx_close_market(inst_id, pos_side, pos_sz_cb)
                            tg(f"🚨 <b>CIRCUIT BREAKER -{CIRCUIT_BREAKER_PCT:.0f}% — FECHO DE EMERGÊNCIA</b>\n"
                               f"Par: <code>{sym}</code> | {dir_txt}\n"
                               f"Movimento adverso: <b>-{adverse_pct:.2f}%</b> em preço\n"
                               f"🛡️ Banca protegida — aguardando confirmação de fecho.")
                        except Exception as e:
                            log.error("circuit breaker close fail %s: %s", sym, e)

            # ── $20 PROFIT LOCK — move SL para blindar lucro mínimo ──────────
            if pos is not None and not _profit_lock_moved:
                upl      = float(pos.get("upl",    0) or 0)
                mark_px  = float(pos.get("markPx", 0) or 0)
                avg_px   = float(pos.get("avgPx",  entry) or entry)
                pos_sz   = int(float(pos.get("pos", qty) or qty))

                if upl >= PROFIT_LOCK_USD and mark_px > 0 and avg_px > 0:
                    # Calcula o preço onde P&L = exactamente +$20
                    # LONG:  price_move = markPx - avgPx  → lock = avgPx + $20*(move/upl)
                    # SHORT: price_move = avgPx - markPx  → lock = avgPx - $20*(move/upl)
                    if side == "buy":
                        price_move = mark_px - avg_px
                        lock_px    = avg_px + PROFIT_LOCK_USD * (price_move / upl)
                    else:
                        price_move = avg_px - mark_px
                        lock_px    = avg_px - PROFIT_LOCK_USD * (price_move / upl)

                    log.info("💰 PROFIT LOCK — $%.2f atingido | %s SL → %.5f (garante $%.0f)",
                             upl, sym, lock_px, PROFIT_LOCK_USD)
                    try:
                        okx_cancel_all_algos(inst_id, pos_side)   # remove SL + Trailing antigos
                        time.sleep(1)
                        okx_initial_sl(inst_id, pos_side, pos_sz, lock_px)   # novo SL no piso $20
                        okx_trailing_stop(inst_id, pos_side, pos_sz,          # trailing continua
                                          mark_px * (1 + TRAIL_ACTIVATE_PCT/100) if side == "buy"
                                          else mark_px * (1 - TRAIL_ACTIVATE_PCT/100))
                        _profit_lock_moved = True
                        tg(f"🔒 <b>GRAU 1 — PROFIT LOCK $20 BLINDADO</b>\n"
                           f"Par: <code>{sym}</code> | {dir_txt}\n"
                           f"Lucro actual: <b>${upl:+.2f} USDT</b>\n"
                           f"🛡️ Novo SL em: <code>{lock_px:.5f}</code>\n"
                           f"Se o preço virar, saímos com <b>≥ $20 garantidos</b>.\n"
                           f"📡 Trailing continua activo em busca de mais.")
                    except Exception as e:
                        log.error("profit lock move: %s — voltando a tentar", e)
                _none_streak = 0
                continue

            # ── posição ainda aberta (lock já activo ou abaixo do threshold) ──
            if pos is not None:
                _none_streak = 0
                continue

            _none_streak += 1
            if _none_streak < 3:
                log.debug("SENTINELA %s: confirmação %d/3...", sym, _none_streak)
                continue

            # 3 Nones consecutivos → posição confirmada fechada
            try: exit_px = okx_ticker(inst_id)
            except Exception: exit_px = entry
            if side == "buy":
                pnl_pct = (exit_px - entry) / entry * 100 * LEVERAGE
            else:
                pnl_pct = (entry - exit_px) / entry * 100 * LEVERAGE
            pnl_usd = bal * pnl_pct / 100
            win     = pnl_pct > 0

            if _profit_lock_moved:
                icon   = "✅" if win else "⚠️"
                result = f"SAÍDA COM LUCRO 🎯 (piso $20 activo)" if win else "SAÍDA ABAIXO DO LOCK ⚠️"
            else:
                icon   = "🎯" if win else "💥"
                result = "SAÍDA COM LUCRO 🎯" if win else "SAÍDA COM PERDA 💥"

            tg(f"{icon} <b>{tag} — {result}</b>\n"
               f"Par: <code>{sym}</code> | {dir_txt}\n"
               f"Entrada: <code>{entry:.5f}</code> → Saída: <code>{exit_px:.5f}</code>\n"
               f"P&L: <b>${pnl_usd:+.2f} USDT</b> ({pnl_pct:+.2f}%)\n"
               f"⏳ Cooldown 60 min activado.")

            log.info("📊 [%s] %s fechado | exit=%.5f P&L $%.2f (%.2f%%) | lock_moved=%s",
                     tag, sym, exit_px, pnl_usd, pnl_pct, _profit_lock_moved)
            with _duo_lock:
                _duo_in_trade       = False
                _duo_cooldown_until = time.time() + DUO_COOLDOWN
            return
        except Exception as e:
            log.error("monitor %s: %s", sym, e)
            _none_streak = 0
            time.sleep(10)

# ══════════════════════════════════════════════════════════════════════════════
# EXECUÇÃO DE TRADE
# ══════════════════════════════════════════════════════════════════════════════

def _fire(inst_id: str, side: str, signal_name: str,
          tag: str = "DUO ELITE", sl_pct: float | None = None) -> bool:
    """Executa ordem de mercado + SL inicial + Trailing Stop.

    Golden Doctrine SL routing (sobrepõe sl_pct passado, excepto se for explícito):
      - HOLD pairs (POL/ETH/SOL): SL = HOLD_SL_PCT (7%) → só circuit breaker
      - STRICT pairs (ADA/XRP/DOGE): SL = STRICT_SL_PCT (1.5%) — não segurar
      - Outros: usa sl_pct passado ou DUO_SL_PCT
    """
    global _duo_in_trade, _lockdown_until
    # ── Routing automático do SL pela classificação do par ────────────────
    if sl_pct is None:
        if   inst_id in HOLD_PAIRS:   sl_pct = HOLD_SL_PCT
        elif inst_id in STRICT_PAIRS: sl_pct = STRICT_SL_PCT
        else:                         sl_pct = DUO_SL_PCT
    ps      = _SIDE_PS[side]
    sym     = inst_id.replace("-USDT-SWAP", "")
    dir_txt = "LONG 🟢" if side == "buy" else "SHORT 🔴"

    # 🛡️ LOCKDOWN imediato — quer a ordem passe ou falhe, fica 15min em silêncio
    # (anti ping-pong: evita 7 retries do mesmo sinal a cada 2 min)
    with _duo_lock:
        _lockdown_until = max(_lockdown_until, time.time() + LOCKDOWN_SECS)

    # ONE DIRECTION ONLY — se EXISTE qualquer posição (mesmo lado oposto), aborta
    existing = okx_any_position_open(ALL_SYMS)
    if existing is not None:
        ex_sym, ex_ps = existing
        log.info("🛑 [%s] BLOQUEADO — posição já aberta em %s/%s. Aguardar fecho.",
                 sym, ex_sym, ex_ps)
        return False

    tg(f"⚔️ <b>{tag} — ATTACK</b>\n"
       f"Sinal: <b>{signal_name}</b> | Par: <code>{sym}</code> | {dir_txt}\n"
       f"SL <b>-{sl_pct}%</b> | Trailing activa <b>+{TRAIL_ACTIVATE_PCT}%</b> | Callback <b>{TRAIL_CALLBACK*100:.0f}%</b> | {LEVERAGE}× ALL-IN")

    bal   = okx_balance() or 0.0
    price = okx_ticker(inst_id)
    qty   = calc_qty(inst_id, price, bal)
    log.info("⚙️ [%s] preparando ordem | bal=$%.2f price=%.5f qty=%d posSide=%s",
             sym, bal, price, qty, ps)
    if bal <= 0:
        log.error("[%s] saldo zero ou inválido — ordem abortada", sym)
        tg(f"❌ <b>{tag}</b> {sym}: saldo zero ou inválido — verifica credenciais OKX.")
        return False
    if qty < 1:
        log.error("[%s] qty calculado < 1 (bal=%.2f price=%.5f) — ordem abortada", sym, bal, price)
        tg(f"❌ <b>{tag}</b> {sym}: qty<1 (bal=${bal:.2f}) — saldo insuficiente para 1 contrato.")
        return False
    res   = okx_order(inst_id, side, qty)
    ord_id = res["data"][0].get("ordId", "?")
    log.info("✅ ORDER [%s] %s %s ordId=%s qty=%d", tag, sym, dir_txt, ord_id, qty)

    time.sleep(2)
    pos = okx_get_position(inst_id, ps)
    avg = float(pos.get("avgPx", price) or price) if pos else price

    sl_px       = avg * (1 - sl_pct / 100)            if side == "buy" else avg * (1 + sl_pct / 100)
    activate_px = avg * (1 + TRAIL_ACTIVATE_PCT / 100) if side == "buy" else avg * (1 - TRAIL_ACTIVATE_PCT / 100)

    okx_initial_sl(inst_id, ps, qty, sl_px)
    okx_trailing_stop(inst_id, ps, qty, activate_px)

    tg(f"📡 <b>TRAILING READY</b> — [{tag}] {sym} {dir_txt}\n"
       f"Entrada: <code>{avg:.5f}</code>\n"
       f"🛡️ SL inicial: <code>{sl_px:.5f}</code> (-{sl_pct}%)\n"
       f"📡 Trailing activa a: <code>{activate_px:.5f}</code> (+{TRAIL_ACTIVATE_PCT}%) | Callback: <b>{TRAIL_CALLBACK*100:.0f}%</b>\n"
       f"ordId: <code>{ord_id}</code>")

    with _duo_lock:
        _duo_in_trade = True

    threading.Thread(target=_monitor,
        args=(inst_id, ps, side, avg, sl_px, activate_px, sym, dir_txt, bal, qty),
        kwargs={"tag": tag},
        daemon=True, name=f"mon_{sym}").start()
    return True

# ══════════════════════════════════════════════════════════════════════════════
# RELATÓRIO E COMANDOS TELEGRAM
# ══════════════════════════════════════════════════════════════════════════════

def _status_text() -> str:
    full = okx_balance_full()
    with _duo_lock:
        in_trade = _duo_in_trade; cd = _duo_cooldown_until; ld = _lockdown_until
    with _auth_lock:
        auth = _bot_authorized
    now = time.time()
    open_pos = okx_any_position_open(ALL_SYMS)

    if not auth:                         status = "⛔ PAUSADO"
    elif open_pos is not None:
        sym, ps = open_pos
        status = f"🔴 EM TRADE ({sym}/{ps.upper()}) — saldo BLOQUEADO"
    elif in_trade:                       status = "🔴 TRADE ATIVA"
    elif now < ld:                       status = f"🔇 LOCKDOWN {max(0,ld-now)/60:.0f}min"
    elif now < cd:                       status = f"⏳ Cooldown {max(0,cd-now)/60:.0f}min"
    else:                                status = "🟢 Aguardando sinal"

    if full is not None:
        eq, avail = full
        used = max(eq - avail, 0.0)
        bal_str = (f"Total <b>${eq:,.2f}</b> | Disponível <b>${avail:,.2f}</b>"
                   f" | Em uso <b>${used:,.2f}</b>")
    else:
        bal_str = "Saldo: —"

    return (f"📊 <b>V8 FULL SQUAD + GOLDEN — {datetime.now(timezone.utc).strftime('%d/%m %H:%M UTC')}</b>\n"
            f"{bal_str}\nStatus: {status}\n"
            f"🥇 POL [ICHIMOKU 1H]  🌊 SOL [SUPERTREND M15]  🎯 XRP [RSI DIV M15]\n"
            f"💧 ETH [VWAP KISS M15]  🔥 SOL [ENGOLFO M15]  🛡️ ADA/XRP [ORDER BLOCK 1H]\n"
            f"💰 Lock <b>$+{PROFIT_LOCK_USD:.0f}</b> | HOLD POL/ETH/SOL | STRICT {STRICT_SL_PCT}% ADA/XRP/DOGE | "
            f"🚨 Circuit Breaker -{CIRCUIT_BREAKER_PCT:.0f}%")

def report_loop() -> None:
    last = time.time()
    while True:
        time.sleep(60)
        if time.time() - last >= 1800:
            try: tg(_status_text())
            except Exception as e: log.warning("report: %s", e)
            last = time.time()

_tg_offset = 0

def telegram_commands_loop() -> None:
    global _tg_offset, _bot_authorized
    if not TELEGRAM_TOKEN:
        log.warning("TELEGRAM_TOKEN não configurado — comandos desativados.")
        return
    log.info("📱 Telegram commands polling activo.")
    while True:
        try:
            r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                             params={"offset": _tg_offset, "timeout": 25, "limit": 10}, timeout=30)
            for upd in r.json().get("result", []):
                _tg_offset = upd["update_id"] + 1
                msg = upd.get("message") or upd.get("edited_message")
                if not msg or not msg.get("text"): continue
                chat_id = msg["chat"]["id"]
                cmd = msg["text"].strip().lower().split()[0].lstrip("/").split("@")[0]
                if cmd in ("start", "resume", "on", "autorizar"):
                    with _auth_lock: _bot_authorized = True
                    _save_state(True)
                    tg("✅ <b>V8 FULL SQUAD + GOLDEN AUTORIZADO</b>\n"
                       "🥇 POL · 🌊 SOL · 🎯 XRP (Golden) + ETH · ADA · XRP (legacy).", chat_id)
                    log.info("Bot autorizado via Telegram")
                elif cmd in ("pause", "stop", "off", "pausar"):
                    with _auth_lock: _bot_authorized = False
                    _save_state(False)
                    tg("⛔ <b>Bot PAUSADO</b> — /start para retomar.", chat_id)
                    log.info("Bot pausado via Telegram")
                elif cmd in ("status", "s"):
                    try: tg(_status_text(), chat_id)
                    except Exception as e: tg(f"Erro: {e}", chat_id)
                elif cmd in ("help", "ajuda"):
                    tg("🤖 <b>V8 FULL SQUAD + GOLDEN DOCTRINE</b>\n"
                       "/start — Autorizar squad\n"
                       "/pause — Pausar\n"
                       "/status — Estado + saldo\n\n"
                       "<b>🏆 GOLDEN (prioridade):</b>\n"
                       "🥇 POL — ICHIMOKU 1H (HOLD)\n"
                       "🌊 SOL — SUPERTREND M15 (HOLD)\n"
                       "🎯 XRP — RSI DIV + VWAP M15 (STRICT)\n\n"
                       "<b>Legacy:</b>\n💧 ETH — VWAP KISS M15\n🔥 SOL — ENGOLFO M15\n"
                       "🛡️ ADA — ORDER BLOCK 1H\n🛡️ XRP — ORDER BLOCK 1H\n\n"
                       f"💰 $20 NET LOCK | HOLD POL/ETH/SOL | STRICT 1.5% ADA/XRP/DOGE\n"
                       f"🚨 Circuit Breaker -7%", chat_id)
        except Exception as e:
            log.warning("tg_polling: %s", e)
            time.sleep(5)

# ══════════════════════════════════════════════════════════════════════════════
# LOOP PRINCIPAL — ONE TARGET ONE KILL
# ══════════════════════════════════════════════════════════════════════════════

def duo_elite_loop() -> None:
    global _duo_in_trade, _duo_cooldown_until
    log.info("🎯 V8 FULL SQUAD + GOLDEN RECOVERY DOCTRINE ENABLED")
    tg("🏆 <b>DOCTRINE UPDATED: POL GOLD READY</b>\n"
       "Doutrina: <b>$20 NET PROFIT LOCK = LAW</b>\n\n"
       "<b>🏆 GOLDEN (prioridade):</b>\n"
       "🥇 POL — ICHIMOKU 1H  (HOLD)  97.4% hit\n"
       "🌊 SOL — SUPERTREND M15  (HOLD)  95.0% hit\n"
       "🎯 XRP — RSI DIV + VWAP M15  (STRICT 1.5%)  PF 2.38\n\n"
       "<b>Legacy squad:</b>\n"
       "💧 ETH — VWAP KISS M15  (HOLD)\n"
       "🔥 SOL — ENGOLFO M15  (HOLD)\n"
       "🛡️ ADA — ORDER BLOCK 1H  (STRICT 1.5%)\n"
       "🛡️ XRP — ORDER BLOCK 1H  (STRICT 1.5%)\n\n"
       f"📡 Trailing: +{TRAIL_ACTIVATE_PCT}% / cb {TRAIL_CALLBACK*100:.0f}%  |  {LEVERAGE}× ALL-IN\n"
       f"💰 Profit Lock <b>$+{PROFIT_LOCK_USD:.0f}</b>\n"
       f"🚨 <b>Circuit Breaker -{CIRCUIT_BREAKER_PCT:.0f}%</b> (banca $900 protegida)\n"
       "🤖 Scan 2 min  |  ONE-DIRECTION DOCTRINE  |  EMA200 em todos\n\n"
       "✅ <b>POL GOLD READY — Hold the hand activated</b>")

    while True:
        try:
            now = time.time()
            with _duo_lock:
                in_trade = _duo_in_trade; cd = _duo_cooldown_until; ld = _lockdown_until
            with _auth_lock:
                auth = _bot_authorized

            if not auth:
                log.info("⛔ Bot pausado.")
                time.sleep(30); continue

            if in_trade:
                log.info("🛡️ Trade ativa — aguardando fecho.")
                time.sleep(30); continue

            if now < ld:
                log.info("🔇 LOCKDOWN — silêncio %.0f min restantes.", (ld - now) / 60)
                time.sleep(60); continue

            if now < cd:
                log.info("⏳ Cooldown %.0f min.", (cd - now) / 60)
                time.sleep(60); continue

            # ONE DIRECTION DOCTRINE — se HÁ posição aberta em qualquer par, parar tudo
            existing = okx_any_position_open(ALL_SYMS)
            if existing is not None:
                ex_sym, ex_ps = existing
                log.info("🛑 ONE-DIRECTION — posição aberta em %s/%s, ignorando todos os sinais.",
                         ex_sym, ex_ps)
                time.sleep(60); continue

            fired = False

            # ╔══════════════ GOLDEN DOCTRINE PRIORITY ═══════════════════════╗
            # ── 🥇 PRIORIDADE 1: POL — ICHIMOKU 1H (97.4% hit, HOLD) ────────
            try:
                sig = ichimoku_signal(okx_candles(GOLD_POL, bar="1H", limit=200))
                if sig:
                    dir_scout = "📈 LONG" if sig == "buy" else "📉 SHORT"
                    log.info("🥇 ICHIMOKU POL → %s", sig.upper())
                    tg(f"🥇 <b>GOLDEN — POL ICHIMOKU FIRED</b>\n"
                       f"Par: <code>POL-USDT-SWAP</code> | Sinal: <b>ICHIMOKU 1H</b>\n"
                       f"Direção: <b>{dir_scout}</b>  | Hit histórico: <b>97.4%</b>\n"
                       f"💰 Hold the hand — alvo $20 NET. Circuit breaker -7%.")
                    fired = _fire(GOLD_POL, sig, "ICHIMOKU POL", tag="🥇 GOLDEN POL")
                else:
                    log.info("[POL] sem sinal")
            except Exception as e:
                log.error("[POL] %s", e)

            # ── 🌊 PRIORIDADE 2: SOL — SUPERTREND 15m (95% hit, HOLD) ───────
            if not fired:
                try:
                    sig = supertrend_signal(okx_candles(DUO_SOL))
                    if sig:
                        dir_scout = "📈 LONG" if sig == "buy" else "📉 SHORT"
                        log.info("🌊 SUPERTREND SOL → %s", sig.upper())
                        tg(f"🌊 <b>GOLDEN — SOL SUPERTREND FIRED</b>\n"
                           f"Par: <code>SOL-USDT-SWAP</code> | Sinal: <b>SUPERTREND M15</b>\n"
                           f"Direção: <b>{dir_scout}</b>  | Hit histórico: <b>95.0%</b>\n"
                           f"💰 Hold the hand — alvo $20 NET. Circuit breaker -7%.")
                        fired = _fire(DUO_SOL, sig, "SUPERTREND SOL", tag="🌊 TREND SURF")
                    else:
                        log.info("[SOL/ST] sem sinal")
                except Exception as e:
                    log.error("[SOL/ST] %s", e)

            # ── 🎯 PRIORIDADE 3: XRP — RSI DIV + VWAP 15m (PF 2.38, STRICT) ─
            if not fired:
                try:
                    sig = rsi_div_vwap_signal(okx_candles(SHIELD_XRP))
                    if sig:
                        dir_scout = "📈 LONG" if sig == "buy" else "📉 SHORT"
                        log.info("🎯 RSI DIV+VWAP XRP → %s", sig.upper())
                        tg(f"🎯 <b>SAFETY SNIPER — XRP RSI DIV FIRED</b>\n"
                           f"Par: <code>XRP-USDT-SWAP</code> | Sinal: <b>RSI DIV + VWAP M15</b>\n"
                           f"Direção: <b>{dir_scout}</b>  | PF histórico: <b>2.38</b>\n"
                           f"⚠️ STRICT SL 1.5% — não segurar a mão.")
                        fired = _fire(SHIELD_XRP, sig, "RSI DIV XRP", tag="🎯 SAFETY SNIPER")
                    else:
                        log.info("[XRP/RSI] sem divergência")
                except Exception as e:
                    log.error("[XRP/RSI] %s", e)
            # ╚═══════════════════════════════════════════════════════════════╝

            # ── 4: ETH — VWAP KISS (legacy DUO) ──────────────────────────────
            if not fired:
              try:
                sig = vwap_kiss_signal(okx_candles(DUO_ETH))
                if sig:
                    dir_scout = "📈 LONG" if sig == "buy" else "📉 SHORT"
                    log.info("💧 VWAP KISS ETH → %s", sig.upper())
                    tg(f"🔭 <b>SCOUT — ALVO DETECTADO</b>\n"
                       f"Par: <code>ETH-USDT-SWAP</code> | Sinal: <b>VWAP KISS M15</b>\n"
                       f"Direção: <b>{dir_scout}</b>\n"
                       f"⚡ Executando ataque...")
                    fired = _fire(DUO_ETH, sig, "VWAP KISS ETH", tag="DUO ELITE")
                else:
                    log.info("[ETH] sem sinal")
              except Exception as e:
                log.error("[ETH] %s", e)

            # ── 2: SOL — ENGOLFO (prioridade 2) ──────────────────────────────
            if not fired:
                try:
                    sig = engolfo_signal(okx_candles(DUO_SOL))
                    if sig:
                        dir_scout = "📈 LONG" if sig == "buy" else "📉 SHORT"
                        log.info("🔥 ENGOLFO SOL → %s", sig.upper())
                        tg(f"🔭 <b>SCOUT — ALVO DETECTADO</b>\n"
                           f"Par: <code>SOL-USDT-SWAP</code> | Sinal: <b>ENGOLFO M15</b>\n"
                           f"Direção: <b>{dir_scout}</b>\n"
                           f"⚡ Executando ataque...")
                        fired = _fire(DUO_SOL, sig, "ENGOLFO SOL", tag="DUO ELITE")
                    else:
                        log.info("[SOL] sem sinal")
                except Exception as e:
                    log.error("[SOL] %s", e)

            # ── 3: ADA — ORDER BLOCK DEFENSE (prioridade 3) ───────────────────
            if not fired:
                try:
                    sig = order_block_signal(okx_candles(SHIELD_ADA, bar="1H", limit=100))
                    if sig:
                        dir_scout = "📈 LONG" if sig == "buy" else "📉 SHORT"
                        log.info("🛡️ ORDER BLOCK ADA → %s", sig.upper())
                        tg(f"🛡️ <b>SHIELD — ADA BLOCK DETECTADO</b>\n"
                           f"Par: <code>ADA-USDT-SWAP</code> | Sinal: <b>ORDER BLOCK 1H</b>\n"
                           f"Direção: <b>{dir_scout}</b>\n"
                           f"⚡ Escudo activado — executando entrada no bloco...")
                        fired = _fire(SHIELD_ADA, sig, "ORDER BLOCK ADA",
                                      tag="SHIELD 🛡️")   # routing → STRICT 1.5%
                    else:
                        log.info("[ADA] sem bloco")
                except Exception as e:
                    log.error("[ADA] %s", e)

            # ── 4: XRP — ORDER BLOCK DEFENSE (prioridade 4) ───────────────────
            if not fired:
                try:
                    sig = order_block_signal(okx_candles(SHIELD_XRP, bar="1H", limit=100))
                    if sig:
                        dir_scout = "📈 LONG" if sig == "buy" else "📉 SHORT"
                        log.info("🛡️ ORDER BLOCK XRP → %s", sig.upper())
                        tg(f"🛡️ <b>SHIELD — XRP BLOCK DETECTADO</b>\n"
                           f"Par: <code>XRP-USDT-SWAP</code> | Sinal: <b>ORDER BLOCK 1H</b>\n"
                           f"Direção: <b>{dir_scout}</b>\n"
                           f"⚡ Escudo activado — executando entrada no bloco...")
                        _fire(SHIELD_XRP, sig, "ORDER BLOCK XRP",
                              tag="SHIELD 🛡️")   # routing → STRICT 1.5%
                    else:
                        log.info("[XRP] sem bloco")
                except Exception as e:
                    log.error("[XRP] %s", e)

        except Exception as e:
            log.error("loop: %s", e)

        time.sleep(120)

# ══════════════════════════════════════════════════════════════════════════════
# HEALTH SERVER — satisfaz requisito de porta do Autoscale (bot não é afetado)
# ══════════════════════════════════════════════════════════════════════════════

class _HealthHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, *_): pass  # silencia logs de acesso

def _start_health_server() -> None:
    """Health server compatível com Render/Cloud Run/Autoscale.

    - Bind 0.0.0.0:$PORT (obrigatório em Render para passar o health check)
    - Retry com back-off em caso de porta ocupada (dev mode) — não morre silenciosamente
    - SO_REUSEADDR para reinícios rápidos sem TIME_WAIT
    """
    socketserver.TCPServer.allow_reuse_address = True
    port = int(os.environ.get("PORT", 10000))
    backoff = 2
    while True:
        try:
            with socketserver.TCPServer(("0.0.0.0", port), _HealthHandler) as srv:
                log.info("🌐 Health server LIVE em 0.0.0.0:%d (PORT=%s)",
                         port, os.environ.get("PORT", "<default 10000>"))
                srv.serve_forever()
        except OSError as e:
            log.warning("Health server bind 0.0.0.0:%d falhou (%s) — retry em %ds",
                        port, e, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
        except Exception as e:
            log.error("Health server crashed: %s — reiniciando em 5s", e)
            time.sleep(5)

# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    log.info("╔═══════════════════════════════════════════════╗")
    log.info("║   TradeSniper V8 FULL SQUAD + GOLDEN          ║")
    log.info("║   🥇 POL  [ICHIMOKU 1H]    HOLD   97.4%% hit   ║")
    log.info("║   🌊 SOL  [SUPERTREND M15] HOLD   95.0%% hit   ║")
    log.info("║   🎯 XRP  [RSI DIV M15]    STRICT PF 2.38     ║")
    log.info("║   💧 ETH [VWAP KISS]  🔥 SOL [ENGOLFO]        ║")
    log.info("║   🛡️ ADA + XRP [ORDER BLOCK 1H]              ║")
    log.info("║   SL: HOLD %.0f%% | STRICT %.1f%% | CB -%.0f%% | %dx  ║",
             HOLD_SL_PCT, STRICT_SL_PCT, CIRCUIT_BREAKER_PCT, LEVERAGE)
    log.info("║   💰 $%.0f NET PROFIT LOCK = LAW              ║", PROFIT_LOCK_USD)
    log.info("╚═══════════════════════════════════════════════╝")

    # Estado persistido
    with _auth_lock:
        _bot_authorized = _load_state()
    log.info("Estado: %s", "AUTORIZADO ✅" if _bot_authorized else "PAUSADO ⛔")

    # Leverage
    for sym in ALL_SYMS:
        okx_set_leverage(sym)

    # Health server — porta para Autoscale (responde 200 OK, bot não é afetado)
    threading.Thread(target=_start_health_server, daemon=True, name="health").start()

    # Threads de suporte
    threading.Thread(target=report_loop,            daemon=True, name="report").start()
    threading.Thread(target=telegram_commands_loop, daemon=True, name="tg").start()

    # Loop principal — bloqueia para sempre
    duo_elite_loop()
