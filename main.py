"""
TradeSniper Bot — V9 FULL SQUAD + FVG EXPANSION + GOLDEN RECOVERY DOCTRINE + STEP TRAIL V5
BUILD: 2026-04-19 — V9: FVG SOL/BNB/ETH adicionados | Step Trail V5 | SL HOLD 5% | Margin 3%
Doutrina : ONE TARGET, ONE KILL  |  STEP TRAIL V5 = LAW
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🏆 GOLDEN DOCTRINE (backtest 103 dias — actualizada Abr/2026):
  🥇 POL — E09 ICHIMOKU 1H V2         (5 filtros anti-falso, reforçado Abr/2026) HOLD
  🌊 SOL — E06 SUPERTREND 15m         (95.0% hit, +$515 líq.)    HOLD
  🎯 XRP — E07 RSI DIV + VWAP 15m     (PF 2.38, alta convicção)  STRICT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🆕 FVG EXPANSION SQUAD (backtest 90 dias — adicionado V9):
  🔷 SOL — E10 FAIR VALUE GAP 15m     (70.6% hit, ROI +70.4%)    HOLD
  🔷 BNB — E11 FAIR VALUE GAP 15m     (65.2% hit, ROI +48.8%)    HOLD
  🔷 ETH — E12 FAIR VALUE GAP 15m     (73.9% hit, ROI +34.9%)    HOLD
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
LEGACY SQUAD (mantido como rede):
  💧 ETH — VWAP KISS M15                                          HOLD
  🔥 SOL — ENGOLFO M15  (extra trigger)                           HOLD
  🛡️ ADA — ORDER BLOCK 1H                                         STRICT
  🛡️ XRP — ORDER BLOCK 1H                                         STRICT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
HOLD pairs (POL/ETH/SOL/BNB): sem SL apertado, só circuit breaker -7%
STRICT pairs (ADA/XRP/DOGE): SL fixo 1.5% — backtest mostra ruína se segurar
GLOBAL CIRCUIT BREAKER: -7% drawdown = fecho imediato (protege banca $900)
Trailing +0.8% cb 1% | 5× Isolated ALL-IN | OKX Perpetual SWAP (hedge mode)
FVG: gap entre high[i-2] e low[i] (bullish) ou low[i-2] e high[i] (bearish)
     entrada no retorno ao midpoint ±0.3% | EMA200 + RSI 35-65 obrigatórios
     gap expira após FVG_GAP_EXPIRY velas sem retorno (evita entradas stale)
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
LEVERAGE  = 5          # alavancagem operacional — alterável via /subir6x /subir7x
RISK_FRAC = 1.0    # ALL-IN

# ── DUO DE ELITE ───────────────────────────────────────────────────────────────
DUO_SL_PCT          = 1.2    # SL inicial (protecção antes do trailing activar)
DUO_COOLDOWN        = 1800   # 30 min cooldown após trade
TRAIL_ACTIVATE_PCT  = 0.8    # trailing activa quando lucro ≥ +0.8%
TRAIL_CALLBACK      = 0.01   # distância trailing = 1.0%

DUO_ETH    = "ETH-USDT-SWAP"
DUO_SOL    = "SOL-USDT-SWAP"
SHIELD_ADA = "ADA-USDT-SWAP"
SHIELD_XRP = "XRP-USDT-SWAP"
GOLD_POL   = "POL-USDT-SWAP"      # 🥇 Golden pair — Ichimoku 1H exclusivo
GOLD_DOGE  = "DOGE-USDT-SWAP"     # incluído na lista STRICT (regra de hold)
FVG_BNB    = "BNB-USDT-SWAP"      # 🆕 V9 — FVG expansion squad
ALL_SYMS   = [DUO_ETH, DUO_SOL, SHIELD_ADA, SHIELD_XRP, GOLD_POL, GOLD_DOGE, FVG_BNB]

# ── ORDER BLOCK DEFENSE (ADA / XRP — 1H) ──────────────────────────────────────
OB_LOOKBACK    = 20    # velas 1H para procurar blocos de ordem
OB_VOL_MULT    = 2.0   # volume do expansion candle ≥ 2× média
OB_BODY_MULT   = 1.5   # corpo do expansion candle ≥ 1.5× média
OB_TOL_PCT     = 0.4   # tolerância ±0.4% para toque no midpoint
OB_SL_PCT      = 1.0   # SL da estratégia OB (diferente do DUO)

# ── STEP TRAILING V5 — 5 graus baseados em PnL não realizado (USDT) ───────────
# Cada tuple: (trigger_usd, lock_usd) — ao atingir trigger, SL sobe para lock
STEP_TRAIL_LEVELS: list[tuple[float, float]] = [
    (25.0,  15.0),   # Grau 1: hit +$25  → piso +$15
    (40.0,  25.0),   # Grau 2: hit +$40  → piso +$25
    (60.0,  40.0),   # Grau 3: hit +$60  → piso +$40
    (80.0,  60.0),   # Grau 4: hit +$80  → piso +$60
    (100.0, 80.0),   # Grau 5: hit +$100 → piso +$80
]

# ══════════════════════════════════════════════════════════════════════════════
# GOLDEN RECOVERY DOCTRINE — regras de hold por par (Abr/2026)
# ══════════════════════════════════════════════════════════════════════════════
HOLD_PAIRS    = {GOLD_POL, DUO_ETH, DUO_SOL, FVG_BNB}    # sem SL apertado
STRICT_PAIRS  = {SHIELD_ADA, SHIELD_XRP, GOLD_DOGE}      # SL fixo 1.5%
STRICT_SL_PCT = 1.5
# HOLD: SL na corretora é REDE DE SEGURANÇA (caso o bot/monitor caia).
# O controlo primário é o CIRCUIT_BREAKER no monitor (7.0%) — dispara primeiro.
# Folga de 1pp evita corrida dupla CB-vs-exchange-SL no mesmo tick.
HOLD_SL_PCT   = 5.0
CIRCUIT_BREAKER_PCT = 4.0   # global — monitor fecha SEMPRE a -4% em preço (reduzido de 7%)
PROFIT_LOCK_USD     = 25.0  # TP fixo — fecha 100% ao atingir +$25 USDT não realizado

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

# ── E10/E11/E12 FAIR VALUE GAP — SOL / BNB / ETH 15m ─────────────────────────
# FVG: gap de liquidez criado por vela de impulso forte entre candles [i-2] e [i]
#   Bullish FVG: high[i-2] < low[i]  → gap acima (SOL/BNB/ETH só acima da EMA200)
#   Bearish FVG: low[i-2]  > high[i] → gap abaixo (só abaixo da EMA200)
# Entrada: retorno do preço ao midpoint do gap (±FVG_TOL_PCT%)
# Expiração: gap descartado após FVG_GAP_EXPIRY velas sem retorno (sinal stale)
FVG_TOL_PCT    = 0.3   # tolerância ±0.3% ao midpoint do gap
FVG_GAP_EXPIRY = 40    # máximo de velas aguardando retorno (≈10h em 15m)
FVG_BODY_MULT  = 1.0   # impulso: corpo da vela central ≥ 1× média20 (filtro de qualidade)

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

# ── Confirmação manual (120s) — sinais não-POL aguardam /go[coin] ─────────────
_pending_signals: dict = {}   # coin_key → (inst_id, side, signal_name, tag, expiry)
_pending_lock          = threading.Lock()

# ── Meta mensal — $600 / mês ────────────────────────────────────────────────
MONTHLY_GOAL_USD = 600.0

# ── Panic pause ────────────────────────────────────────────────────────────
_panic_until: float = 0.0

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
# Cache: pares cuja alavancagem já foi confirmada nesta sessão (evita reprocessar
# em cada ciclo, o que gera taxas inúteis e ruído de logs)
_LEVERAGE_SET: set[str] = set()

def okx_set_leverage(inst_id: str, force: bool = False) -> None:
    """Define a alavancagem para o par (long+short).

    Roda APENAS UMA VEZ por par/sessão (cache em _LEVERAGE_SET).
    Reset com `force=True` quando ocorrer 'Parameter Error' numa ordem.
    """
    if not _has_creds(): return
    if inst_id in _LEVERAGE_SET and not force:
        return
    path = "/api/v5/account/set-leverage"
    ok_sides = 0
    for ps in ("long", "short"):
        payload = {"instId": inst_id, "lever": str(LEVERAGE), "mgnMode": "isolated", "posSide": ps}
        body = json.dumps(payload)
        try:
            r = requests.post(f"https://www.okx.com{path}", headers=_headers("POST", path, body), data=body, timeout=8)
            d = r.json()
            if d.get("code") == "0":
                log.info("leverage %s %s: %dx ✓", inst_id, ps, LEVERAGE)
                ok_sides += 1
            else:
                log.warning("leverage %s %s: %s", inst_id, ps, d.get("msg"))
        except Exception as e:
            log.warning("leverage %s %s: %s", inst_id, ps, e)
    if ok_sides == 2:
        _LEVERAGE_SET.add(inst_id)

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

# ── Margem de segurança ALL-IN ────────────────────────────────────────────────
# 3% reservado: cobre taker fee open+close (0.05%×2 × 5x notional = 0.5% bal),
# slippage de market order, e qualquer drift do availBal reportado pela OKX
# entre o /balance e o /trade/order. Evita sCode=51008 (Insufficient Margin).
SAFETY_MARGIN = 0.97

def calc_qty(inst_id: str, price: float, balance: float) -> int:
    ct_val = okx_lot_size(inst_id)
    safe_balance = balance * SAFETY_MARGIN
    return max(1, int(safe_balance * RISK_FRAC * LEVERAGE / (price * ct_val)))

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
        # Em 'Parameter Error' (51000) a alavancagem pode ter sido descartada — força re-set
        if sCode == "51000":
            _LEVERAGE_SET.discard(inst_id)
            log.warning("⚠️ [%s] Parameter Error → cache leverage limpo, próximo trade re-aplica.", inst_id)
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
    """🥇 E09 ICHIMOKU CLOUD V2 — POL 1H  (reforçado Abr/2026).

    Versão original gerava falsos sinais (2 stops seguidos).
    V2 adiciona 5 filtros que eliminam ~60% dos sinais falsos:

    [F1] KUMO FUTURO ALINHADO — Span A futuro > Span B (kumo verde para LONG).
         Sem isto entra em kumo fino = zona de reversão fácil.
    [F2] TK CROSS NAS ÚLTIMAS 3 VELAS — não apenas 1 vela atrás.
         Evita crosses velhos que já perderam momentum.
    [F3] DISTÂNCIA AO KIJUN ≥ 0.3% — o Kijun atua como ímã.
         Se o preço está demasiado perto, o mercado volta.
    [F4] RSI 1H DIRECCIONAL — LONG: RSI 45–68 | SHORT: RSI 32–55.
         Evita entrar em extremos absolutos (sobrecompra/sobrevenda).
    [F5] KUMO NÃO DEMASIADO FINO — espessura ≥ 0.15% do preço.
         Kumo fino = sem convicção = reversão provável.

    Confirmações obrigatórias (todas):
      • close acima/abaixo do Kumo actual
      • Tenkan > Kijun (LONG) ou < (SHORT)
      • TK cross nas últimas 3 velas
      • Chikou livre (acima/abaixo do preço de 26 velas atrás)
      • Kumo futuro alinhado com a direção
      • Distância ao Kijun ≥ 0.3%
      • RSI na zona direccional
      • Espessura do Kumo ≥ 0.15%
    """
    n = max(ICHI_SENKOU + ICHI_KIJUN + 10, 100)
    if len(df) < n: return None
    df = df.copy()
    high, low, close = df["high"], df["low"], df["close"]

    df["tenkan"] = (high.rolling(ICHI_TENKAN).max() + low.rolling(ICHI_TENKAN).min()) / 2
    df["kijun"]  = (high.rolling(ICHI_KIJUN).max()  + low.rolling(ICHI_KIJUN).min())  / 2
    df["span_a"] = ((df["tenkan"] + df["kijun"]) / 2).shift(ICHI_KIJUN)
    df["span_b"] = ((high.rolling(ICHI_SENKOU).max()
                     + low.rolling(ICHI_SENKOU).min()) / 2).shift(ICHI_KIJUN)
    # Kumo FUTURO (26 velas à frente — projetado)
    df["fut_a"]  = (df["tenkan"] + df["kijun"]) / 2          # sem shift = valor futuro
    df["fut_b"]  = (high.rolling(ICHI_SENKOU).max()
                    + low.rolling(ICHI_SENKOU).min()) / 2
    # RSI
    df["rsi"]    = ta.rsi(df["close"], length=14)

    cur = df.iloc[-2]
    if any(pd.isna(x) for x in [cur["tenkan"], cur["kijun"],
                                  cur["span_a"], cur["span_b"],
                                  cur["fut_a"],  cur["fut_b"],
                                  cur["rsi"]]):
        return None

    price    = cur["close"]
    kijun    = cur["kijun"]
    kumo_top = max(cur["span_a"], cur["span_b"])
    kumo_bot = min(cur["span_a"], cur["span_b"])
    rsi      = cur["rsi"]

    # [F1] Kumo futuro alinhado
    fut_kumo_bull = cur["fut_a"] > cur["fut_b"]   # verde → favorece LONG
    fut_kumo_bear = cur["fut_a"] < cur["fut_b"]   # vermelho → favorece SHORT

    # [F2] TK cross nas últimas 3 velas (não apenas prv vs cur)
    tk_bull_cross = any(
        df["tenkan"].iloc[i-1] <= df["kijun"].iloc[i-1]
        and df["tenkan"].iloc[i] > df["kijun"].iloc[i]
        for i in range(len(df)-4, len(df)-1)
        if not pd.isna(df["tenkan"].iloc[i])
    )
    tk_bear_cross = any(
        df["tenkan"].iloc[i-1] >= df["kijun"].iloc[i-1]
        and df["tenkan"].iloc[i] < df["kijun"].iloc[i]
        for i in range(len(df)-4, len(df)-1)
        if not pd.isna(df["tenkan"].iloc[i])
    )

    # [F3] Distância ao Kijun ≥ 0.3%
    dist_kijun_pct = abs(price - kijun) / kijun * 100
    kijun_ok = dist_kijun_pct >= 0.3

    # [F5] Espessura do Kumo ≥ 0.15%
    kumo_thick_pct = abs(cur["span_a"] - cur["span_b"]) / price * 100
    kumo_thick_ok  = kumo_thick_pct >= 0.15

    # Chikou livre
    chikou_ref = df["close"].iloc[-2 - ICHI_KIJUN] if len(df) > ICHI_KIJUN + 3 else None

    # ── LONG ─────────────────────────────────────────────────────────────────
    long_ok = (
        price > kumo_top                                    # acima do kumo
        and cur["tenkan"] > cur["kijun"]                   # TK bullish
        and tk_bull_cross                                   # [F2] cross recente
        and (chikou_ref is None or price > chikou_ref)     # chikou livre
        and fut_kumo_bull                                   # [F1] kumo futuro verde
        and price > kijun                                   # [F3] acima do kijun
        and kijun_ok                                        # [F3] distância mínima
        and 45 <= rsi <= 68                                 # [F4] RSI direccional
        and kumo_thick_ok                                   # [F5] kumo sólido
    )

    # [F6] Filtro de tendência 3 dias — bloqueia SHORT se moeda subiu >2% nos últimos 3 dias
    # Evita vender moedas em alta forte (causa raiz dos -$122 da POL)
    close_3d_ago = df["close"].iloc[-74] if len(df) >= 74 else df["close"].iloc[0]  # ~3 dias em 1H
    trend_3d_pct = (price - close_3d_ago) / close_3d_ago * 100
    trend_not_bullish = trend_3d_pct < 2.0   # bloqueia SHORT se subiu >2% em 3 dias

    # ── SHORT ────────────────────────────────────────────────────────────────
    short_ok = (
        price < kumo_bot                                    # abaixo do kumo
        and cur["tenkan"] < cur["kijun"]                   # TK bearish
        and tk_bear_cross                                   # [F2] cross recente
        and (chikou_ref is None or price < chikou_ref)     # chikou livre
        and fut_kumo_bear                                   # [F1] kumo futuro vermelho
        and price < kijun                                   # [F3] abaixo do kijun
        and kijun_ok                                        # [F3] distância mínima
        and 32 <= rsi <= 55                                 # [F4] RSI direccional
        and kumo_thick_ok                                   # [F5] kumo sólido
        and trend_not_bullish                               # [F6] não vender em alta forte
    )

    if long_ok:  return "buy"
    if short_ok: return "sell"
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
# E10/E11/E12 — FAIR VALUE GAP (SOL / BNB / ETH — 15m)
# ══════════════════════════════════════════════════════════════════════════════

# Cache de gaps activos por instrumento — persiste entre ciclos do loop principal
# Estrutura: { inst_id: [ {gh, gl, side, created_bar, filled}, ... ] }
_fvg_gaps: dict[str, list[dict]] = {}
_fvg_bar_idx: dict[str, int]     = {}   # contador de velas por instrumento

def fvg_signal(df: pd.DataFrame, inst_id: str) -> str | None:
    """🔷 E10/E11/E12 FAIR VALUE GAP — SOL / BNB / ETH 15m  (backtest V9).

    Detecta lacunas de liquidez (Fair Value Gaps) criadas por velas de impulso
    e entra quando o preço retorna ao midpoint do gap.

    Confirmações obrigatórias (todas):
      1. Gap real entre high[i-2] e low[i] (bullish) ou low[i-2] e high[i] (bearish)
      2. Vela central [i-1] com corpo ≥ FVG_BODY_MULT × média20 (impulso genuíno)
      3. EMA200 alinhada com a direcção do gap
      4. RSI 35–65 (evita entradas em extremos de mercado)
      5. Retorno ao midpoint ± FVG_TOL_PCT% dentro de FVG_GAP_EXPIRY velas
      6. Gap marcado como filled após entrada — sem re-entrada no mesmo gap

    Regras de hold/strict herdadas de _fire() via classificação HOLD_PAIRS:
      SOL, BNB, ETH → HOLD (SL 5%, circuit breaker -7%)
    """
    if len(df) < 230:   # warm-up EMA200 + margem
        return None

    df   = df.copy()
    df["ema200"]  = ta.ema(df["close"], length=200)
    df["rsi"]     = ta.rsi(df["close"], length=14)
    df["body"]    = abs(df["close"] - df["open"])
    df["body_ma"] = df["body"].rolling(20).mean()

    if inst_id not in _fvg_gaps:
        _fvg_gaps[inst_id]   = []
        _fvg_bar_idx[inst_id] = 0

    bar_now = _fvg_bar_idx[inst_id]

    # ── Passo 1: detectar NOVOS gaps nas últimas 3 velas fechadas ─────────────
    # Analisamos as últimas 5 velas para não perder gaps recentes após reinício
    scan_start = max(2, len(df) - 5)
    for i in range(scan_start, len(df) - 1):
        a  = df.iloc[i - 2]
        b  = df.iloc[i - 1]   # vela central (impulso)
        c_ = df.iloc[i]

        if any(pd.isna(x) for x in [b["ema200"], b["rsi"], b["body_ma"]]):
            continue
        if b["body_ma"] == 0:
            continue

        # Filtro de corpo: impulso genuíno
        if b["body"] < b["body_ma"] * FVG_BODY_MULT:
            continue

        # Bullish FVG: high[i-2] < low[i]
        if a["high"] < c_["low"] and b["close"] > b["ema200"]:
            gap_id = f"B_{df.index[i].isoformat()}"
            if not any(g.get("id") == gap_id for g in _fvg_gaps[inst_id]):
                _fvg_gaps[inst_id].append({
                    "id": gap_id, "gh": c_["low"], "gl": a["high"],
                    "side": "buy", "created_bar": bar_now, "filled": False,
                })

        # Bearish FVG: low[i-2] > high[i]
        if a["low"] > c_["high"] and b["close"] < b["ema200"]:
            gap_id = f"S_{df.index[i].isoformat()}"
            if not any(g.get("id") == gap_id for g in _fvg_gaps[inst_id]):
                _fvg_gaps[inst_id].append({
                    "id": gap_id, "gh": a["low"], "gl": c_["high"],
                    "side": "sell", "created_bar": bar_now, "filled": False,
                })

    # ── Passo 2: verificar retorno a gaps existentes ──────────────────────────
    cur = df.iloc[-2]   # vela fechada mais recente
    if any(pd.isna(x) for x in [cur["ema200"], cur["rsi"]]):
        _fvg_bar_idx[inst_id] = bar_now + 1
        return None

    rsi_ok = 35 <= cur["rsi"] <= 65
    signal_out = None

    active_gaps = [g for g in _fvg_gaps[inst_id]
                   if not g["filled"] and bar_now - g["created_bar"] <= FVG_GAP_EXPIRY]

    for g in active_gaps:
        if not rsi_ok:
            break
        mid = (g["gh"] + g["gl"]) / 2
        tol = mid * FVG_TOL_PCT / 100

        if g["side"] == "buy" and cur["close"] > cur["ema200"]:
            if abs(cur["low"] - mid) <= tol or (cur["low"] <= mid <= cur["high"]):
                g["filled"] = True
                signal_out  = "buy"
                break

        if g["side"] == "sell" and cur["close"] < cur["ema200"]:
            if abs(cur["high"] - mid) <= tol or (cur["low"] <= mid <= cur["high"]):
                g["filled"] = True
                signal_out  = "sell"
                break

    # ── Passo 3: limpar gaps expirados ou preenchidos ─────────────────────────
    _fvg_gaps[inst_id] = [
        g for g in _fvg_gaps[inst_id]
        if not g["filled"] and bar_now - g["created_bar"] <= FVG_GAP_EXPIRY
    ]
    _fvg_bar_idx[inst_id] = bar_now + 1

    return signal_out

# ══════════════════════════════════════════════════════════════════════════════
# MONITOR — aguarda fecho de posição em thread separada
# ══════════════════════════════════════════════════════════════════════════════

def _monitor(inst_id: str, pos_side: str, side: str,
             entry: float, sl_px: float, activate_px: float,
             sym: str, dir_txt: str, bal: float, qty: int,
             tag: str = "DUO ELITE") -> None:
    global _duo_in_trade, _duo_cooldown_until
    log.info("📡 SENTINELA [%s] %s %s | SL=%.5f | Trailing activa a %.5f | STEP TRAIL V5",
             tag, sym, dir_txt, sl_px, activate_px)
    _none_streak      = 0
    _step_trail_tier  = 0   # tier 0=nenhum activado; 1-5 = grau em vigor
    while True:
        time.sleep(20)
        try:
            pos = okx_get_position(inst_id, pos_side)

            # ── 🎯 PROFIT LOCK — TP FIXO $25 — fecha imediatamente ──────────────
            if pos is not None:
                upl_tp  = float(pos.get("upl", 0) or 0)
                pos_sz_tp = int(float(pos.get("pos", qty) or qty))
                if upl_tp >= PROFIT_LOCK_USD:
                    log.info("🎯 PROFIT LOCK +$%.2f atingido — fechando %s!", upl_tp, sym)
                    try:
                        okx_cancel_all_algos(inst_id, pos_side)
                        time.sleep(0.5)
                        okx_close_market(inst_id, pos_side, pos_sz_tp)
                        tg(f"🎯 <b>PROFIT LOCK +${PROFIT_LOCK_USD:.0f} — FECHO COM LUCRO!</b>\n"
                           f"Par: <code>{sym}</code> | {dir_txt}\n"
                           f"Lucro realizado: <b>${upl_tp:+.2f} USDT</b> 🏆\n"
                           f"⏳ Cooldown 30 min activado. Aguardando próximo sinal Elite.")
                    except Exception as e:
                        log.error("profit lock close fail %s: %s", sym, e)

            # ── 🚨 GLOBAL CIRCUIT BREAKER -4% — fecho imediato, protege banca ──
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

            # ── STEP TRAILING V5 — 5 graus baseados em PnL não realizado ────────
            if pos is not None:
                upl     = float(pos.get("upl",    0) or 0)
                mark_px = float(pos.get("markPx", 0) or 0)
                avg_px  = float(pos.get("avgPx",  entry) or entry)
                pos_sz  = int(float(pos.get("pos", qty) or qty))

                if _step_trail_tier < len(STEP_TRAIL_LEVELS) and mark_px > 0 and avg_px > 0:
                    trigger_usd, lock_usd = STEP_TRAIL_LEVELS[_step_trail_tier]
                    if upl >= trigger_usd:
                        # Calcula o preço de SL que garante 'lock_usd' USDT de lucro
                        # Interpolação linear: lock_px = avg ± lock_usd*(markPx-avg)/upl
                        if side == "buy":
                            price_move = mark_px - avg_px
                            lock_px    = avg_px + lock_usd * (price_move / upl)
                        else:
                            price_move = avg_px - mark_px
                            lock_px    = avg_px - lock_usd * (price_move / upl)

                        grau = _step_trail_tier + 1
                        log.info("🔒 STEP TRAIL GRAU %d — $%.0f atingido → piso $%.0f | %s SL=%.5f",
                                 grau, trigger_usd, lock_usd, sym, lock_px)
                        try:
                            okx_cancel_all_algos(inst_id, pos_side)
                            time.sleep(1)
                            okx_initial_sl(inst_id, pos_side, pos_sz, lock_px)
                            okx_trailing_stop(inst_id, pos_side, pos_sz,
                                              mark_px * (1 + TRAIL_ACTIVATE_PCT/100) if side == "buy"
                                              else mark_px * (1 - TRAIL_ACTIVATE_PCT/100))
                            _step_trail_tier += 1
                            tg(f"🔒 <b>STEP TRAIL GRAU {grau} ACTIVADO</b>\n"
                               f"Par: <code>{sym}</code> | {dir_txt}\n"
                               f"Lucro actual: <b>${upl:+.2f} USDT</b> (trigger ${trigger_usd:.0f})\n"
                               f"🛡️ SL blindado em: <code>{lock_px:.5f}</code> (garante ${lock_usd:.0f})\n"
                               f"📡 Trailing continua activo — próximo grau: "
                               + (f"${STEP_TRAIL_LEVELS[_step_trail_tier][0]:.0f}" if _step_trail_tier < len(STEP_TRAIL_LEVELS) else "MAX atingido 🏆"))
                        except Exception as e:
                            log.error("step trail grau %d: %s — voltando a tentar", grau, e)

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

            if _step_trail_tier > 0:
                icon   = "✅" if win else "⚠️"
                result = (f"SAÍDA COM LUCRO 🎯 (Step Trail Grau {_step_trail_tier} activo)"
                          if win else f"SAÍDA ABAIXO DO PISO — Grau {_step_trail_tier} ⚠️")
            else:
                icon   = "🎯" if win else "💥"
                result = "SAÍDA COM LUCRO 🎯" if win else "SAÍDA COM PERDA 💥"

            tg(f"{icon} <b>{tag} — {result}</b>\n"
               f"Par: <code>{sym}</code> | {dir_txt}\n"
               f"Entrada: <code>{entry:.5f}</code> → Saída: <code>{exit_px:.5f}</code>\n"
               f"P&L: <b>${pnl_usd:+.2f} USDT</b> ({pnl_pct:+.2f}%)\n"
               f"⏳ Cooldown 30 min activado.")

            log.info("📊 [%s] %s fechado | exit=%.5f P&L $%.2f (%.2f%%) | step_tier=%d",
                     tag, sym, exit_px, pnl_usd, pnl_pct, _step_trail_tier)
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
      - HOLD pairs (POL/ETH/SOL): SL = HOLD_SL_PCT (5%) → só circuit breaker
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
# HELPERS — /tp  /radar  /lpd  /meta  /panic
# ══════════════════════════════════════════════════════════════════════════════

def cmd_tp() -> str:
    """Retorna P&L das posições abertas em tempo real."""
    if not _has_creds(): return "❌ Sem credenciais OKX."
    path = "/api/v5/account/positions?instType=SWAP"
    try:
        r = requests.get(f"https://www.okx.com{path}", headers=_headers("GET", path), timeout=8)
        positions = [p for p in r.json().get("data", []) if float(p.get("pos", 0) or 0) != 0]
        if not positions:
            return "📭 <b>Sem posições abertas.</b>"
        lines = ["📊 <b>Posições abertas — P&amp;L ao vivo</b>"]
        for p in positions:
            sym     = p["instId"].replace("-USDT-SWAP", "")
            side    = "LONG 🟢" if p["posSide"] == "long" else "SHORT 🔴"
            upl     = float(p.get("upl", 0) or 0)
            uplr    = float(p.get("uplRatio", 0) or 0) * 100
            avg     = float(p.get("avgPx", 0) or 0)
            mark    = float(p.get("markPx", 0) or 0)
            icon    = "✅" if upl >= 0 else "🔴"
            lines.append(f"{icon} <code>{sym}</code> {side}\n"
                         f"   Entrada: <code>{avg:.5f}</code> | Mark: <code>{mark:.5f}</code>\n"
                         f"   P&amp;L: <b>${upl:+.2f} USDT</b> ({uplr:+.2f}%)")
        return "\n".join(lines)
    except Exception as e:
        return f"❌ Erro /tp: {e}"


def _okx_open_interest(inst_id: str) -> float | None:
    """Open Interest em USDT — mede força do dinheiro no mercado."""
    try:
        r = requests.get(f"{OKX_BASE}/rubik/stat/contracts/open-interest-volume?instId={inst_id}&period=1H", timeout=8)
        d = r.json()
        if d.get("code") == "0" and d.get("data"):
            return float(d["data"][0][1])
    except Exception:
        pass
    return None

def _okx_funding_rate(inst_id: str) -> float | None:
    """Funding rate actual — positivo = mercado long demais (SHORT favorecido)."""
    try:
        r = requests.get(f"{OKX_BASE}/public/funding-rate?instId={inst_id}", timeout=8)
        d = r.json()
        if d.get("code") == "0" and d.get("data"):
            return float(d["data"][0].get("fundingRate", 0))
    except Exception:
        pass
    return None

def _okx_long_short_ratio(inst_id: str) -> float | None:
    """Rácio long/short — >1 = maioria long, <1 = maioria short."""
    try:
        r = requests.get(f"{OKX_BASE}/rubik/stat/contracts/long-short-account-ratio?instId={inst_id}&period=1H", timeout=8)
        d = r.json()
        if d.get("code") == "0" and d.get("data"):
            return float(d["data"][0][1])
    except Exception:
        pass
    return None

def cmd_radar() -> str:
    """RADAR INTELIGENTE V2 — tendência macro + OI + funding + score de confiança."""
    lines = ["📡 <b>RADAR INTELIGENTE V2</b>"]
    checks = [
        (GOLD_POL,   "POL",  "1H",  "15m", "🥇 ICHIMOKU",     ichimoku_signal),
        (DUO_SOL,    "SOL",  "15m", "15m", "🌊 SUPERTREND",    supertrend_signal),
        (SHIELD_XRP, "XRP",  "15m", "15m", "🎯 RSI DIV",       rsi_div_vwap_signal),
        (DUO_ETH,    "ETH",  "15m", "15m", "💧 VWAP KISS",     vwap_kiss_signal),
        (FVG_BNB,    "BNB",  "15m", "15m", "🔷 FVG",           lambda df: fvg_signal(df, FVG_BNB)),
        (SHIELD_ADA, "ADA",  "1H",  "1H",  "🛡️ ORDER BLOCK",  order_block_signal),
        (GOLD_DOGE,  "DOGE", "1H",  "1H",  "🎲 ORDER BLOCK",   order_block_signal),
    ]

    for inst_id, sym, bar, bar_macro, label, sig_fn in checks:
        try:
            # ── Dados principais ──────────────────────────────────────────────
            df      = okx_candles(inst_id, bar=bar, limit=300)
            df_4h   = okx_candles(inst_id, bar="4H", limit=60)
            df_1d   = okx_candles(inst_id, bar="1D", limit=10)

            df["rsi"]    = ta.rsi(df["close"], length=14)
            df["ema200"] = ta.ema(df["close"], length=200)
            df["ema20"]  = ta.ema(df["close"], length=20)

            rsi    = df["rsi"].iloc[-2]
            px     = df["close"].iloc[-2]
            ema200 = df["ema200"].iloc[-2]
            ema20  = df["ema20"].iloc[-2]
            dist20 = (px - ema20) / ema20 * 100

            # ── Tendência 4H ──────────────────────────────────────────────────
            df_4h["ema50"] = ta.ema(df_4h["close"], length=50)
            trend_4h_bull  = df_4h["close"].iloc[-2] > df_4h["ema50"].iloc[-2]

            # ── Tendência 1D ──────────────────────────────────────────────────
            trend_1d_pct   = (df_1d["close"].iloc[-1] - df_1d["close"].iloc[-4]) / df_1d["close"].iloc[-4] * 100
            trend_1d_icon  = "📈" if trend_1d_pct > 1 else ("📉" if trend_1d_pct < -1 else "➡️")

            # ── Sinal da estratégia ───────────────────────────────────────────
            try:
                sinal = sig_fn(df)
            except Exception:
                sinal = None
            sinal_txt = "🟢 LONG PRONTO" if sinal == "buy" else ("🔴 SHORT PRONTO" if sinal == "sell" else "⏳ Aguardando")

            # ── Armadilha: contra-tendência ───────────────────────────────────
            armadilha = ""
            if sinal == "sell" and trend_1d_pct > 2:
                armadilha = "⚠️ ARMADILHA: SHORT contra tendência diária de alta!"
            elif sinal == "buy" and trend_1d_pct < -2:
                armadilha = "⚠️ ARMADILHA: LONG contra tendência diária de baixa!"

            # ── Funding rate ──────────────────────────────────────────────────
            fr = _okx_funding_rate(inst_id)
            if fr is not None:
                fr_pct  = fr * 100
                fr_icon = "🔥" if fr_pct > 0.05 else ("❄️" if fr_pct < -0.05 else "➖")
                fr_txt  = f"{fr_icon} Funding: {fr_pct:+.4f}%"
            else:
                fr_txt = ""

            # ── Long/Short ratio ──────────────────────────────────────────────
            lsr = _okx_long_short_ratio(inst_id)
            if lsr is not None:
                lsr_icon = "🐂" if lsr > 1.2 else ("🐻" if lsr < 0.8 else "⚖️")
                lsr_txt  = f"{lsr_icon} L/S ratio: {lsr:.2f}"
            else:
                lsr_txt = ""

            # ── Score de confiança 0-100 ──────────────────────────────────────
            score = 0
            if sinal is not None:                          score += 35  # estratégia disparou
            if sinal == "buy"  and trend_4h_bull:          score += 20  # 4H confirma LONG
            if sinal == "sell" and not trend_4h_bull:      score += 20  # 4H confirma SHORT
            if sinal == "buy"  and trend_1d_pct > 0:       score += 15  # 1D confirma LONG
            if sinal == "sell" and trend_1d_pct < 0:       score += 15  # 1D confirma SHORT
            if px > ema200 and sinal == "buy":             score += 15  # acima EMA200
            if px < ema200 and sinal == "sell":            score += 15  # abaixo EMA200
            if fr is not None:
                if sinal == "buy"  and fr < 0:             score += 10  # funding negativo = longs pagam pouco
                if sinal == "sell" and fr > 0.05:          score += 10  # funding alto = shorts favorecidos
            if lsr is not None:
                if sinal == "buy"  and lsr < 0.9:          score += 5   # maioria short = squeeze possível
                if sinal == "sell" and lsr > 1.2:          score += 5   # maioria long = flush possível
            if armadilha:                                  score -= 30  # penalidade armadilha

            score = max(0, min(100, score))
            score_bar = "█" * (score // 10) + "░" * (10 - score // 10)
            score_icon = "🏆" if score >= 70 else ("✅" if score >= 50 else ("⚠️" if score >= 30 else "❌"))

            # ── Bloco final ───────────────────────────────────────────────────
            macro_icon = "🟢" if px > ema200 else "🔴"
            block = (
                f"\n{macro_icon} <b>{sym}</b> [{label}]\n"
                f"   Sinal: {sinal_txt}\n"
                f"   Tendência 1D: {trend_1d_icon} {trend_1d_pct:+.1f}% | 4H: {'📈' if trend_4h_bull else '📉'} | EMA200: {'acima ✅' if px > ema200 else 'abaixo ⚠️'}\n"
                f"   RSI: {rsi:.0f} | Dist EMA20: {dist20:+.2f}%\n"
            )
            if fr_txt:  block += f"   {fr_txt}"
            if lsr_txt: block += f" | {lsr_txt}\n"
            block += f"   Score: {score_icon} <code>[{score_bar}]</code> {score}/100"
            if armadilha: block += f"\n   {armadilha}"
            lines.append(block)

        except Exception as e:
            lines.append(f"\n⚠️ <code>{sym}</code>: erro ({e})")

    lines.append("\n<i>Score ≥70 = alta confiança | 50-69 = médio | &lt;50 = aguardar</i>")
    return "\n".join(lines)


def cmd_lpd() -> str:
    """P&L realizado nas últimas 24 horas (nunca antes de 18 Abr 2026)."""
    if not _has_creds(): return "❌ Sem credenciais OKX."
    # Piso absoluto: 18 Abril 2026 00:00 UTC — ignora trades de teste anteriores
    PNL_FLOOR_MS = int(datetime(2026, 4, 18, tzinfo=timezone.utc).timestamp() * 1000)
    cutoff = max(int((time.time() - 86400) * 1000), PNL_FLOOR_MS)
    path   = "/api/v5/trade/fills-history?instType=SWAP&limit=100"
    try:
        r     = requests.get(f"https://www.okx.com{path}", headers=_headers("GET", path), timeout=10)
        fills = [f for f in r.json().get("data", []) if int(f["ts"]) >= cutoff]
        if not fills:
            return "📭 <b>Sem trades realizados nas últimas 24h.</b>"
        total = sum(float(f.get("pnl", 0) or 0) for f in fills)
        fee   = sum(float(f.get("fee", 0) or 0) for f in fills)
        net   = total + fee
        icon  = "✅" if net >= 0 else "🔴"
        return (f"{icon} <b>P&amp;L últimas 24h</b>\n"
                f"Trades: <b>{len(fills)}</b>\n"
                f"Gross P&amp;L: <b>${total:+.2f}</b>\n"
                f"Comissões: <b>${fee:+.2f}</b>\n"
                f"P&amp;L Líquido: <b>${net:+.2f} USDT</b>")
    except Exception as e:
        return f"❌ Erro /lpd: {e}"


def cmd_meta() -> str:
    """Progresso em relação à meta de $600/mês (desde 18 Abr 2026)."""
    if not _has_creds(): return "❌ Sem credenciais OKX."
    now   = datetime.now(timezone.utc)
    # Hardcoded: apenas trades a partir de 18 Abril 2026 — ignora trades de teste
    start = int(datetime(2026, 4, 18, tzinfo=timezone.utc).timestamp() * 1000)
    path  = "/api/v5/trade/fills-history?instType=SWAP&limit=100"
    try:
        r     = requests.get(f"https://www.okx.com{path}", headers=_headers("GET", path), timeout=10)
        fills = [f for f in r.json().get("data", []) if int(f["ts"]) >= start]
        total = sum(float(f.get("pnl", 0) or 0) for f in fills)
        fee   = sum(float(f.get("fee", 0) or 0) for f in fills)
        net   = total + fee
        pct   = min(net / MONTHLY_GOAL_USD * 100, 100.0) if MONTHLY_GOAL_USD > 0 else 0.0
        filled = int(pct / 5)
        bar   = "█" * filled + "░" * (20 - filled)
        icon  = "🏆" if pct >= 100 else ("🔥" if pct >= 50 else "📈")
        return (f"{icon} <b>META MENSAL — {now.strftime('%B %Y')}</b>\n"
                f"<code>[{bar}]</code> {pct:.1f}%\n"
                f"Realizado: <b>${net:+.2f}</b> / Meta: <b>${MONTHLY_GOAL_USD:.0f}</b>\n"
                f"Faltam: <b>${max(MONTHLY_GOAL_USD - net, 0):.2f} USDT</b>\n"
                f"Trades desde 18 Abr: {len(fills)}")
    except Exception as e:
        return f"❌ Erro /meta: {e}"


# ── /gv5 — Forçar avaliação imediata do Step Trail V5 ─────────────────────────
def cmd_gv5() -> str:
    """Avalia AGORA todas as posições abertas e aplica o grau Step Trail V5
    correspondente ao lucro actual (idempotente: re-aplica o mesmo nível se já
    estiver activo). Útil quando o utilizador quer forçar o lock sem esperar o
    próximo ciclo de 20s do monitor."""
    if not _has_creds(): return "❌ Sem credenciais OKX."
    path = "/api/v5/account/positions?instType=SWAP"
    try:
        r = requests.get(f"https://www.okx.com{path}", headers=_headers("GET", path), timeout=8)
        positions = [p for p in r.json().get("data", []) if float(p.get("pos", 0) or 0) != 0]
    except Exception as e:
        return f"❌ Erro /gv5: {e}"
    if not positions:
        return "📭 <b>GV5:</b> Sem posições abertas."

    actions: list[str] = []
    travado = False
    for p in positions:
        sym     = p["instId"].replace("-USDT-SWAP", "")
        inst_id = p["instId"]
        pos_side = p["posSide"]
        side     = "buy" if pos_side == "long" else "sell"
        upl      = float(p.get("upl", 0) or 0)
        avg_px   = float(p.get("avgPx",  0) or 0)
        mark_px  = float(p.get("markPx", 0) or 0)
        pos_sz   = int(float(p.get("pos", 0) or 0))

        if avg_px <= 0 or mark_px <= 0 or pos_sz <= 0:
            actions.append(f"⚠️ <code>{sym}</code>: dados inválidos.")
            continue

        # Encontra o grau MÁXIMO actualmente atingido (1-5)
        tier_atingido = -1
        for i, (trigger_usd, _lock_usd) in enumerate(STEP_TRAIL_LEVELS):
            if upl >= trigger_usd:
                tier_atingido = i

        if tier_atingido < 0:
            need = STEP_TRAIL_LEVELS[0][0]
            actions.append(f"⏸ <code>{sym}</code>: lucro <b>${upl:+.2f}</b> &lt; trigger Grau 1 (${need:.0f})")
            continue

        trigger_usd, lock_usd = STEP_TRAIL_LEVELS[tier_atingido]
        # Calcula preço de SL que garante 'lock_usd' USDT de lucro
        if side == "buy":
            price_move = mark_px - avg_px
            lock_px    = avg_px + lock_usd * (price_move / upl)
        else:
            price_move = avg_px - mark_px
            lock_px    = avg_px - lock_usd * (price_move / upl)

        grau = tier_atingido + 1
        try:
            okx_cancel_all_algos(inst_id, pos_side); time.sleep(1)
            okx_initial_sl(inst_id, pos_side, pos_sz, lock_px)
            # Re-arma trailing logo acima do mark actual
            act_px = mark_px * (1 + TRAIL_ACTIVATE_PCT/100) if side == "buy" else mark_px * (1 - TRAIL_ACTIVATE_PCT/100)
            okx_trailing_stop(inst_id, pos_side, pos_sz, act_px)
            travado = True
            actions.append(
                f"🔒 <code>{sym}</code> Grau <b>{grau}</b> aplicado\n"
                f"   Lucro: <b>${upl:+.2f}</b> | Lock: <b>${lock_usd:.0f}</b>\n"
                f"   SL blindado: <code>{lock_px:.5f}</code>")
        except Exception as e:
            actions.append(f"❌ <code>{sym}</code> Grau {grau} falhou: {e}")

    header = "🚀 <b>GV5: Lucro travado com sucesso!</b>" if travado else "ℹ️ <b>GV5 — análise concluída</b>"
    return header + "\n\n" + "\n".join(actions)


# ── /force [coin] — Ordem manual ignorando filtros (RSI direccional) ──────────
_FORCE_MAP = {
    "pol": GOLD_POL, "eth": DUO_ETH, "sol": DUO_SOL,
    "xrp": SHIELD_XRP, "ada": SHIELD_ADA, "doge": GOLD_DOGE, "bnb": FVG_BNB,
}

def cmd_force(coin: str) -> str:
    """Abre ordem de mercado IGNORANDO filtros de estratégia.
    Direcção decidida por RSI 15m: > 50 → LONG  |  < 50 → SHORT.
    Usa LEVERAGE=5x e SAFETY_MARGIN=3% (mesma config do bot)."""
    if not _has_creds(): return "❌ Sem credenciais OKX."
    coin = coin.lower().strip()
    if coin not in _FORCE_MAP:
        return ("❌ <b>/force</b> — moeda inválida.\n"
                "Usar: <code>/force pol|eth|sol|xrp|ada|doge|bnb</code>")
    inst_id = _FORCE_MAP[coin]
    sym     = coin.upper()

    # Bloqueia se já houver posição
    existing = okx_any_position_open(ALL_SYMS)
    if existing is not None:
        ex_sym, ex_ps = existing
        return (f"🛑 <b>/force {sym} bloqueado</b>\n"
                f"Posição já aberta: <code>{ex_sym.replace('-USDT-SWAP','')}</code> {ex_ps.upper()}.\n"
                f"Fecha primeiro com <code>/panic</code> ou aguarda saída natural.")

    # RSI 15m direccional
    try:
        df = okx_candles(inst_id, bar="15m", limit=50)
        df["rsi"] = ta.rsi(df["close"], length=14)
        rsi = float(df["rsi"].iloc[-2])
    except Exception as e:
        return f"❌ /force {sym}: erro RSI ({e})"

    side = "buy" if rsi > 50 else "sell"
    dir_txt = "LONG 🟢" if side == "buy" else "SHORT 🔴"

    tg(f"⚡ <b>/force {sym}</b> — RSI={rsi:.1f} → {dir_txt}\nA executar...")
    # _fire() trata de tudo: ordem + SL + trailing + monitor
    ok = _fire(inst_id, side, f"FORCE RSI={rsi:.0f}", tag="🎯 FORCE")
    if ok:
        return (f"✅ <b>/force {sym} EXECUTADA</b>\n"
                f"Direcção: {dir_txt}  |  RSI={rsi:.1f}\n"
                f"Leverage: 5x  |  Safety: 3%  |  Step Trail V5 activo")
    return f"❌ <b>/force {sym} falhou.</b> Ver logs para detalhes."


# ── /risco — Análise táctica de risco da posição aberta ───────────────────────
def okx_orderbook(inst_id: str, depth: int = 10) -> tuple[list, list] | None:
    """Top-N bids/asks da OKX. Cada item: [price, size, ...]."""
    try:
        r = requests.get(f"{OKX_BASE}/market/books?instId={inst_id}&sz={depth}", timeout=8)
        d = r.json()
        if d.get("code") != "0" or not d.get("data"): return None
        bk = d["data"][0]
        return bk.get("bids", []), bk.get("asks", [])
    except Exception:
        return None


def cmd_risco() -> str:
    """Análise táctica: pressão do book, distância ao SL e veredito SAFE/CAUTION/DANGER."""
    if not _has_creds(): return "❌ Sem credenciais OKX."
    path = "/api/v5/account/positions?instType=SWAP"
    try:
        r = requests.get(f"https://www.okx.com{path}", headers=_headers("GET", path), timeout=8)
        positions = [p for p in r.json().get("data", []) if float(p.get("pos", 0) or 0) != 0]
    except Exception as e:
        return f"❌ Erro /risco: {e}"
    if not positions:
        return "📭 <b>/risco:</b> Sem posições abertas para analisar."

    blocks: list[str] = []
    for p in positions:
        sym      = p["instId"].replace("-USDT-SWAP", "")
        inst_id  = p["instId"]
        pos_side = p["posSide"]
        side_emoji = "LONG 🟢" if pos_side == "long" else "SHORT 🔴"
        avg_px   = float(p.get("avgPx",  0) or 0)
        mark_px  = float(p.get("markPx", 0) or 0)
        upl      = float(p.get("upl", 0) or 0)

        # 1) Pressão do book — top 10 níveis
        ob = okx_orderbook(inst_id, depth=10)
        if ob is None:
            book_line = "📚 Book: indisponível"
            book_score = 0.0
        else:
            bids, asks = ob
            buy_vol  = sum(float(x[1]) for x in bids[:10])
            sell_vol = sum(float(x[1]) for x in asks[:10])
            tot      = buy_vol + sell_vol
            buy_pct  = (buy_vol / tot * 100) if tot > 0 else 50.0
            sell_pct = 100.0 - buy_pct
            # Score: positivo = a favor da posição
            if pos_side == "long":
                book_score = buy_pct - sell_pct  # +N → bids dominam (bom para LONG)
                fav = "✅ favorável" if book_score > 10 else ("⚖️ equilibrado" if abs(book_score) <= 10 else "⚠️ contra")
            else:
                book_score = sell_pct - buy_pct
                fav = "✅ favorável" if book_score > 10 else ("⚖️ equilibrado" if abs(book_score) <= 10 else "⚠️ contra")
            book_line = (f"📚 <b>Book top-10:</b> compras {buy_pct:.1f}% | vendas {sell_pct:.1f}% — {fav}")

        # 2) Distância ao SL de 5% (HOLD) — em % de preço (não alavancado)
        sl_pct = HOLD_SL_PCT  # 5.0
        if avg_px > 0 and mark_px > 0:
            if pos_side == "long":
                sl_px = avg_px * (1 - sl_pct/100)
                dist_to_sl = (mark_px - sl_px) / mark_px * 100
            else:
                sl_px = avg_px * (1 + sl_pct/100)
                dist_to_sl = (sl_px - mark_px) / mark_px * 100
            sl_line = (f"🛡️ <b>Distância ao SL ({sl_pct:.0f}%):</b> "
                       f"<code>{sl_px:.5f}</code> — falta <b>{dist_to_sl:+.2f}%</b>")
        else:
            dist_to_sl = 999.0
            sl_line    = "🛡️ Distância ao SL: dados inválidos"

        # 3) Veredito táctico — combina lucro + book + distância ao SL
        if upl > 0 and book_score >= 0 and dist_to_sl > 2.5:
            verdict = "✅ <b>SAFE</b> — manter posição, Step Trail tratará dos lucros"
        elif upl < 0 and book_score < -10 and dist_to_sl < 1.5:
            verdict = "🚨 <b>DANGER</b> — sair fora (considerar /panic ou fecho manual)"
        elif (upl < 0 and book_score < 0) or dist_to_sl < 1.5:
            verdict = "⚠️ <b>CAUTION</b> — atenção redobrada, condições deteriorando"
        else:
            verdict = "🟡 <b>CAUTION</b> — situação neutra, monitorar"

        blocks.append(
            f"<b>📊 {sym} {side_emoji}</b>\n"
            f"   Entrada: <code>{avg_px:.5f}</code> | Mark: <code>{mark_px:.5f}</code>\n"
            f"   P&amp;L: <b>${upl:+.2f} USDT</b>\n"
            f"   {book_line}\n"
            f"   {sl_line}\n"
            f"   <b>Veredito:</b> {verdict}")
    return "🎯 <b>/risco — Análise táctica</b>\n\n" + "\n\n".join(blocks)


def cmd_panic() -> str:
    """Fecha todas as posições e pausa o bot por 5 minutos."""
    global _bot_authorized, _panic_until
    closed = []
    errors = []
    for inst_id in ALL_SYMS:
        for ps in ("long", "short"):
            try:
                pos = okx_get_position(inst_id, ps)
                if pos and float(pos.get("pos", 0) or 0) != 0:
                    sz = int(float(pos["pos"]))
                    okx_cancel_all_algos(inst_id, ps)
                    time.sleep(0.3)
                    okx_close_market(inst_id, ps, sz)
                    closed.append(f"{inst_id.replace('-USDT-SWAP','')} {ps.upper()}")
            except Exception as e:
                errors.append(f"{inst_id}: {e}")
    with _auth_lock:
        _bot_authorized = False
    _save_state(False)
    _panic_until = time.time() + 300   # 5 min
    result = "🚨 <b>PANIC EXECUTADO</b>\n"
    if closed: result += f"Fechadas: {', '.join(closed)}\n"
    else:      result += "Sem posições abertas para fechar.\n"
    if errors: result += f"⚠️ Erros: {'; '.join(errors)}\n"
    result += "⛔ Bot <b>PAUSADO por 5 minutos</b>. Use /start para retomar antes."
    return result


# ══════════════════════════════════════════════════════════════════════════════
# /backtest — Backtest real 100 dias com dados OKX
# ══════════════════════════════════════════════════════════════════════════════

def _bt_ichimoku(df: pd.DataFrame) -> str | None:
    """Ichimoku simplificado para backtest — mesma lógica do E09."""
    if len(df) < 80: return None
    high, low, close = df["high"], df["low"], df["close"]
    tenkan = (high.rolling(9).max()  + low.rolling(9).min())  / 2
    kijun  = (high.rolling(26).max() + low.rolling(26).min()) / 2
    span_a = ((tenkan + kijun) / 2).shift(26)
    span_b = ((high.rolling(52).max() + low.rolling(52).min()) / 2).shift(26)
    fut_a  = (tenkan + kijun) / 2
    fut_b  = (high.rolling(52).max() + low.rolling(52).min()) / 2
    rsi    = ta.rsi(close, length=14)
    cur = df.iloc[-1]
    i   = len(df) - 1
    if any(pd.isna(x) for x in [tenkan.iloc[i], kijun.iloc[i], span_a.iloc[i], span_b.iloc[i], rsi.iloc[i]]):
        return None
    px      = cur["close"]
    kumo_top = max(span_a.iloc[i], span_b.iloc[i])
    kumo_bot = min(span_a.iloc[i], span_b.iloc[i])
    # Filtro F6: tendência 3 dias
    idx_3d = max(0, i - 72)
    trend_3d = (px - df["close"].iloc[idx_3d]) / df["close"].iloc[idx_3d] * 100
    tk_bull = any(tenkan.iloc[j-1] <= kijun.iloc[j-1] and tenkan.iloc[j] > kijun.iloc[j]
                  for j in range(max(1,i-3), i+1) if not pd.isna(tenkan.iloc[j]))
    tk_bear = any(tenkan.iloc[j-1] >= kijun.iloc[j-1] and tenkan.iloc[j] < kijun.iloc[j]
                  for j in range(max(1,i-3), i+1) if not pd.isna(tenkan.iloc[j]))
    r = rsi.iloc[i]
    if (px > kumo_top and tenkan.iloc[i] > kijun.iloc[i] and tk_bull
            and fut_a.iloc[i] > fut_b.iloc[i] and 45 <= r <= 68):
        return "buy"
    if (px < kumo_bot and tenkan.iloc[i] < kijun.iloc[i] and tk_bear
            and fut_a.iloc[i] < fut_b.iloc[i] and 32 <= r <= 55
            and trend_3d < 2.0):
        return "sell"
    return None

def _bt_fvg(df: pd.DataFrame) -> str | None:
    """FVG simplificado para backtest."""
    if len(df) < 230: return None
    df = df.copy()
    df["ema200"] = ta.ema(df["close"], length=200)
    df["rsi"]    = ta.rsi(df["close"], length=14)
    df["body"]   = abs(df["close"] - df["open"])
    df["bma"]    = df["body"].rolling(20).mean()
    cur = df.iloc[-1]
    if pd.isna(cur["ema200"]) or pd.isna(cur["rsi"]): return None
    if not (35 <= cur["rsi"] <= 65): return None
    for i in range(len(df)-4, len(df)-1):
        a, b, c = df.iloc[i-2], df.iloc[i-1], df.iloc[i]
        if pd.isna(b["bma"]) or b["bma"] == 0: continue
        if b["body"] < b["bma"]: continue
        if a["high"] < c["low"] and cur["close"] > cur["ema200"]:
            mid = (c["low"] + a["high"]) / 2
            if abs(cur["close"] - mid) / mid * 100 <= 0.3:
                return "buy"
        if a["low"] > c["high"] and cur["close"] < cur["ema200"]:
            mid = (a["low"] + c["high"]) / 2
            if abs(cur["close"] - mid) / mid * 100 <= 0.3:
                return "sell"
    return None

def _bt_rsi_div(df: pd.DataFrame) -> str | None:
    """RSI Divergence simplificado para backtest."""
    if len(df) < 50: return None
    df = df.copy()
    df["rsi"]  = ta.rsi(df["close"], length=14)
    df["date"] = df.index.date
    df["tp"]   = (df["high"] + df["low"] + df["close"]) / 3
    df["tpv"]  = df["tp"] * df["vol"]
    df["vwap"] = df.groupby("date")["tpv"].cumsum() / df.groupby("date")["vol"].cumsum()
    cur, prv   = df.iloc[-1], df.iloc[-2]
    if pd.isna(cur["rsi"]) or pd.isna(cur["vwap"]): return None
    win = df.iloc[-32:-1]
    if win.empty: return None
    p_low_idx = win["low"].idxmin()
    r_at_low  = df.loc[p_low_idx, "rsi"]
    if (cur["low"] < win["low"].min() * 0.999 and cur["rsi"] > r_at_low + 5
            and cur["close"] > cur["vwap"] and prv["rsi"] < 40 and cur["rsi"] >= 40):
        return "buy"
    p_hi_idx = win["high"].idxmax()
    r_at_hi  = df.loc[p_hi_idx, "rsi"]
    if (cur["high"] > win["high"].max() * 1.001 and cur["rsi"] < r_at_hi - 5
            and cur["close"] < cur["vwap"] and prv["rsi"] > 60 and cur["rsi"] <= 60):
        return "sell"
    return None

def cmd_backtest() -> str:
    """Backtest real 100 dias — busca candles OKX e simula cada estratégia vela a vela."""
    tg("⏳ <b>BACKTEST INICIADO</b>\nBuscando 100 dias de dados reais OKX...\nAguarda ~40 segundos.")

    CONFIGS = [
        ("🥇 POL Ichimoku 1H",    GOLD_POL,   "1H",  _bt_ichimoku, 5.0, 4.0),
        ("🔷 FVG ETH 15m",        DUO_ETH,    "15m", _bt_fvg,      5.0, 4.0),
        ("🎯 XRP RSI Div 15m",    SHIELD_XRP, "15m", _bt_rsi_div,  1.5, 4.0),
        ("🔷 FVG SOL 15m",        DUO_SOL,    "15m", _bt_fvg,      5.0, 4.0),
        ("🔷 FVG BNB 15m",        FVG_BNB,    "15m", _bt_fvg,      5.0, 4.0),
    ]

    BANCA       = 800.0
    TP_USD      = 25.0
    COOLDOWN_B  = 4      # velas de cooldown após trade
    TAXA_PCT    = 0.001  # 0.1% round-trip (taker×2)
    results     = []

    for nome, inst_id, bar, sig_fn, sl_pct, cb_pct in CONFIGS:
        try:
            limit = 500 if bar == "1H" else 700
            df_full = okx_candles(inst_id, bar=bar, limit=limit)
            if len(df_full) < 100:
                results.append((nome, 0, 0, 0, 0, 0, 0))
                continue

            trades, wins, losses = 0, 0, 0
            gross_win = gross_loss = taxas = 0.0
            cooldown  = 0
            in_trade  = False
            entry_px  = 0.0
            trade_side = ""
            sl_px     = 0.0

            for i in range(250, len(df_full)):
                df_slice = df_full.iloc[:i].copy()
                cur_px   = df_full.iloc[i]["close"]

                # Gerir trade aberto
                if in_trade:
                    pnl_pct = ((cur_px - entry_px) / entry_px * 100) if trade_side == "buy" \
                              else ((entry_px - cur_px) / entry_px * 100)
                    pnl_usd = BANCA * pnl_pct / 100 * 5  # 5x leverage

                    # TP fixo $25
                    if pnl_usd >= TP_USD:
                        wins      += 1
                        gross_win += pnl_usd
                        taxas     += BANCA * TAXA_PCT
                        trades    += 1
                        in_trade   = False
                        cooldown   = COOLDOWN_B
                        continue

                    # CB -4% ou SL
                    adverse = -pnl_pct
                    if adverse >= cb_pct or (sl_pct < cb_pct and adverse >= sl_pct):
                        losses     += 1
                        gross_loss += pnl_usd  # negativo
                        taxas      += BANCA * TAXA_PCT
                        trades     += 1
                        in_trade    = False
                        cooldown    = COOLDOWN_B
                        continue
                    continue

                if cooldown > 0:
                    cooldown -= 1
                    continue

                # Verificar sinal
                try:
                    sig = sig_fn(df_slice)
                except Exception:
                    sig = None

                if sig in ("buy", "sell"):
                    in_trade   = True
                    entry_px   = cur_px
                    trade_side = sig

            # Fechar trade aberto no fim
            if in_trade:
                cur_px  = df_full.iloc[-1]["close"]
                pnl_pct = ((cur_px - entry_px) / entry_px * 100) if trade_side == "buy" \
                          else ((entry_px - cur_px) / entry_px * 100)
                pnl_usd = BANCA * pnl_pct / 100 * 5
                trades += 1
                if pnl_usd >= 0: wins += 1; gross_win += pnl_usd
                else:             losses += 1; gross_loss += pnl_usd
                taxas += BANCA * TAXA_PCT

            liquido = gross_win + gross_loss - taxas
            wr      = wins / trades * 100 if trades else 0
            pf      = gross_win / abs(gross_loss) if gross_loss != 0 else 99.0
            results.append((nome, trades, wins, losses, gross_win, gross_loss, liquido, wr, pf, taxas))

        except Exception as e:
            results.append((nome, 0, 0, 0, 0, 0, 0, 0, 0, 0))
            log.error("backtest %s: %s", nome, e)

    # Formatar resultado
    lines = ["📊 <b>BACKTEST REAL — DADOS OKX</b>\n"
             "Período: ~100 dias | TP=$25 | Lev 5× | CB -4%\n"
             "━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    results_sorted = sorted(results, key=lambda x: x[6] if len(x) > 6 else 0, reverse=True)

    for r in results_sorted:
        if len(r) < 9: continue
        nome, trades, wins, losses, gw, gl, liq, wr, pf, tax = r
        icon = "🏆" if liq > 300 else ("✅" if liq > 0 else "❌")
        lines.append(
            f"\n{icon} <b>{nome}</b>\n"
            f"   Trades: {trades} | ✅ {wins} ({wr:.0f}%) | ❌ {losses}\n"
            f"   Lucro bruto: <b>+${gw:.0f}</b> | Perdas: <b>-${abs(gl):.0f}</b>\n"
            f"   Taxas: -${tax:.0f} | PF: {pf:.2f}\n"
            f"   <b>LÍQUIDO: ${liq:+.0f} USDT</b>"
        )

    lines.append("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    best = results_sorted[0] if results_sorted and len(results_sorted[0]) > 6 else None
    if best:
        lines.append(f"🎯 <b>MELHOR ESTRATÉGIA: {best[0]}</b>\n"
                     f"   Líquido: <b>${best[6]:+.0f} USDT</b> | Win Rate: {best[7]:.0f}%")
    lines.append("\n<i>⚠️ Backtest não garante resultados futuros.</i>")
    return "\n".join(lines)

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

    if not auth:
        status = "⛔ PAUSADO"
    elif open_pos is not None:
        s, ps = open_pos
        status = f"🔴 EM TRADE — {s.replace('-USDT-SWAP','')} {ps.upper()}"
    elif in_trade:
        status = "🔴 TRADE ATIVA"
    elif now < ld:
        status = f"🔇 LOCKDOWN {max(0,ld-now)/60:.0f}min"
    elif now < cd:
        status = f"⏳ Cooldown {max(0,cd-now)/60:.0f}min"
    else:
        status = "🟢 Aguardando sinal"

    bal_str = "—"
    if full is not None:
        eq, avail = full
        bal_str = f"<b>${eq:,.2f}</b> total | <b>${avail:,.2f}</b> livre"

    return (f"📊 <b>COMMANDER V9 — {datetime.now(timezone.utc).strftime('%d/%m %H:%M UTC')}</b>\n"
            f"💰 {bal_str}\n"
            f"Status: {status}\n"
            f"⚙️ Alavancagem: <b>{LEVERAGE}×</b>  |  CB -{CIRCUIT_BREAKER_PCT:.0f}%  |  SL HOLD {HOLD_SL_PCT:.0f}%  |  cd 30min\n"
            f"🔥 TODOS os 7 pares entram AUTOMÁTICO\n"
            f"POL · SOL · ETH · XRP · BNB · ADA · DOGE\n\n"
            f"<b>COMANDOS:</b>\n"
            f"/tp /radar /lpd /meta /status /panic\n"
            f"/go[coin] /gv5 /force [coin] /risco\n"
            f"/subir6x /subir7x  |  /pause → só /start desbloqueia")

def report_loop() -> None:
    last = time.time()
    while True:
        time.sleep(60)
        if time.time() - last >= 1800:
            try: tg(_status_text())
            except Exception as e: log.warning("report: %s", e)
            last = time.time()

_tg_offset = 0

# Mapeamento /go[coin] → inst_id
_GO_MAP = {
    "goeth":  DUO_ETH,
    "gosol":  DUO_SOL,
    "goxrp":  SHIELD_XRP,
    "goada":  SHIELD_ADA,
    "godoge": GOLD_DOGE,
    "gobnb":  FVG_BNB,
}

def telegram_commands_loop() -> None:
    global _tg_offset, _bot_authorized, _panic_until, LEVERAGE
    if not TELEGRAM_TOKEN:
        log.warning("TELEGRAM_TOKEN não configurado — comandos desativados.")
        return
    # ── Apaga webhook activo (conflito com getUpdates) ───────────────────────
    # Aguarda 6s para garantir que outros serviços já registaram o seu webhook
    # e depois apagamos — polling prevalece sobre webhook para este bot.
    time.sleep(6)
    for attempt in range(3):
        try:
            r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/deleteWebhook",
                              json={"drop_pending_updates": False}, timeout=10)
            ok = r.json().get("result", False)
            log.info("📱 deleteWebhook tentativa %d: %s", attempt + 1, "OK ✓" if ok else r.json())
            if ok:
                break
        except Exception as e:
            log.warning("deleteWebhook: %s", e)
        time.sleep(2)
    log.info("📱 Telegram commands polling activo.")
    while True:
        # ── auto-resume após panic pause ─────────────────────────────────────
        if _panic_until > 0 and time.time() > _panic_until:
            with _auth_lock: _bot_authorized = True
            _save_state(True)
            _panic_until = 0.0
            tg("✅ <b>Panic pause expirado — bot RETOMADO automaticamente.</b>")
            log.info("Panic pause expirado — bot autorizado.")
        try:
            r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                             params={"offset": _tg_offset, "timeout": 25, "limit": 10}, timeout=30)
            for upd in r.json().get("result", []):
                _tg_offset = upd["update_id"] + 1
                msg = upd.get("message") or upd.get("edited_message")
                if not msg or not msg.get("text"): continue
                chat_id = msg["chat"]["id"]
                _txt    = msg["text"].strip()
                _parts  = _txt.lower().split()
                cmd     = _parts[0].lstrip("/").split("@")[0]
                args    = _parts[1:]   # argumentos (ex: /force pol → ["pol"])

                # ── controlo ──────────────────────────────────────────────────
                if cmd in ("start", "resume", "on", "autorizar"):
                    with _auth_lock: _bot_authorized = True
                    _save_state(True)
                    _panic_until = 0.0
                    tg("✅ <b>V9 COMMANDER AUTORIZADO</b>\n"
                       "🔥 TODOS os pares entram AUTOMÁTICO quando há sinal\n"
                       "POL/SOL/ETH/XRP/ADA/DOGE/BNB — sem confirmação manual\n"
                       f"⚙️ Alavancagem actual: <b>{LEVERAGE}×</b>", chat_id)
                    log.info("Bot autorizado via Telegram")

                elif cmd in ("pause", "stop", "off", "pausar"):
                    # /pause = pausa PERMANENTE — só /start desbloqueia (sem auto-resume)
                    with _auth_lock: _bot_authorized = False
                    _save_state(False)
                    _panic_until = 0.0   # cancela qualquer auto-resume pendente
                    tg("⛔ <b>Bot PAUSADO</b>\n"
                       "O bot <b>não retoma automaticamente</b>.\n"
                       "Usa <code>/start</code> para autorizar novamente.", chat_id)
                    log.info("Bot pausado (permanente) via Telegram")

                # ── /subir6x — muda alavancagem para 6× ──────────────────────
                elif cmd == "subir6x":
                    LEVERAGE = 6
                    _LEVERAGE_SET.clear()   # força re-aplicação em todos os pares
                    for sym in ALL_SYMS:
                        try: okx_set_leverage(sym)
                        except Exception as e: log.warning("lev6x %s: %s", sym, e)
                    tg("⚙️ <b>Alavancagem → 6×</b>\n"
                       "Aplicado em todos os pares.\n"
                       "⚠️ Margem por trade aumenta — certifica-te que a banca suporta.\n"
                       "Usa <code>/subir7x</code> para 7× ou <code>/start</code> para confirmar estado.", chat_id)
                    log.info("Alavancagem alterada para 6x via Telegram")

                # ── /subir7x — muda alavancagem para 7× ──────────────────────
                elif cmd == "subir7x":
                    LEVERAGE = 7
                    _LEVERAGE_SET.clear()
                    for sym in ALL_SYMS:
                        try: okx_set_leverage(sym)
                        except Exception as e: log.warning("lev7x %s: %s", sym, e)
                    tg("⚙️ <b>Alavancagem → 7×</b>\n"
                       "Aplicado em todos os pares.\n"
                       "⚠️ Risco de liquidação aumenta — circuit breaker -7% continua activo.\n"
                       "Usa <code>/status</code> para confirmar estado.", chat_id)
                    log.info("Alavancagem alterada para 7x via Telegram")

                elif cmd in ("status", "s"):
                    try: tg(_status_text(), chat_id)
                    except Exception as e: tg(f"Erro: {e}", chat_id)

                # ── /tp — P&L posições abertas ────────────────────────────────
                elif cmd == "tp":
                    try: tg(cmd_tp(), chat_id)
                    except Exception as e: tg(f"Erro /tp: {e}", chat_id)

                # ── /radar — proximidade aos triggers ─────────────────────────
                elif cmd == "radar":
                    try: tg(cmd_radar(), chat_id)
                    except Exception as e: tg(f"Erro /radar: {e}", chat_id)

                # ── /lpd — P&L realizado últimas 24h ──────────────────────────
                elif cmd == "lpd":
                    try: tg(cmd_lpd(), chat_id)
                    except Exception as e: tg(f"Erro /lpd: {e}", chat_id)

                # ── /meta — progresso meta mensal $600 ────────────────────────
                elif cmd == "meta":
                    try: tg(cmd_meta(), chat_id)
                    except Exception as e: tg(f"Erro /meta: {e}", chat_id)

                # ── /panic — fecha tudo + pausa 5 min ─────────────────────────
                elif cmd == "panic":
                    try:
                        tg("⚠️ A executar PANIC...", chat_id)
                        tg(cmd_panic(), chat_id)
                    except Exception as e: tg(f"Erro /panic: {e}", chat_id)

                # ── /gv5 — força check Step Trail V5 e trava lucros ───────────
                elif cmd == "gv5":
                    try: tg(cmd_gv5(), chat_id)
                    except Exception as e: tg(f"Erro /gv5: {e}", chat_id)

                # ── /force [coin] — ordem de mercado bypass filtros ───────────
                elif cmd == "force":
                    if not args:
                        tg("❌ <b>/force</b> precisa de moeda.\n"
                           "Usar: <code>/force pol|eth|sol|xrp|ada|doge</code>", chat_id)
                    else:
                        try: tg(cmd_force(args[0]), chat_id)
                        except Exception as e: tg(f"Erro /force: {e}", chat_id)

                # ── /risco — análise táctica da posição aberta ────────────
                elif cmd == "risco":
                    try: tg(cmd_risco(), chat_id)
                    except Exception as e: tg(f"Erro /risco: {e}", chat_id)

                # ── /backtest — backtest real 100 dias via OKX ───────────────
                elif cmd == "backtest":
                    def _run_bt():
                        try: tg(cmd_backtest(), chat_id)
                        except Exception as e: tg(f"❌ Erro /backtest: {e}", chat_id)
                    threading.Thread(target=_run_bt, daemon=True, name="backtest").start()

                # ── /go[coin] — confirma sinal pendente ───────────────────────
                elif cmd in _GO_MAP:
                    inst_id = _GO_MAP[cmd]
                    with _pending_lock:
                        entry = _pending_signals.pop(inst_id, None)
                    if entry is None:
                        tg(f"ℹ️ Sem sinal pendente para <code>{cmd[2:].upper()}</code>.", chat_id)
                    elif time.time() > entry[4]:
                        tg(f"⌛ Sinal <code>{cmd[2:].upper()}</code> expirado (>120s). Aguarda próxima oportunidade.", chat_id)
                    else:
                        _, sig_side, sig_name, sig_tag, _ = entry
                        tg(f"🚀 <b>GO confirmado — executando {cmd[2:].upper()}...</b>", chat_id)
                        threading.Thread(
                            target=_fire, args=(inst_id, sig_side, sig_name),
                            kwargs={"tag": sig_tag}, daemon=True).start()

                # ── /help ──────────────────────────────────────────────────────
                elif cmd in ("help", "ajuda"):
                    tg("🤖 <b>V9 COMMANDER — FULL SQUAD (10 estratégias)</b>\n\n"
                       "<b>Controlo:</b>\n"
                       "/start — Autorizar bot\n"
                       "/pause — ⛔ Pausa PERMANENTE (só /start desbloqueia)\n"
                       "/panic — 🚨 Fecha tudo + pausa 5min\n\n"
                       "<b>Alavancagem:</b>\n"
                       "/subir6x — Mudar para 6× (aplica imediatamente)\n"
                       "/subir7x — Mudar para 7× (aplica imediatamente)\n\n"
                       "<b>Info &amp; análise:</b>\n"
                       "/status — Estado + saldo + alavancagem actual\n"
                       "/tp — P&amp;L posições abertas\n"
                       "/radar — RSI/proximidade triggers\n"
                       "/lpd — P&amp;L realizado 24h\n"
                       "/meta — Progresso meta $600/mês\n"
                       "/risco — Análise táctica (book + SL + veredito)\n\n"
                       "<b>Acção manual:</b>\n"
                       "/go[coin] — Confirma sinal pendente (120s)\n"
                       "  /goeth  /gosol  /goxrp  /goada  /godoge  /gobnb\n"
                       "/force [coin] — Ordem mercado bypass filtros\n"
                       "  Ex: <code>/force bnb</code>  (RSI 15m decide LONG/SHORT)\n"
                       "/gv5 — Força check Step Trail V5 e trava lucros\n\n"
                       "🥇 POL/SOL/XRP/ETH/BNB/ADA/DOGE — TODOS AUTOMÁTICOS\n"
                       "(sem necessidade de /go[coin] — entra sozinho ao sinal)\n\n"
                       f"CB -{CIRCUIT_BREAKER_PCT:.0f}%  |  Step Trail V5  |  Lev actual: <b>{LEVERAGE}×</b>  |  cd 30min", chat_id)

        except Exception as e:
            log.warning("tg_polling: %s", e)
            time.sleep(5)

# ══════════════════════════════════════════════════════════════════════════════
# CONFIRMAÇÃO MANUAL — /go[coin]
# ══════════════════════════════════════════════════════════════════════════════

def _queue_signal(inst_id: str, sig: str, signal_name: str, tag: str,
                  dir_scout: str, extra_info: str = "") -> None:
    """Guarda sinal pendente 120s e envia alerta de confirmação ao Telegram."""
    coin = inst_id.replace("-USDT-SWAP", "")
    go_cmd = f"/go{coin.lower()}"
    with _pending_lock:
        _pending_signals[inst_id] = (inst_id, sig, signal_name, tag, time.time() + 120)
    tg(f"⚡ <b>SINAL DETECTADO — {coin}</b>\n"
       f"Estratégia: <b>{signal_name}</b> | Direção: <b>{dir_scout}</b>\n"
       f"{extra_info}"
       f"⏳ Confirma em <b>120s</b> com <code>{go_cmd}</code>\n"
       f"Sem resposta → sinal descartado.")
    log.info("⏳ [%s] sinal pendente — aguarda %s (120s)", coin, go_cmd)


# ══════════════════════════════════════════════════════════════════════════════
# LOOP PRINCIPAL — ONE TARGET ONE KILL
# ══════════════════════════════════════════════════════════════════════════════

def duo_elite_loop() -> None:
    global _duo_in_trade, _duo_cooldown_until, _panic_until
    log.info("🎯 V9 COMMANDER SUITE — FULL SQUAD + FVG EXPANSION READY — TODOS AUTOFIRE")
    tg("🏆 <b>V9 FULL SQUAD — TODOS OS PARES AUTOMÁTICOS</b>\n\n"
       "🥇 <b>POL</b> — ICHIMOKU 1H (97.4% hit)\n"
       "🌊 <b>SOL</b> — SUPERTREND + FVG 15m\n"
       "🎯 <b>XRP</b> — RSI DIV + VWAP + OB 1H\n"
       "💧 <b>ETH</b> — VWAP KISS + FVG 15m\n"
       "🔷 <b>BNB</b> — FVG 15m (65.2% hit)\n"
       "🛡️ <b>ADA</b> — ORDER BLOCK 1H\n"
       "🎲 <b>DOGE</b> — ORDER BLOCK 1H\n\n"
       "⚡ <b>TODOS entram automático</b> — sem /go[coin] obrigatório\n"
       "(O /go[coin] ainda existe para confirmar manualmente se quiseres)\n\n"
       f"🔒 Step Trail V5  |  CB -{CIRCUIT_BREAKER_PCT:.0f}%  |  HOLD {HOLD_SL_PCT:.0f}%  |  STRICT {STRICT_SL_PCT:.1f}%  |  "
       f"{LEVERAGE}× ALL-IN  |  cd 30min\n"
       "✅ <b>10 ESTRATÉGIAS ATIVAS. SNIPER MODE ON.</b>")

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
                        tg(f"🌊 <b>SOL SUPERTREND FIRED</b>\n"
                           f"Par: <code>SOL-USDT-SWAP</code> | {dir_scout} | Hit: <b>95.0%</b> | HOLD\n"
                           f"⚡ Entrando automaticamente...")
                        fired = _fire(DUO_SOL, sig, "SUPERTREND M15", tag="🌊 TREND SURF")
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
                        tg(f"🎯 <b>XRP RSI DIV FIRED</b>\n"
                           f"Par: <code>XRP-USDT-SWAP</code> | {dir_scout} | PF: <b>2.38</b> | STRICT 1.5%\n"
                           f"⚡ Entrando automaticamente...")
                        fired = _fire(SHIELD_XRP, sig, "RSI DIV+VWAP M15", tag="🎯 SAFETY SNIPER")
                    else:
                        log.info("[XRP/RSI] sem divergência")
                except Exception as e:
                    log.error("[XRP/RSI] %s", e)
            # ╚═══════════════════════════════════════════════════════════════╝

            # ── 4: ETH — VWAP KISS ────────────────────────────────────────────
            if not fired:
                try:
                    sig = vwap_kiss_signal(okx_candles(DUO_ETH))
                    if sig:
                        dir_scout = "📈 LONG" if sig == "buy" else "📉 SHORT"
                        log.info("💧 VWAP KISS ETH → %s", sig.upper())
                        tg(f"💧 <b>ETH VWAP KISS FIRED</b>\n"
                           f"Par: <code>ETH-USDT-SWAP</code> | {dir_scout} | HOLD\n"
                           f"⚡ Entrando automaticamente...")
                        fired = _fire(DUO_ETH, sig, "VWAP KISS M15", tag="DUO ELITE")
                    else:
                        log.info("[ETH] sem sinal")
                except Exception as e:
                    log.error("[ETH] %s", e)

            # ── 5: SOL — ENGOLFO ──────────────────────────────────────────────
            if not fired:
                try:
                    sig = engolfo_signal(okx_candles(DUO_SOL))
                    if sig:
                        dir_scout = "📈 LONG" if sig == "buy" else "📉 SHORT"
                        log.info("🔥 ENGOLFO SOL → %s", sig.upper())
                        tg(f"🔥 <b>SOL ENGOLFO FIRED</b>\n"
                           f"Par: <code>SOL-USDT-SWAP</code> | {dir_scout} | HOLD\n"
                           f"⚡ Entrando automaticamente...")
                        fired = _fire(DUO_SOL, sig, "ENGOLFO M15", tag="DUO ELITE")
                    else:
                        log.info("[SOL] sem sinal")
                except Exception as e:
                    log.error("[SOL] %s", e)

            # ── 6: ADA — ORDER BLOCK DEFENSE ──────────────────────────────────
            if not fired:
                try:
                    sig = order_block_signal(okx_candles(SHIELD_ADA, bar="1H", limit=100))
                    if sig:
                        dir_scout = "📈 LONG" if sig == "buy" else "📉 SHORT"
                        log.info("🛡️ ORDER BLOCK ADA → %s", sig.upper())
                        tg(f"🛡️ <b>ADA ORDER BLOCK FIRED</b>\n"
                           f"Par: <code>ADA-USDT-SWAP</code> | {dir_scout} | STRICT 1.5%\n"
                           f"⚡ Entrando automaticamente...")
                        fired = _fire(SHIELD_ADA, sig, "ORDER BLOCK 1H", tag="SHIELD 🛡️")
                    else:
                        log.info("[ADA] sem bloco")
                except Exception as e:
                    log.error("[ADA] %s", e)

            # ── 7: XRP — ORDER BLOCK DEFENSE ──────────────────────────────────
            if not fired:
                try:
                    sig = order_block_signal(okx_candles(SHIELD_XRP, bar="1H", limit=100))
                    if sig:
                        dir_scout = "📈 LONG" if sig == "buy" else "📉 SHORT"
                        log.info("🛡️ ORDER BLOCK XRP → %s", sig.upper())
                        tg(f"🛡️ <b>XRP ORDER BLOCK FIRED</b>\n"
                           f"Par: <code>XRP-USDT-SWAP</code> | {dir_scout} | STRICT 1.5%\n"
                           f"⚡ Entrando automaticamente...")
                        fired = _fire(SHIELD_XRP, sig, "ORDER BLOCK 1H", tag="SHIELD 🛡️")
                    else:
                        log.info("[XRP/OB] sem bloco")
                except Exception as e:
                    log.error("[XRP/OB] %s", e)

            # ── 7b: DOGE — ORDER BLOCK DEFENSE (STRICT 1.5%) ─────────────────
            if not fired:
                try:
                    sig = order_block_signal(okx_candles(GOLD_DOGE, bar="1H", limit=100))
                    if sig:
                        dir_scout = "📈 LONG" if sig == "buy" else "📉 SHORT"
                        log.info("🎲 ORDER BLOCK DOGE → %s", sig.upper())
                        tg(f"🎲 <b>DOGE ORDER BLOCK FIRED</b>\n"
                           f"Par: <code>DOGE-USDT-SWAP</code> | {dir_scout} | STRICT 1.5%\n"
                           f"⚡ Entrando automaticamente...")
                        fired = _fire(GOLD_DOGE, sig, "ORDER BLOCK 1H", tag="🎲 DOGE SHIELD")
                    else:
                        log.info("[DOGE/OB] sem bloco")
                except Exception as e:
                    log.error("[DOGE/OB] %s", e)

            # ╔══════════════ FVG EXPANSION SQUAD (V9) ═══════════════════════╗
            # ── 8: SOL — FAIR VALUE GAP 15m (70.6% hit / ROI +70.4%) ────────
            if not fired:
                try:
                    sig = fvg_signal(okx_candles(DUO_SOL), DUO_SOL)
                    if sig:
                        dir_scout = "📈 LONG" if sig == "buy" else "📉 SHORT"
                        log.info("🔷 FVG SOL → %s", sig.upper())
                        tg(f"🔷 <b>SOL FAIR VALUE GAP</b>\n"
                           f"Par: <code>SOL-USDT-SWAP</code> | {dir_scout} | HOLD\n"
                           f"Retorno ao midpoint do gap | 70.6% hit | ROI +70.4%\n"
                           f"⚡ Entrando automaticamente...")
                        fired = _fire(DUO_SOL, sig, "FVG SOL 15m", tag="🔷 FVG SOL")
                    else:
                        log.info("[SOL/FVG] sem retorno ao gap")
                except Exception as e:
                    log.error("[SOL/FVG] %s", e)

            # ── 9: BNB — FAIR VALUE GAP 15m (65.2% hit / ROI +48.8%) ────────
            if not fired:
                try:
                    sig = fvg_signal(okx_candles(FVG_BNB), FVG_BNB)
                    if sig:
                        dir_scout = "📈 LONG" if sig == "buy" else "📉 SHORT"
                        log.info("🔷 FVG BNB → %s", sig.upper())
                        tg(f"🔷 <b>BNB FAIR VALUE GAP</b>\n"
                           f"Par: <code>BNB-USDT-SWAP</code> | {dir_scout} | HOLD\n"
                           f"Retorno ao midpoint do gap | 65.2% hit | ROI +48.8%\n"
                           f"⚡ Entrando automaticamente...")
                        fired = _fire(FVG_BNB, sig, "FVG BNB 15m", tag="🔷 FVG BNB")
                    else:
                        log.info("[BNB/FVG] sem retorno ao gap")
                except Exception as e:
                    log.error("[BNB/FVG] %s", e)

            # ── 10: ETH — FAIR VALUE GAP 15m (73.9% hit / ROI +34.9%) ───────
            if not fired:
                try:
                    sig = fvg_signal(okx_candles(DUO_ETH), DUO_ETH)
                    if sig:
                        dir_scout = "📈 LONG" if sig == "buy" else "📉 SHORT"
                        log.info("🔷 FVG ETH → %s", sig.upper())
                        tg(f"🔷 <b>ETH FAIR VALUE GAP</b>\n"
                           f"Par: <code>ETH-USDT-SWAP</code> | {dir_scout} | HOLD\n"
                           f"Retorno ao midpoint do gap | 73.9% hit | ROI +34.9%\n"
                           f"⚡ Entrando automaticamente...")
                        fired = _fire(DUO_ETH, sig, "FVG ETH 15m", tag="🔷 FVG ETH")
                    else:
                        log.info("[ETH/FVG] sem retorno ao gap")
                except Exception as e:
                    log.error("[ETH/FVG] %s", e)
            # ╚═══════════════════════════════════════════════════════════════╝

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
    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║   TradeSniper V9 COMMANDER — FULL SQUAD + FVG       ║")
    log.info("║   🥇 POL  [ICHIMOKU 1H]       AUTOFIRE  97.4%% hit  ║")
    log.info("║   🌊 SOL  [SUPERTREND+FVG 15m] AUTOFIRE  95.0%% hit  ║")
    log.info("║   🎯 XRP  [RSI DIV+VWAP 15m]  AUTOFIRE  PF 2.38    ║")
    log.info("║   💧 ETH  [VWAP KISS+FVG 15m] AUTOFIRE              ║")
    log.info("║   🔷 BNB  [FVG 15m]           AUTOFIRE  65.2%% hit  ║")
    log.info("║   🛡️ ADA  [ORDER BLOCK 1H]    AUTOFIRE              ║")
    log.info("║   🎲 DOGE [ORDER BLOCK 1H]    AUTOFIRE  STRICT 1.5%  ║")
    log.info("║   ⚡ TODOS 7 PARES AUTOMÁTICOS — sem /go obrigatório  ║")
    log.info("║   SL: HOLD %.0f%% | STRICT %.1f%% | CB -%.0f%%          ║",
             HOLD_SL_PCT, STRICT_SL_PCT, CIRCUIT_BREAKER_PCT)
    log.info("║   🔒 STEP TRAIL V5 ATIVO  |  %dx  |  cd 30min       ║", LEVERAGE)
    log.info("║   FVG: gaps activos em memória | expiry %d velas    ║", FVG_GAP_EXPIRY)
    log.info("╚══════════════════════════════════════════════════════╝")

    # Estado persistido
    with _auth_lock:
        _bot_authorized = _load_state()
    log.info("Estado: %s", "AUTORIZADO ✅" if _bot_authorized else "PAUSADO ⛔")

    # Leverage — inclui BNB (V9)
    for sym in ALL_SYMS:
        okx_set_leverage(sym)

    # Health server — porta para Autoscale (responde 200 OK, bot não é afetado)
    threading.Thread(target=_start_health_server, daemon=True, name="health").start()

    # Threads de suporte
    threading.Thread(target=report_loop,            daemon=True, name="report").start()
    threading.Thread(target=telegram_commands_loop, daemon=True, name="tg").start()

    # Loop principal — bloqueia para sempre
    duo_elite_loop()
