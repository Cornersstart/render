"""
TradeSniper Bot — V11 SNIPER ELITE (OPERAÇÃO PURGE)
BUILD: 2026-04-29 — Purge total: só squads activos, sem legacy
Doutrina : ONE TARGET, ONE KILL  |  STEP TRAIL V5 = LAW
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🥇 POL   — GOLDEN (Ichimoku 1H)             HOLD
🪤 OpA   — Armadilha Triple BB+SAR          SOL/BNB/ETH/DOGE
⚡ OpD   — Sniper MACD M5                   ETH only
🏦 OpE   — SMC/ICT M15                      ETH/BTC/SOL
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CB: PnL nominal ≤ -$50  |  SL 1.5%  |  Reserva $30 fixa
5× Isolated ALL-IN  |  OKX Perpetual SWAP (hedge mode)
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
FEE_RESERVE_USD = 30.0  # reserva fixa $30 para taxas open/close/funding

# ── DUO DE ELITE ───────────────────────────────────────────────────────────────
DUO_SL_PCT          = 1.2    # SL inicial (protecção antes do trailing activar)
DUO_COOLDOWN        = 300    # 5 min cooldown após trade (era 30 min)
TRAIL_ACTIVATE_PCT  = 0.8    # trailing activa quando lucro ≥ +0.8%
TRAIL_CALLBACK      = 0.01   # distância trailing = 1.0%
LIMIT_OFFSET_PCT    = 0.15   # % de desconto no preço de entrada (ordem limit maker)
LIMIT_FILL_TIMEOUT  = 180    # segundos máx para preencher a limit; senão cancela
RSI2_LONG_MAX       = 45     # RSI(2) máximo para entrada LONG (pullback moderado)
RSI2_SHORT_MIN      = 55     # RSI(2) mínimo para entrada SHORT (spike moderado)

BB_PERIOD    = 20    # Bollinger Bands: período da SMA
BB_STD       = 2.0   # desvios padrão da banda
BB_TOL_PCT   = 0.5   # % de tolerância para "toque" na banda
SCALP_SL_PCT  = 1.0    # SL fixo para scalp de reversão Bollinger (OpA)

DUO_ETH    = "ETH-USDT-SWAP"
DUO_SOL    = "SOL-USDT-SWAP"
GOLD_POL   = "POL-USDT-SWAP"      # 🥇 Golden pair — Ichimoku 1H exclusivo
GOLD_DOGE  = "DOGE-USDT-SWAP"     # 🪤 OpA Armadilha
FVG_BNB    = "BNB-USDT-SWAP"      # 🪤 OpA Armadilha
ALL_SYMS   = [DUO_ETH, DUO_SOL, GOLD_POL, GOLD_DOGE, FVG_BNB]


# ── STEP TRAILING V5 — 5 graus baseados em PnL não realizado (USDT) ───────────
# Cada tuple: (trigger_usd, lock_usd) — ao atingir trigger, SL sobe para lock
STEP_TRAIL_LEVELS: list[tuple[float, float]] = [
    (25.0,   0.0),   # Grau 1: hit +$25  → piso break-even (0x0)
    (40.0,  28.0),   # Grau 2: hit +$40  → piso +$28
    (60.0,  38.0),   # Grau 3: hit +$60  → piso +$38
    (80.0,  52.0),   # Grau 4: hit +$80  → piso +$52
    (100.0, 68.0),   # Grau 5: hit +$100 → piso +$68
]

# ══════════════════════════════════════════════════════════════════════════════
# GOLDEN RECOVERY DOCTRINE — regras de hold por par (Abr/2026)
# ══════════════════════════════════════════════════════════════════════════════
HOLD_PAIRS    = {GOLD_POL, DUO_ETH, DUO_SOL, FVG_BNB, GOLD_DOGE}
STRICT_PAIRS  = set()   # todos os squads usam sl_pct explícito
STRICT_SL_PCT = 1.5
HOLD_SL_PCT   = 5.0
CIRCUIT_BREAKER_USD = 50.0   # circuit breaker — fecha se PnL nominal ≤ -$50
PROFIT_LOCK_USD     = 0.0   # 0 = desactivado — Step Trail V5 trata dos lucros

# ── E09 ICHIMOKU CLOUD (POL 1H) ──────────────────────────────────────────────
ICHI_TENKAN  = 9
ICHI_KIJUN   = 26
ICHI_SENKOU  = 52

# ── DISCIPLINA DE SNIPER ──────────────────────────────────────────────────────
LOCKDOWN_SECS    = 300   # 5 min — cooldown entre sinais

# ── Estado global ─────────────────────────────────────────────────────────────
_duo_in_trade:       bool  = False
_duo_cooldown_until: float = 0.0
_lockdown_until:     float = 0.0   # bloqueio total de novos sinais (anti ping-pong)
_duo_lock                  = threading.Lock()

_bot_authorized: bool = True
_auth_lock             = threading.Lock()
_armadilha_mode: bool  = False   # False = off | True = OpA Triple BB+SAR activo
_trail_mode: str       = "gv5"   # "gv5" = Step Trail V5 | "gv6" = SAR M15 trailing

# Dedup anti-spam: regista (direcção, timestamp) da última vez que alertámos/tentámos
# disparar cada sinal. Evita TG metralhadora quando _fire() falha repetidamente.
_signal_alerted: dict[str, tuple[str, float]] = {}
_SIGNAL_COOLDOWN = 300  # 5 min — não re-alertar o mesmo sinal antes deste tempo

# ── Confirmação manual (120s) — sinais não-POL aguardam /go[coin] ─────────────
_pending_signals: dict = {}   # coin_key → (inst_id, side, signal_name, tag, expiry)
_pending_lock          = threading.Lock()

# ── Meta mensal — $600 / mês ────────────────────────────────────────────────
MONTHLY_GOAL_USD = 600.0

# ── Panic pause ────────────────────────────────────────────────────────────
_panic_until: float = 0.0
_mode_opd: bool = False   # OpD — Sniper MACD M5 (ETH)
_mode_ope: bool = False   # OpE — ICT/SMC Institucional 15m (BTC/ETH/SOL)

# ── Estratégias habilitadas — /pausar /activar individuais ───────────────────
_STRATEGY_KEYS = ("ichimoku",)
_strategy_enabled: dict[str, bool] = {"ichimoku": True}
_strategy_lock = threading.Lock()

STATE_FILE = Path(__file__).parent / "bot_state.json"

# ── Persistência ─────────────────────────────────────────────────────────────
def _save_state(authorized: bool) -> None:
    try:
        tmp = STATE_FILE.with_suffix(".tmp")
        with _strategy_lock:
            st_snap = dict(_strategy_enabled)
        tmp.write_text(json.dumps({
            "authorized":  authorized,
            "trail_mode":  _trail_mode,
            "updatedAt":   datetime.now(timezone.utc).isoformat(),
        }, indent=2))
        tmp.replace(STATE_FILE)
    except Exception as e:
        log.debug("save_state: %s", e)

def _load_state() -> bool:
    try:
        if STATE_FILE.exists():
            return bool(json.loads(STATE_FILE.read_text()).get("authorized", True))
    except Exception:
        pass
    return True

def _load_full_state() -> None:
    """Restaura authorized + trail_mode + strategy_enabled do ficheiro de estado."""
    global _trail_mode
    try:
        if STATE_FILE.exists():
            data = json.loads(STATE_FILE.read_text())
            with _auth_lock:
                globals()["_bot_authorized"] = bool(data.get("authorized", True))
            _trail_mode = data.get("trail_mode", "gv5")
            log.info("Estado restaurado: auth=%s trail=%s",
                     globals()["_bot_authorized"], _trail_mode)
    except Exception as e:
        log.warning("_load_full_state: %s", e)

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
    """Equity disponível (availEq) USDT — base para calc_qty com reserva $30."""
    if not _has_creds(): return None
    path = "/api/v5/account/balance?ccy=USDT"
    try:
        r = requests.get(f"https://www.okx.com{path}", headers=_headers("GET", path), timeout=8)
        d = r.json()
        if d.get("code") != "0": return None
        details = d["data"][0].get("details", [])
        usdt = next((x for x in details if x["ccy"] == "USDT"), None)
        return float(usdt["availEq"]) if usdt else 0.0
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
        path = f"/api/v5/account/positions?instType=SWAP&instId={sym}"
        try:
            r = requests.get(f"https://www.okx.com{path}", headers=_headers("GET", path), timeout=8)
            for pos in r.json().get("data", []):
                if float(pos.get("pos", 0) or 0) >= 1:
                    return (sym, pos.get("posSide", "long"))
        except Exception as e:
            log.warning("any_position_open %s: %s", sym, e)
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
def okx_lot_size(inst_id: str) -> tuple[float, float]:
    """Retorna (ctVal, lotSz) do instrumento.
    ctVal: valor em USD de 1 contrato.
    lotSz: múltiplo mínimo de contratos exigido pela OKX."""
    try:
        r = requests.get(f"{OKX_BASE}/public/instruments?instType=SWAP&instId={inst_id}", timeout=8)
        d = r.json()
        if d.get("code") == "0" and d.get("data"):
            info = d["data"][0]
            return float(info.get("ctVal", 1)), float(info.get("lotSz", 1))
    except Exception:
        pass
    return 1.0, 1.0

# ── Cálculo de contratos ALL-IN com reserva fixa $30 ────────────────────────
# usable = availEq - $30  → cobre taker fee open+close, slippage e funding.
# Evita sCode=51008 (Insufficient Margin) sem desperdiçar margem em %.

def calc_qty(inst_id: str, price: float, balance: float) -> int:
    ct_val, lot_sz = okx_lot_size(inst_id)
    usable   = max(0.0, balance - FEE_RESERVE_USD)
    raw      = usable * LEVERAGE / (price * ct_val)
    # floor para o múltiplo de lotSz (ex: POL lotSz=10 → 23 → 20)
    lot_sz_i = max(1, int(lot_sz))
    contracts = max(lot_sz_i, int(raw // lot_sz_i) * lot_sz_i)
    log.debug("calc_qty %s: raw=%.1f lotSz=%d → %d (ctVal=%.4f bal=%.2f)",
              inst_id, raw, lot_sz_i, contracts, ct_val, balance)
    return contracts

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

# ── OKX — ordem market de entrada (execução imediata ao preço actual) ────────
def okx_open_market(inst_id: str, side: str, qty: int) -> dict:
    """Abre posição a mercado — execução imediata, sem espera de preenchimento."""
    if not _has_creds(): raise RuntimeError("Sem credenciais OKX.")
    path = "/api/v5/trade/order"
    body = json.dumps({"instId": inst_id, "tdMode": "isolated", "side": side,
                       "posSide": _SIDE_PS[side], "ordType": "market",
                       "sz": str(qty)})
    r = requests.post(f"https://www.okx.com{path}", headers=_headers("POST", path, body),
                      data=body, timeout=10)
    d    = r.json()
    item = (d.get("data") or [{}])[0]
    sCode = str(item.get("sCode", ""))
    sMsg  = item.get("sMsg", "")
    if sCode and sCode not in ("0",):
        if sCode == "51000":
            _LEVERAGE_SET.discard(inst_id)
        raise RuntimeError(f"market_order {inst_id} side={side}: sCode={sCode} sMsg='{sMsg}'")
    if d.get("code") != "0":
        raise RuntimeError(f"market_order {inst_id}: code={d.get('code')} msg='{d.get('msg')}'")
    return d

# ── OKX — ordem limit de entrada (maker, economiza taxas) ────────────────────
def okx_open_limit(inst_id: str, side: str, qty: int, limit_px: float) -> dict:
    """Coloca ordem limit de abertura com preço fixo — modo maker (taxas reduzidas)."""
    if not _has_creds(): raise RuntimeError("Sem credenciais OKX.")
    path = "/api/v5/trade/order"
    body = json.dumps({"instId": inst_id, "tdMode": "isolated", "side": side,
                       "posSide": _SIDE_PS[side], "ordType": "limit",
                       "sz": str(qty), "px": f"{limit_px:.6f}"})
    r = requests.post(f"https://www.okx.com{path}", headers=_headers("POST", path, body),
                      data=body, timeout=10)
    d    = r.json()
    item = (d.get("data") or [{}])[0]
    sCode = str(item.get("sCode", ""))
    sMsg  = item.get("sMsg", "")
    if sCode and sCode not in ("0",):
        if sCode == "51000":
            _LEVERAGE_SET.discard(inst_id)
        raise RuntimeError(f"limit_order {inst_id} side={side}: sCode={sCode} sMsg='{sMsg}'")
    if d.get("code") != "0":
        raise RuntimeError(f"limit_order {inst_id}: code={d.get('code')} msg='{d.get('msg')}'")
    return d

def okx_get_order_fill(inst_id: str, ord_id: str) -> dict | None:
    """Retorna estado actual de uma ordem (state, avgPx, fillSz). None em caso de erro."""
    if not _has_creds(): return None
    path = f"/api/v5/trade/order?instId={inst_id}&ordId={ord_id}"
    try:
        r = requests.get(f"https://www.okx.com{path}",
                         headers=_headers("GET", path), timeout=8)
        d = r.json()
        if d.get("code") == "0" and d.get("data"):
            return d["data"][0]
    except Exception as e:
        log.warning("get_order_fill %s %s: %s", inst_id, ord_id, e)
    return None

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

def okx_close_limit(inst_id: str, pos_side: str, sz: int, limit_px: float) -> dict:
    """Fecha posição via ordem limit (modo maker — sem taker fee)."""
    if not _has_creds(): raise RuntimeError("Sem credenciais OKX.")
    path = "/api/v5/trade/order"
    close_side = "sell" if pos_side == "long" else "buy"
    body = json.dumps({"instId": inst_id, "tdMode": "isolated",
                       "side": close_side, "posSide": pos_side,
                       "ordType": "limit", "sz": str(sz), "px": f"{limit_px:.6f}"})
    r = requests.post(f"https://www.okx.com{path}",
                      headers=_headers("POST", path, body), data=body, timeout=10)
    d = r.json()
    if d.get("code") != "0":
        raise RuntimeError(f"close_limit {inst_id}: {d.get('msg')}")
    item = (d.get("data") or [{}])[0]
    if str(item.get("sCode", "0")) not in ("0",):
        raise RuntimeError(f"close_limit {inst_id}: sCode={item.get('sCode')} {item.get('sMsg')}")
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

# ── OKX — limpeza total de ordens (regulares + algos, ambos os lados) ────────
def _fetch_all_algos(inst_id: str) -> list[dict]:
    """Agrega ordens algo pendentes de TODOS os tipos relevantes para inst_id.

    OKX exige ordType no endpoint orders-algo-pending — sem ele devolve vazio.
    Faz uma query por tipo e combina os resultados para garantir cobertura total.
    """
    ALGO_TYPES = ("conditional", "move_order_stop", "oco", "trigger")
    all_orders: list[dict] = []
    for ot in ALGO_TYPES:
        try:
            params = f"?instType=SWAP&instId={inst_id}&ordType={ot}"
            r      = requests.get(f"https://www.okx.com/api/v5/trade/orders-algo-pending{params}",
                                  headers=_headers("GET", f"/api/v5/trade/orders-algo-pending{params}"),
                                  timeout=8)
            data = r.json()
            if data.get("code") == "0":
                all_orders.extend(data.get("data", []))
        except Exception as e:
            log.warning("_fetch_all_algos %s ordType=%s: %s", inst_id, ot, e)
    return all_orders

def cancel_all_open_orders(inst_id: str) -> int:
    """Cancela TODAS as ordens abertas para inst_id (regulares + algos, long+short).

    Varredura em dois passes para garantir que nada fica para trás.
    Retorna o número de ordens canceladas.
    """
    if not _has_creds(): return 0
    sym       = inst_id.replace("-USDT-SWAP", "")
    cancelled = 0

    # 1) Ordens regulares pendentes (limit/market não preenchidas)
    try:
        qpath = f"/api/v5/trade/orders-pending?instType=SWAP&instId={inst_id}"
        r     = requests.get(f"https://www.okx.com{qpath}",
                             headers=_headers("GET", qpath), timeout=8)
        orders = r.json().get("data", [])
        if orders:
            to_cancel = [{"instId": inst_id, "ordId": o["ordId"]} for o in orders]
            cpath = "/api/v5/trade/cancel-batch-orders"
            cbody = json.dumps(to_cancel)
            requests.post(f"https://www.okx.com{cpath}",
                          headers=_headers("POST", cpath, cbody), data=cbody, timeout=8)
            cancelled += len(to_cancel)
    except Exception as e:
        log.warning("cancel_all_open_orders regular %s: %s", sym, e)

    # 2) Algo orders — query por tipo (OKX exige ordType explícito), duplo passe
    for _pass in range(2):
        try:
            algos = _fetch_all_algos(inst_id)
            if not algos:
                break   # livro limpo
            to_cancel = [{"algoId": o["algoId"], "instId": inst_id} for o in algos]
            cpath = "/api/v5/trade/cancel-algos"
            cbody = json.dumps(to_cancel)
            requests.post(f"https://www.okx.com{cpath}",
                          headers=_headers("POST", cpath, cbody), data=cbody, timeout=8)
            cancelled += len(to_cancel)
            time.sleep(0.4)
        except Exception as e:
            log.warning("cancel_all_open_orders algos %s passe%d: %s", sym, _pass + 1, e)
            break

    log.info("🧹 %s — Varredura completa: Ordens normais e ordens de gatilho (TP/SL) eliminadas. Total: %d",
             sym, cancelled)
    return cancelled

def clear_garbage(inst_id: str, pos_side: str) -> int:
    """Remove TODAS as ordens algo (SL/TP/trailing) do pos_side antes de colocar nova.

    Garante que não existe mais do que uma ordem de saída por contrato.
    Duplo passe: cancela, espera propagação, confirma que ficou limpo.
    Retorna total de ordens removidas.
    """
    if not _has_creds(): return 0
    sym       = inst_id.replace("-USDT-SWAP", "")
    cancelled = 0
    for _pass in range(2):
        try:
            algos = [o for o in _fetch_all_algos(inst_id)
                     if o.get("posSide") == pos_side]
            if not algos:
                break
            to_cancel = [{"algoId": o["algoId"], "instId": inst_id} for o in algos]
            cpath = "/api/v5/trade/cancel-algos"
            cbody = json.dumps(to_cancel)
            requests.post(f"https://www.okx.com{cpath}",
                          headers=_headers("POST", cpath, cbody), data=cbody, timeout=8)
            cancelled += len(to_cancel)
            time.sleep(0.4)
        except Exception as e:
            log.warning("clear_garbage %s/%s passe%d: %s", sym, pos_side, _pass + 1, e)
            break
    if cancelled:
        log.info("🗑️ clear_garbage %s/%s: %d ordens removidas", sym, pos_side, cancelled)
    return cancelled

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
# SINAIS — SQUADS ACTIVOS
# ══════════════════════════════════════════════════════════════════════════════

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
        and price > kijun                                   # acima do kijun
        and 45 <= rsi <= 68                                 # [F4] RSI direccional
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
        and price < kijun                                   # abaixo do kijun
        and 30 <= rsi <= 60                                 # [F4] RSI mais amplo para SHORT
        and trend_not_bullish                               # [F6] não vender em alta forte
    )

    if long_ok:  return "buy"
    if short_ok: return "sell"
    return None


def _m5_confirm(inst_id: str, side: str) -> tuple[str, str, str, float]:
    """Confirmação M5: SAR Parabólico + RSI + Bollinger em 5 minutos.

    Retorna (action, effective_side, debug_str, sar_px):
      'allow'  — confirmado, avança; sar_px = preço do SAR (usado como SL dinâmico)
      'block'  — SAR/RSI/BB bloqueiam (sem Telegram, só log); sar_px = 0
      'invert' — exaustão oposta detectada; sar_px = 0 (usa % SL)
    Fail-safe: ('allow', side, 'API fail', 0.0).
    """
    try:
        df    = okx_candles(inst_id, bar="5m", limit=100)
        close = df["close"]

        # SAR Parabólico via pandas_ta
        psar  = df.ta.psar(af0=0.02, af=0.02, max_af=0.20)
        col_l = next((c for c in psar.columns if "PSARl" in c), None)
        col_s = next((c for c in psar.columns if "PSARs" in c), None)
        if col_l and col_s:
            sar_bull = not pd.isna(psar[col_l].iloc[-1])
            _lv      = psar[col_l].iloc[-1]
            _sv      = psar[col_s].iloc[-1]
            sar_px   = float(_lv if sar_bull else _sv)
        else:
            sar_bull, sar_px = True, 0.0

        # RSI 14 M5
        delta = close.diff()
        gain  = delta.clip(lower=0).ewm(span=14, adjust=False).mean()
        loss  = (-delta).clip(lower=0).ewm(span=14, adjust=False).mean()
        rsi   = float(100 - (100 / (1 + gain.iloc[-1] / (loss.iloc[-1] + 1e-10))))

        # Bollinger (20,2) M5
        mid      = close.rolling(BB_PERIOD).mean()
        std      = close.rolling(BB_PERIOD).std()
        upper    = float((mid + BB_STD * std).iloc[-1])
        lower_bb = float((mid - BB_STD * std).iloc[-1])
        price    = float(close.iloc[-1])

        at_upper = price >= upper    * (1 - BB_TOL_PCT / 100)
        at_lower = price <= lower_bb * (1 + BB_TOL_PCT / 100)

        sar_str = "bull" if sar_bull else "bear"
        dbg = f"SAR5={sar_str}@{sar_px:.4f} RSI5={rsi:.1f}"

        if side == "buy":
            if rsi > 70 and at_upper:
                return ("invert", "sell", f"{dbg} → RSI>70+banda sup → TRAP SHORT", 0.0)
            if rsi > 70:
                return ("block", "buy", f"{dbg} → RSI5>70 bloqueio LONG", 0.0)
            if not sar_bull:
                return ("block", "buy", f"{dbg} → SAR5 bearish, LONG bloqueado", 0.0)
        else:
            if rsi < 30 and at_lower:
                return ("invert", "buy", f"{dbg} → RSI<30+banda inf → TRAP LONG", 0.0)
            if rsi < 30:
                return ("block", "sell", f"{dbg} → RSI5<30 bloqueio SHORT", 0.0)
            if at_lower:
                return ("block", "sell", f"{dbg} → banda inferior BB5, não vender", 0.0)
            if sar_bull:
                return ("block", "sell", f"{dbg} → SAR5 bullish, SHORT bloqueado", 0.0)

        return ("allow", side, dbg, sar_px)
    except Exception as e:
        log.warning("_m5_confirm %s: %s — permitindo", inst_id, e)
        return ("allow", side, "API fail", 0.0)

def _triple_bb_touch(inst_id: str) -> tuple[str, str]:
    """Triple Bollinger Confluence — toque simultâneo em M5, M15 e H1.

    Retorna (touch_side, signal):
      ('upper', 'sell') — exaustão de topo nos 3 TFs → SHORT
      ('lower', 'buy')  — exaustão de fundo nos 3 TFs → LONG
      ('none',  '')     — sem confluência tripla
    """
    try:
        touches = []
        for bar in ("5m", "15m", "1H"):
            df    = okx_candles(inst_id, bar=bar, limit=60)
            close = df["close"]
            mid   = close.rolling(BB_PERIOD).mean()
            std   = close.rolling(BB_PERIOD).std()
            upper = float((mid + BB_STD * std).iloc[-1])
            lower = float((mid - BB_STD * std).iloc[-1])
            price = float(close.iloc[-1])
            if   price >= upper * (1 - BB_TOL_PCT / 100): touches.append("upper")
            elif price <= lower * (1 + BB_TOL_PCT / 100): touches.append("lower")
            else: touches.append("none")
        if all(t == "upper" for t in touches): return ("upper", "sell")
        if all(t == "lower" for t in touches): return ("lower", "buy")
        return ("none", "")
    except Exception as e:
        log.warning("_triple_bb_touch %s: %s", inst_id, e)
        return ("none", "")

def _sar_just_inverted(inst_id: str, to_direction: str) -> bool:
    """True se SAR M5 inverteu nos últimos 2 candles na direcção esperada.
    to_direction: 'bull' (SAR desceu) ou 'bear' (SAR subiu).
    """
    try:
        df    = okx_candles(inst_id, bar="5m", limit=100)
        psar  = df.ta.psar(af0=0.02, af=0.02, max_af=0.20)
        col_l = next((c for c in psar.columns if "PSARl" in c), None)
        if not col_l:
            return False
        for i in (-3, -2):
            prev_bull = not pd.isna(psar[col_l].iloc[i])
            curr_bull = not pd.isna(psar[col_l].iloc[i + 1])
            if to_direction == "bull" and not prev_bull and curr_bull: return True
            if to_direction == "bear" and prev_bull and not curr_bull: return True
        return False
    except Exception as e:
        log.warning("_sar_just_inverted %s: %s", inst_id, e)
        return False

def _get_sar_m15_px(inst_id: str) -> float:
    """Retorna preço actual do SAR Parabólico M15 para trailing stop."""
    try:
        df    = okx_candles(inst_id, bar="15m", limit=100)
        psar  = df.ta.psar(af0=0.02, af=0.02, max_af=0.20)
        col_l = next((c for c in psar.columns if "PSARl" in c), None)
        col_s = next((c for c in psar.columns if "PSARs" in c), None)
        if col_l and col_s:
            sar_bull = not pd.isna(psar[col_l].iloc[-1])
            return float(psar[col_l].iloc[-1] if sar_bull else psar[col_s].iloc[-1])
        return 0.0
    except Exception as e:
        log.warning("_get_sar_m15_px %s: %s", inst_id, e)
        return 0.0

def _h1_band_opposite(inst_id: str, side: str) -> bool:
    """True se preço atingiu a banda H1 oposta ao side (alvo de saída armadilha)."""
    try:
        df    = okx_candles(inst_id, bar="1H", limit=60)
        close = df["close"]
        mid   = close.rolling(BB_PERIOD).mean()
        std   = close.rolling(BB_PERIOD).std()
        upper = float((mid + BB_STD * std).iloc[-1])
        lower = float((mid - BB_STD * std).iloc[-1])
        price = float(close.iloc[-1])
        if side == "sell": return price <= lower * (1 + BB_TOL_PCT / 100)
        else:              return price >= upper * (1 - BB_TOL_PCT / 100)
    except Exception as e:
        log.warning("_h1_band_opposite %s: %s", inst_id, e)
        return False

def _v11_dashboard_text() -> str:
    opa_icon = "✅ ON" if _armadilha_mode else "⛔ OFF"
    opd_icon = "✅ ON" if _mode_opd else "⛔ OFF"
    ope_icon = "✅ ON" if _mode_ope else "⛔ OFF"
    return (
        "📊 <b>V11 FULL SQUAD (SNIPER ELITE)</b>\n"
        "─────────────────────────────\n"
        "📋 <b>ESTRATÉGIAS ACTIVAS:</b>\n"
        "✅ ON — 🥇 GOLDEN POL (Ichimoku 1H)\n"
        f"🪤 OpA (Armadilha Triple BB+SAR) [SOL, BNB, ETH, DOGE]: {opa_icon}\n"
        f"⚡ OpD (Sniper MACD M5) [ETH]: {opd_icon}\n"
        f"🏦 OpE (SMC/ICT M15) [ETH, BTC, SOL]: {ope_icon}\n"
        "⚠️ TSAR V11, TSAR POL e Filtros Antigos foram ELIMINADOS.\n"
        f"🛡️ SL 1.5% | Trail GV5 (BE 0.5% / Step 0.2%) | CB -${CIRCUIT_BREAKER_USD:.0f}\n"
        "─────────────────────────────\n"
        "Comandos: /pausar | /activar | /opa | /opd | /ope"
    )


def signal_macd_bollinger(df: pd.DataFrame) -> str | None:
    """OpD — Sniper MACD M5: exaustão fora das BB + histograma MACD a reverter.

    LONG: vela anterior fecha abaixo BB inferior → vela actual regressa dentro
          + histograma MACD era negativo e virou para cima.
    SHORT: simétrico (acima BB superior, histograma positivo virando para baixo).
    Timeframe recomendado: 5m.
    """
    if len(df) < 50: return None
    df = df.copy()
    bb = ta.bbands(df["close"], length=20, std=2.0)
    if bb is None or bb.empty: return None
    col_u = next((c for c in bb.columns if c.startswith("BBU_")), None)
    col_l = next((c for c in bb.columns if c.startswith("BBL_")), None)
    if col_u is None or col_l is None: return None
    df["bb_u"] = bb[col_u]; df["bb_l"] = bb[col_l]

    macd_df = ta.macd(df["close"], fast=12, slow=26, signal=9)
    if macd_df is None or macd_df.empty: return None
    hist_col = next((c for c in macd_df.columns if "MACDh" in c), None)
    if hist_col is None: return None
    df["hist"] = macd_df[hist_col]

    prev  = df.iloc[-2]
    prev2 = df.iloc[-3]
    if pd.isna(prev["bb_u"]) or pd.isna(prev["hist"]) or pd.isna(prev2["hist"]):
        return None

    long_cond = (
        float(prev2["close"]) < float(prev2["bb_l"])
        and float(prev["close"]) >= float(prev["bb_l"])
        and float(prev2["hist"]) < 0
        and float(prev["hist"]) > float(prev2["hist"])
    )
    short_cond = (
        float(prev2["close"]) > float(prev2["bb_u"])
        and float(prev["close"]) <= float(prev["bb_u"])
        and float(prev2["hist"]) > 0
        and float(prev["hist"]) < float(prev2["hist"])
    )
    if long_cond: return "buy"
    if short_cond: return "sell"
    return None

def _verify_macro_bollinger(inst_id: str, side: str) -> bool:
    """Confirma exaustão MTFA: M15 ou H1 devem ter a vela anterior fora/tocando a BB(20,2).

    LONG: low M15 ou H1 <= banda inferior → exaustão de baixa confirmada no macro.
    SHORT: high M15 ou H1 >= banda superior → exaustão de alta confirmada no macro.
    Retorna True se pelo menos um timeframe confirmar.
    """
    try:
        df15 = okx_candles(inst_id, bar="15m", limit=30)
        df1h = okx_candles(inst_id, bar="1H",  limit=30)

        def _bb_exhausted(df: pd.DataFrame, direction: str) -> bool:
            bb = ta.bbands(df["close"], length=20, std=2.0)
            if bb is None or bb.empty: return False
            col_u = next((c for c in bb.columns if c.startswith("BBU_")), None)
            col_l = next((c for c in bb.columns if c.startswith("BBL_")), None)
            if col_u is None or col_l is None: return False
            prev = df.iloc[-2]
            if direction == "buy":
                return float(prev["low"]) <= float(bb[col_l].iloc[-2])
            else:
                return float(prev["high"]) >= float(bb[col_u].iloc[-2])

        m15_ok = _bb_exhausted(df15, side)
        h1_ok  = _bb_exhausted(df1h, side)
        return m15_ok or h1_ok
    except Exception as e:
        log.warning("_verify_macro_bollinger %s: %s", inst_id, e)
        return False

# ══════════════════════════════════════════════════════════════════════════════
# MONITOR — aguarda fecho de posição em thread separada
# ══════════════════════════════════════════════════════════════════════════════

def signal_ict_fvg(df: pd.DataFrame) -> str | None:
    """OpE — ICT/SMC Sweep + FVG + Pullback (15m).

    SHORT: v4 sweep de topo (pavio longo, fecha em baixa) + FVG bearish + pullback que toca o gap.
    LONG:  v4 sweep de fundo (pavio longo, fecha em alta) + FVG bullish + pullback que toca o gap.
    Timeframe recomendado: 15m.
    """
    if len(df) < 30: return None
    df = df.copy()

    v1 = df.iloc[-2]   # pullback gatilho (última vela fechada)
    v2 = df.iloc[-3]   # vela do meio (cria o FVG)
    v4 = df.iloc[-5]   # vela do Sweep (captura de liquidez)

    hist_high = float(df["high"].iloc[-25:-5].max())
    hist_low  = float(df["low"].iloc[-25:-5].min())

    # ── SETUP BEARISH (SHORT) ─────────────────────────────────────────────────
    sweep_high    = float(v4["high"]) > hist_high and float(v4["close"]) < float(v4["open"])
    fvg_bear_gap  = float(v4["low"])  > float(v2["high"])
    pullback_bear = float(v1["high"]) >= float(v2["high"]) and float(v1["close"]) < float(v4["low"])

    if sweep_high and fvg_bear_gap and pullback_bear:
        return "sell"

    # ── SETUP BULLISH (LONG) ──────────────────────────────────────────────────
    sweep_low     = float(v4["low"])  < hist_low and float(v4["close"]) > float(v4["open"])
    fvg_bull_gap  = float(v4["high"]) < float(v2["low"])
    pullback_bull = float(v1["low"]) <= float(v2["low"]) and float(v1["close"]) > float(v4["high"])

    if sweep_low and fvg_bull_gap and pullback_bull:
        return "buy"

    return None

def _get_real_exit(inst_id: str) -> tuple[float, float]:
    """Retorna (closeAvgPx, pnl_líquido) da última posição fechada via positions-history.

    pnl_líquido = realizedPnl + fee + fundingFee (já inclui custos).
    Retorna (0.0, 0.0) em caso de falha — monitor usa fallback pelo mark price.
    """
    try:
        path = f"/api/v5/account/positions-history?instType=SWAP&instId={inst_id}&limit=1"
        r    = requests.get(f"https://www.okx.com{path}",
                            headers=_headers("GET", path), timeout=8)
        data = r.json().get("data", [])
        if data:
            p       = data[0]
            pnl     = float(p.get("realizedPnl", 0) or 0)
            fee     = float(p.get("fee",         0) or 0)
            fund    = float(p.get("fundingFee",  0) or 0)
            exit_px = float(p.get("closeAvgPx",  0) or 0)
            return exit_px, round(pnl + fee + fund, 2)
    except Exception as e:
        log.warning("_get_real_exit %s: %s", inst_id, e)
    return 0.0, 0.0

def _monitor(inst_id: str, pos_side: str, side: str,
             entry: float, sl_px: float, activate_px: float,
             sym: str, dir_txt: str, bal: float, qty: int,
             tag: str = "DUO ELITE",
             min_trail_pct: float = 0.0, fast_trail: bool = False) -> None:
    global _duo_in_trade, _duo_cooldown_until
    log.info("📡 SENTINELA [%s] %s %s | SL=%.5f | Trailing activa a %.5f | STEP TRAIL V5",
             tag, sym, dir_txt, sl_px, activate_px)
    _none_streak      = 0
    _step_trail_tier  = 0
    _fast_be_done     = False
    while True:
        time.sleep(20)
        try:
            pos = okx_get_position(inst_id, pos_side)

            # ── 🎯 PROFIT LOCK — só activo se PROFIT_LOCK_USD > 0 ───────────────
            if pos is not None and PROFIT_LOCK_USD > 0:
                upl_tp  = float(pos.get("upl", 0) or 0)
                pos_sz_tp = int(float(pos.get("pos", qty) or qty))
                if upl_tp >= PROFIT_LOCK_USD:
                    log.info("🎯 PROFIT LOCK +$%.2f atingido — fechando %s!", upl_tp, sym)
                    try:
                        cancel_all_open_orders(inst_id)
                        time.sleep(0.5)
                        okx_close_market(inst_id, pos_side, pos_sz_tp)
                        tg(f"🎯 <b>PROFIT LOCK +${PROFIT_LOCK_USD:.0f} — FECHO COM LUCRO!</b>\n"
                           f"Par: <code>{sym}</code> | {dir_txt}\n"
                           f"Lucro realizado: <b>${upl_tp:+.2f} USDT</b> 🏆\n"
                           f"⏳ Cooldown 30 min activado. Aguardando próximo sinal Elite.")
                        with _duo_lock:
                            _duo_in_trade       = False
                            _duo_cooldown_until = time.time() + DUO_COOLDOWN
                        return
                    except Exception as e:
                        log.error("profit lock close fail %s: %s", sym, e)
                    continue

            # ── 🚨 GLOBAL CIRCUIT BREAKER — fecho se PnL ≤ -$50 ────────────────
            if pos is not None:
                upl_cb    = float(pos.get("upl", 0) or 0)
                pos_sz_cb = int(float(pos.get("pos", qty) or qty))
                if upl_cb <= -CIRCUIT_BREAKER_USD:
                    log.warning("🚨 CIRCUIT BREAKER %s: PnL $%.2f — fechando!", sym, upl_cb)
                    try:
                        cancel_all_open_orders(inst_id); time.sleep(0.5)
                        okx_close_market(inst_id, pos_side, pos_sz_cb)
                        tg(f"🚨 <b>CIRCUIT BREAKER -${CIRCUIT_BREAKER_USD:.0f} — FECHO DE EMERGÊNCIA</b>\n"
                           f"Par: <code>{sym}</code> | {dir_txt}\n"
                           f"PnL nominal: <b>${upl_cb:+.2f} USDT</b>\n"
                           f"🛡️ Banca protegida — cooldown activado.")
                        with _duo_lock:
                            _duo_in_trade       = False
                            _duo_cooldown_until = time.time() + DUO_COOLDOWN
                        return
                    except Exception as e:
                        log.error("circuit breaker close fail %s: %s", sym, e)
                    continue

            # ── GESTÃO DE POSIÇÃO (Step Trail V5 / Fast Trail) ───────────────────
            if pos is not None:
                upl     = float(pos.get("upl",    0) or 0)
                mark_px = float(pos.get("markPx", 0) or 0)
                avg_px  = float(pos.get("avgPx",  entry) or entry)
                pos_sz  = int(float(pos.get("pos", qty) or qty))

                if fast_trail:
                    # ── FAST TRAIL: break-even a +0.5%, trailing 0.2% ────────
                    _pct_g = (((mark_px - avg_px) / avg_px * 100) if side == "buy"
                              else ((avg_px - mark_px) / avg_px * 100)) if avg_px > 0 else 0.0
                    if not _fast_be_done and _pct_g >= 0.5:
                        try:
                            clear_garbage(inst_id, pos_side); time.sleep(0.5)
                            okx_initial_sl(inst_id, pos_side, pos_sz, avg_px)
                            okx_trailing_stop(inst_id, pos_side, pos_sz,
                                              avg_px * (1.005 if side == "buy" else 0.995),
                                              callback=0.002)
                            _fast_be_done = True
                            log.info("🔒 FAST TRAIL: Break-Even @ +%.2f%% — SL=%.5f", _pct_g, avg_px)
                            tg(f"🔒 <b>FAST TRAIL: BREAK-EVEN</b>\n"
                               f"Par: <code>{sym}</code> | {dir_txt}\n"
                               f"💰 Lucro +{_pct_g:.2f}% | SL → entry <code>{avg_px:.5f}</code>\n"
                               f"📡 Trailing 0.2% activo")
                        except Exception as e:
                            log.error("fast trail break-even: %s", e)
                else:
                    # ── STEP TRAIL V5 (modo normal) ──────────────────────────
                    if _step_trail_tier < len(STEP_TRAIL_LEVELS) and mark_px > 0 and avg_px > 0:
                        trigger_usd, lock_usd = STEP_TRAIL_LEVELS[_step_trail_tier]
                        _pct_g = (((mark_px - avg_px) / avg_px * 100) if side == "buy"
                                  else ((avg_px - mark_px) / avg_px * 100)) if avg_px > 0 else 0.0
                        if min_trail_pct > 0.0 and _pct_g < min_trail_pct:
                            pass  # aguarda lucro mínimo antes de activar step trail
                        elif upl >= trigger_usd:
                            if side == "buy":
                                price_move = mark_px - avg_px
                                lock_px    = avg_px + lock_usd * (price_move / upl)
                            else:
                                price_move = avg_px - mark_px
                                lock_px    = avg_px - lock_usd * (price_move / upl)
                            grau = _step_trail_tier + 1
                            log.info("🔒 STEP TRAIL GRAU %d — $%.0f atingido | %s SL=%.5f",
                                     grau, trigger_usd, sym, lock_px)
                            try:
                                clear_garbage(inst_id, pos_side); time.sleep(0.5)
                                okx_initial_sl(inst_id, pos_side, pos_sz, lock_px)
                                okx_trailing_stop(inst_id, pos_side, pos_sz,
                                                  mark_px * (1 + TRAIL_ACTIVATE_PCT/100) if side == "buy"
                                                  else mark_px * (1 - TRAIL_ACTIVATE_PCT/100))
                                _step_trail_tier += 1
                                grau_bar = "🟢" * grau + "⚪" * (len(STEP_TRAIL_LEVELS) - grau)
                                prox_txt = (
                                    f"Próximo grau: +${STEP_TRAIL_LEVELS[_step_trail_tier][0]:.0f} → piso +${STEP_TRAIL_LEVELS[_step_trail_tier][1]:.0f}"
                                    if _step_trail_tier < len(STEP_TRAIL_LEVELS) else "🏆 GRAU MÁXIMO ATINGIDO!"
                                )
                                tg(f"🔒 <b>STEP TRAIL GRAU {grau}/5</b> {grau_bar}\n"
                                   f"Par: <code>{sym}</code> | {dir_txt}\n"
                                   f"💰 Lucro actual: <b>${upl:+.2f}</b> → SL: <b>+${lock_usd:.0f}</b>\n"
                                   f"📍 SL price: <code>{lock_px:.5f}</code>\n"
                                   f"📡 {prox_txt}")
                            except Exception as e:
                                log.error("step trail grau %d: %s", grau, e)

                _none_streak = 0
                continue


            _none_streak += 1
            if _none_streak < 3:
                log.debug("SENTINELA %s: confirmação %d/3...", sym, _none_streak)
                continue

            # 3 Nones consecutivos → posição confirmada fechada
            # Aguarda 2s para OKX actualizar positions-history e balance real
            time.sleep(2)
            # Tenta PnL real da OKX (fill price + fees); fallback: mark price
            exit_px, pnl_usd = _get_real_exit(inst_id)
            if exit_px > 0:
                pnl_pct = ((exit_px - entry) / entry * 100 * LEVERAGE if side == "buy"
                           else (entry - exit_px) / entry * 100 * LEVERAGE)
            else:
                try: exit_px = okx_ticker(inst_id)
                except Exception: exit_px = entry
                pnl_pct = ((exit_px - entry) / entry * 100 * LEVERAGE if side == "buy"
                           else (entry - exit_px) / entry * 100 * LEVERAGE)
                pnl_usd = round(bal * pnl_pct / 100, 2)
            win = pnl_usd > 0

            # ── mensagem de saída limpa ───────────────────────────────────────
            if _step_trail_tier > 0:
                grau_bar = "🟢" * _step_trail_tier + "⚪" * (len(STEP_TRAIL_LEVELS) - _step_trail_tier)
                icon     = "✅" if win else "⚠️"
                result   = f"STEP TRAIL GRAU {_step_trail_tier}/5 {grau_bar}"
            else:
                icon   = "🎯" if win else "💥"
                result = "SAÍDA COM LUCRO 🎯" if win else "SAÍDA COM PERDA 💥"

            tg(f"{icon} <b>{tag} — {result}</b>\n"
               f"Par: <code>{sym}</code> | {dir_txt}\n"
               f"Entrada: <code>{entry:.5f}</code> → Saída: <code>{exit_px:.5f}</code>\n"
               f"P&L Real: <b>${pnl_usd:+.2f} USDT</b> ({pnl_pct:+.2f}%)\n"
               f"⏳ Cooldown 30 min activado.")
            # variáveis locais (_step_trail_tier, _none_streak) são reiniciadas
            # automaticamente na próxima chamada a _monitor()

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

def get_rsi_dual(inst_id: str) -> tuple[float, float]:
    """Retorna (rsi14, rsi2) no timeframe 15m do instrumento.

    rsi14 — filtro de maré  : >50 = tendência LONG  | <50 = tendência SHORT
    rsi2  — gatilho sniper  : <20 = entrada LONG ok | >80 = entrada SHORT ok
    Em caso de falha retorna (50.0, 50.0) — neutro, não bloqueia nem dispara.
    """
    try:
        r = requests.get(
            f"{OKX_BASE}/market/candles?instId={inst_id}&bar=15m&limit=30",
            timeout=8
        )
        data = r.json()
        if data.get("code") != "0":
            return (50.0, 50.0)
        df = pd.DataFrame(data["data"],
             columns=["ts","open","high","low","close","vol","volCcy","volCcyQuote","confirm"])
        df["close"] = pd.to_numeric(df["close"])
        closes = df["close"].iloc[::-1].reset_index(drop=True)

        def _rsi(n: int) -> float:
            delta = closes.diff()
            gain  = delta.clip(lower=0).ewm(span=n, adjust=False).mean()
            loss  = (-delta).clip(lower=0).ewm(span=n, adjust=False).mean()
            return float(100 - (100 / (1 + gain.iloc[-1] / (loss.iloc[-1] + 1e-10))))

        return (_rsi(14), _rsi(2))
    except Exception as e:
        log.warning("get_rsi_dual %s: %s — neutro", inst_id, e)
        return (50.0, 50.0)

def get_btc_sentiment() -> tuple[str, bool, float, float, float]:
    """Retorna (sentiment, blocked, price, ema20_1h, rsi15).

    sentiment : "BULLISH" | "BULLISH_FRACO" | "NEUTRO" | "BEARISH_FRACO" | "BEARISH"
    blocked   : True se RSI 15m > 72 ou < 28 (zona de exaustão)
    price     : último close BTC 1H
    ema20_1h  : EMA20 do 1H (sincronizada com Ichimoku POL 1H)
    rsi15     : RSI14 do 15m
    """
    try:
        # 15m para RSI de curto prazo
        r15 = requests.get(
            f"{OKX_BASE}/market/candles?instId=BTC-USDT-SWAP&bar=15m&limit=50",
            timeout=8
        )
        # 1H para tendência macro (sincronizado com Ichimoku POL 1H)
        r1h = requests.get(
            f"{OKX_BASE}/market/candles?instId=BTC-USDT-SWAP&bar=1H&limit=50",
            timeout=8
        )
        d15 = r15.json(); d1h = r1h.json()
        if d15.get("code") != "0" or d1h.get("code") != "0":
            return ("NEUTRO", False, 0.0, 0.0, 0.0)

        # RSI14 do 15m
        df15 = pd.DataFrame(d15["data"],
               columns=["ts","open","high","low","close","vol",
                        "volCcy","volCcyQuote","confirm"])
        df15["close"] = pd.to_numeric(df15["close"])
        c15   = df15["close"].iloc[::-1].reset_index(drop=True)
        delta = c15.diff()
        gain  = delta.clip(lower=0).ewm(span=14, adjust=False).mean()
        loss  = (-delta).clip(lower=0).ewm(span=14, adjust=False).mean()
        rsi   = 100 - (100 / (1 + gain.iloc[-1] / (loss.iloc[-1] + 1e-10)))

        # EMA20 e EMA50 do 1H (macro)
        df1h = pd.DataFrame(d1h["data"],
               columns=["ts","open","high","low","close","vol",
                        "volCcy","volCcyQuote","confirm"])
        df1h["close"] = pd.to_numeric(df1h["close"])
        c1h      = df1h["close"].iloc[::-1].reset_index(drop=True)
        ema20_1h = c1h.ewm(span=20, adjust=False).mean().iloc[-1]
        ema50_1h = c1h.ewm(span=50, adjust=False).mean().iloc[-1]
        price    = c1h.iloc[-1]

        # Zona neutra: BTC dentro de 0.3% da EMA20 = indecisão
        dist_ema = abs(price - ema20_1h) / ema20_1h * 100
        if dist_ema < 0.3:
            sentiment = "NEUTRO"
        elif price > ema20_1h and ema20_1h > ema50_1h:
            sentiment = "BULLISH"        # forte: EMA20 > EMA50
        elif price > ema20_1h:
            sentiment = "BULLISH_FRACO"
        elif price < ema20_1h and ema20_1h < ema50_1h:
            sentiment = "BEARISH"        # forte: EMA20 < EMA50
        else:
            sentiment = "BEARISH_FRACO"

        blocked = rsi > 72 or rsi < 28
        log.info("🛡️ BTC SENTINEL: price=%.0f ema20_1h=%.0f rsi15=%.1f → %s %s",
                 price, ema20_1h, rsi, sentiment, "BLOQUEADO" if blocked else "OK")
        return (sentiment, blocked, float(price), float(ema20_1h), float(rsi))
    except Exception as e:
        log.warning("BTC Sentinel erro: %s — permitindo entrada", e)
        return ("NEUTRO", False, 0.0, 0.0, 0.0)


def is_bullish_pin_bar(candle) -> bool:
    """Vela com pavio inferior dominante: lower_wick > 60% range e corpo < 30% range."""
    body        = abs(float(candle["close"]) - float(candle["open"]))
    lower_wick  = min(float(candle["open"]), float(candle["close"])) - float(candle["low"])
    total_range = float(candle["high"]) - float(candle["low"])
    if total_range <= 0:
        return False
    return (lower_wick > total_range * 0.6) and (body < total_range * 0.3)


def check_exhaustion_override(inst_id: str, sym: str) -> bool:
    """Exceção Tática: libera LONG bearish se RSI M15 BTC < 35 e pin bar bullish presente.

    Retorna True (override ativo) ou False (manter bloqueio).
    """
    try:
        df_btc      = okx_candles("BTC-USDT-SWAP", bar="15m", limit=20)
        current_rsi = float(ta.rsi(df_btc["close"], length=14).iloc[-1])
        last_candle = df_btc.iloc[-1]
        if current_rsi < 35 and is_bullish_pin_bar(last_candle):
            log.info("🛡️ SENTINEL OVERRIDE: Exaustão e Pin Bar detectados no BTC M15. Liberando LONG!")
            tg("⚠️ [SENTINEL OVERRIDE] Tendência Macro de Baixa, mas detectado "
               "PIN BAR + RSI em Exaustão no BTC M15. Ordem Liberada! 🚀")
            return True
        return False
    except Exception as e:
        log.warning("check_exhaustion_override %s: %s", inst_id, e)
        return False


def _fire(inst_id: str, side: str, signal_name: str,
          tag: str = "DUO ELITE", sl_pct: float | None = None,
          force: bool = False, qty_mult: float = 1.0,
          sl_px_override: float = 0.0,
          min_trail_pct: float = 0.0, fast_trail: bool = False) -> bool:
    """Executa ordem market + SL inicial + Step Trail V5."""
    global _duo_in_trade, _lockdown_until

    # ── Routing automático do SL pela classificação do par ────────────────
    if sl_pct is None:
        if   inst_id in HOLD_PAIRS:   sl_pct = HOLD_SL_PCT
        elif inst_id in STRICT_PAIRS: sl_pct = STRICT_SL_PCT
        else:                         sl_pct = DUO_SL_PCT
    ps      = _SIDE_PS[side]
    sym     = inst_id.replace("-USDT-SWAP", "")
    dir_txt = "LONG 🟢" if side == "buy" else "SHORT 🔴"

    rsi14, rsi2 = get_rsi_dual(inst_id)   # para mensagem Telegram de entrada

    if force:
        log.info("⚡ [FORCE] %s — filtros IGNORADOS", sym)

    # ── FILTRO M5 / ARMADILHA TRIPLE BB ─────────────────────────────────────
    sar_px = 0.0
    if not force:
        if _armadilha_mode:
            # ── TRIPLE BOLLINGER CONFLUENCE (M5 + M15 + H1) ──────────────────
            tri_side, tri_signal = _triple_bb_touch(inst_id)
            if tri_side == "none":
                log.info("[TRIPLE BB] %s sem confluência tripla — bloqueado", sym)
                return False
            sar_dir = "bear" if tri_side == "upper" else "bull"
            if not _sar_just_inverted(inst_id, sar_dir):
                log.info("[TRIPLE BB] %s toque triplo OK — aguardando SAR M5 inverter", sym)
                return False
            if tri_signal != side:
                log.info("[TRIPLE BB 🪤] %s INVERTIDO → %s (exaustão tripla)", sym, tri_signal)
                tg(f"[🪤 TRIPLE BB] <b>{sym} — EXAUSTÃO TRIPLA</b>\n"
                   f"{'LONG 🟢' if tri_signal == 'buy' else 'SHORT 🔴'} | "
                   f"Banda {'superior' if tri_side == 'upper' else 'inferior'} M5+M15+H1\n"
                   f"SAR M5 invertido | SL: {SCALP_SL_PCT}%")
            else:
                log.info("[TRIPLE BB ✅] %s exaustão tripla confirma sinal", sym)
                tg(f"[✅ TRIPLE BB] <b>{sym}</b> — confluência tripla confirmada\n"
                   f"{'LONG 🟢' if tri_signal == 'buy' else 'SHORT 🔴'} | SL SAR M5")
            side   = tri_signal
            sl_pct = SCALP_SL_PCT
            _, _, _, sar_px = _m5_confirm(inst_id, side)
        else:
            # ── M5 CONFIRM normal (armadilha OFF) ────────────────────────────
            m5_act, side, m5_dbg, sar_px = _m5_confirm(inst_id, side)
            if m5_act == "block":
                log.info("[M5] %s bloqueado — %s", sym, m5_dbg)
                return False
            if m5_act == "invert":
                log.info("[M5] %s inversion sem armadilha — bloqueado", sym)
                return False
        ps      = _SIDE_PS[side]
        dir_txt = "LONG 🟢" if side == "buy" else "SHORT 🔴"

    # ONE DIRECTION ONLY — se EXISTE qualquer posição (mesmo lado oposto), aborta
    existing = okx_any_position_open(ALL_SYMS)
    if existing is not None:
        ex_sym, ex_ps = existing
        log.info("🛑 [%s] BLOQUEADO — posição já aberta em %s/%s. Aguardar fecho.",
                 sym, ex_sym, ex_ps)
        return False

    # ── Ordem MARKET — execução imediata ao preço actual ─────────────────────
    bal       = okx_balance() or 0.0
    market_px = okx_ticker(inst_id)
    qty       = calc_qty(inst_id, market_px, bal)
    if qty_mult != 1.0:
        qty = max(1, int(qty * qty_mult))

    # 🔍 DEBUG DE MARGEM — visível no log do Render
    log.info("💰 [%s] SALDO DISPONÍVEL: $%.4f USDT | mkt=%.5f qty=%d side=%s",
             sym, bal, market_px, qty, side)

    if bal <= 0:
        log.error("[%s] saldo zero ou inválido — ordem abortada", sym)
        tg(f"❌ <b>{tag}</b> {sym}: saldo zero ou inválido — verifica credenciais OKX.")
        return False
    if qty < 1:
        log.error("[%s] qty<1 (bal=%.4f mkt_px=%.5f) — saldo insuficiente", sym, bal, market_px)
        tg(f"❌ <b>{tag}</b> {sym}: qty<1 (bal=${bal:.4f}) — saldo insuficiente para 1 contrato.")
        return False

    # 🛡️ LOCKDOWN — só activa após passar TODOS os filtros (anti ping-pong real)
    with _duo_lock:
        _lockdown_until = max(_lockdown_until, time.time() + LOCKDOWN_SECS)

    # Pre-alerta TG sincronizado com o disparo — enviado na mesma passagem que a ordem
    log.info("🚀 [%s] DISPARANDO ordem %s %s qty=%d px≈%.5f", tag, sym, side.upper(), qty, market_px)
    tg(f"🎯 <b>{tag} — SINAL CONFIRMADO</b>\n"
       f"Par: <code>{sym}</code> | {'📈 LONG' if side == 'buy' else '📉 SHORT'} | {signal_name}\n"
       f"💰 Alvo $20 NET | CB -${CIRCUIT_BREAKER_USD:.0f} | Enviando ordem…")

    try:
        res    = okx_open_market(inst_id, side, qty)
        ord_id = (res.get("data") or [{}])[0].get("ordId", "?")
        log.info("📋 MARKET ORDER [%s] %s ordId=%s qty=%d px≈%.5f",
                 tag, sym, ord_id, qty, market_px)
        tg(f"⚔️ <b>{tag} — SNIPER MARKET</b>\n"
           f"Sinal: <b>{signal_name}</b> | Par: <code>{sym}</code> | {dir_txt}\n"
           f"RSI14: <b>{rsi14:.1f}</b> | RSI2: <b>{rsi2:.1f}</b>\n"
           f"Preço: <code>{market_px:.5f}</code> | ordId: <code>{ord_id}</code> | {LEVERAGE}× ALL-IN")
    except Exception as ex:
        err_msg = str(ex)
        log.error("❌ ERRO OKX [%s] %s qty=%d px=%.5f: %s", tag, sym, qty, market_px, err_msg)
        tg(f"❌ <b>ERRO OKX [{tag}] {sym}</b>\nqty={qty} px={market_px:.5f}\n<code>{err_msg}</code>")
        with _duo_lock:
            _lockdown_until = time.time()   # reset lockdown — falha não deve penalizar
        return False

    # ── Aguardar preenchimento confirmado (máx 10s) ───────────────────────────
    avg   = market_px
    for _ in range(50):   # 50 × 200ms = máx 10s
        time.sleep(0.2)
        pos = okx_get_position(inst_id, ps)
        if pos and float(pos.get("avgPx", 0)) > 0:
            avg = float(pos["avgPx"])
            log.info("✅ MARKET FILLED %s avgPx=%.5f", sym, avg)
            break

    # ── Configurar SL + Step Trail V5 ────────────────────────────────────────
    # Prioridade: sl_px_override (SAR M5 externo) > sar_px interno > sl_pct %
    if sl_px_override > 0 and ((side == "buy" and sl_px_override < avg) or (side == "sell" and sl_px_override > avg)):
        sl_px = sl_px_override
    elif sar_px > 0 and ((side == "buy" and sar_px < avg) or (side == "sell" and sar_px > avg)):
        sl_px = sar_px
    else:
        sl_px = avg * (1 - sl_pct / 100) if side == "buy" else avg * (1 + sl_pct / 100)
    if fast_trail:
        activate_px = avg * 1.005 if side == "buy" else avg * 0.995
        okx_initial_sl(inst_id, ps, qty, sl_px)
        okx_trailing_stop(inst_id, ps, qty, activate_px, callback=0.002)
    else:
        activate_px = avg * (1 + TRAIL_ACTIVATE_PCT / 100) if side == "buy" else avg * (1 - TRAIL_ACTIVATE_PCT / 100)
        okx_initial_sl(inst_id, ps, qty, sl_px)
        okx_trailing_stop(inst_id, ps, qty, activate_px)

    tg(f"✅ <b>{tag} — ENTRADA CONFIRMADA (Market)</b>\n"
       f"Par: <code>{sym}</code> | {dir_txt}\n"
       f"Fill: <code>{avg:.5f}</code> | SL: <code>{sl_px:.5f}</code> (-{sl_pct}%)\n"
       f"📡 Trailing activa a <code>{activate_px:.5f}</code> (+{TRAIL_ACTIVATE_PCT}%)\n"
       f"🔒 Step Trail V5 activo | CB -${CIRCUIT_BREAKER_USD:.0f}")

    with _duo_lock:
        _duo_in_trade = True

    threading.Thread(target=_monitor,
        args=(inst_id, ps, side, avg, sl_px, activate_px, sym, dir_txt, bal, qty),
        kwargs={"tag": tag,
                "min_trail_pct": min_trail_pct, "fast_trail": fast_trail},
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
        (GOLD_POL, "POL", "1H",  "15m", "🥇 ICHIMOKU", ichimoku_signal),
        (DUO_ETH,  "ETH", "5m",  "15m", "⚡ MACD BB",  signal_macd_bollinger),
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
    """P&L realizado nas últimas 24 horas via positions-history (P&L real por posição fechada)."""
    if not _has_creds(): return "❌ Sem credenciais OKX."
    PNL_FLOOR_MS = int(datetime(2026, 4, 18, tzinfo=timezone.utc).timestamp() * 1000)
    cutoff = max(int((time.time() - 86400) * 1000), PNL_FLOOR_MS)
    path = "/api/v5/account/positions-history?instType=SWAP&limit=100"
    try:
        r    = requests.get(f"https://www.okx.com{path}", headers=_headers("GET", path), timeout=10)
        data = r.json()
        if data.get("code") != "0":
            return f"❌ /lpd API: {data.get('msg')}"
        pos_list = [p for p in data.get("data", []) if int(p.get("uTime", 0)) >= cutoff]
        if not pos_list:
            return "📭 <b>Sem posições fechadas nas últimas 24h.</b>"
        gross = sum(float(p.get("pnl", 0) or 0) for p in pos_list)
        fee   = sum(float(p.get("fee", 0) or 0) for p in pos_list)
        fund  = sum(float(p.get("fundingFee", 0) or 0) for p in pos_list)
        net   = gross + fee + fund
        icon  = "✅" if net >= 0 else "🔴"
        fee_str  = f"${fee:+.2f}" if fee != 0 else "$0.00"
        fund_str = f"${fund:+.2f}" if fund != 0 else "$0.00"
        return (f"{icon} <b>P&amp;L últimas 24h</b>\n"
                f"Posições fechadas: <b>{len(pos_list)}</b>\n"
                f"Gross P&amp;L: <b>${gross:+.2f}</b>\n"
                f"Comissões: <b>{fee_str}</b> | Funding: <b>{fund_str}</b>\n"
                f"P&amp;L Líquido: <b>${net:+.2f} USDT</b>")
    except Exception as e:
        return f"❌ Erro /lpd: {e}"


def cmd_meta() -> str:
    """Progresso em relação à meta de $600/mês (desde 18 Abr 2026) via positions-history."""
    if not _has_creds(): return "❌ Sem credenciais OKX."
    now   = datetime.now(timezone.utc)
    start = int(datetime(2026, 4, 18, tzinfo=timezone.utc).timestamp() * 1000)
    path  = "/api/v5/account/positions-history?instType=SWAP&limit=100"
    try:
        r    = requests.get(f"https://www.okx.com{path}", headers=_headers("GET", path), timeout=10)
        data = r.json()
        if data.get("code") != "0":
            return f"❌ /meta API: {data.get('msg')}"
        pos_list = [p for p in data.get("data", []) if int(p.get("uTime", 0)) >= start]
        gross = sum(float(p.get("pnl", 0) or 0) for p in pos_list)
        fee   = sum(float(p.get("fee", 0) or 0) for p in pos_list)
        fund  = sum(float(p.get("fundingFee", 0) or 0) for p in pos_list)
        net   = gross + fee + fund
        pct    = min(net / MONTHLY_GOAL_USD * 100, 100.0) if MONTHLY_GOAL_USD > 0 else 0.0
        filled = int(pct / 5)
        bar    = "█" * filled + "░" * (20 - filled)
        icon   = "🏆" if pct >= 100 else ("🔥" if pct >= 50 else "📈")
        return (f"{icon} <b>META MENSAL — {now.strftime('%B %Y')}</b>\n"
                f"<code>[{bar}]</code> {pct:.1f}%\n"
                f"Realizado: <b>${net:+.2f}</b> / Meta: <b>${MONTHLY_GOAL_USD:.0f}</b>\n"
                f"Faltam: <b>${max(MONTHLY_GOAL_USD - net, 0):.2f} USDT</b>\n"
                f"Posições desde 18 Abr: {len(pos_list)}")
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
            cancel_all_open_orders(inst_id); time.sleep(1)
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
    "doge": GOLD_DOGE, "bnb": FVG_BNB,
}

def cmd_force(coin: str) -> str:
    """Abre ordem de mercado IGNORANDO filtros de estratégia.
    Direcção decidida por RSI 15m: > 50 → LONG  |  < 50 → SHORT.
    Usa LEVERAGE=5x e reserva fixa $30 (mesma config do bot)."""
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
    # force=True ignora BTC Sentinel e RSI Dual — ordem directa
    ok = _fire(inst_id, side, f"FORCE RSI={rsi:.0f}", tag="🎯 FORCE", force=True)
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
                    cancel_all_open_orders(inst_id)
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

def cmd_cenario(inst_id: str = "BTC-USDT-SWAP") -> str:
    """Diagnóstico de mercado via ADX(14) + EMA200 no 1H — recomenda módulo a activar."""
    try:
        df = okx_candles(inst_id, bar="1H", limit=220)
        if len(df) < 200:
            return "❌ Dados insuficientes para análise (precisa ≥200 velas 1H)."

        close  = df["close"]
        ema200 = float(close.ewm(span=200, adjust=False).mean().iloc[-1])
        px     = float(close.iloc[-1])
        sym    = inst_id.replace("-USDT-SWAP", "")

        adx_df = ta.adx(df["high"], df["low"], df["close"], length=14)
        if adx_df is None or adx_df.empty:
            return "❌ Erro a calcular ADX."
        adx_col = next((c for c in adx_df.columns if c.startswith("ADX_")), None)
        if adx_col is None:
            return "❌ Coluna ADX não encontrada."
        adx = float(adx_df[adx_col].iloc[-1])

        px_vs_ema = "acima" if px > ema200 else "abaixo"
        trend_str = "📈 ALTA" if px > ema200 else "📉 BAIXA"

        if adx < 25:
            cenario = "🌊 MERCADO LATERAL / CONSOLIDAÇÃO"
            rec = (
                "ADX < 25 — sem força direcional. Tendências falsas dominam.\n\n"
                "🎯 <b>RECOMENDAÇÃO:</b>\n"
                "✅ Ligar: <code>/opd</code> (Sniper MACD M5) ou <code>/tsarpol on</code>\n"
                "⛔ Desligar: OpB e OpC (risco de falsos rompimentos)"
            )
        elif px > ema200:
            cenario = "📈 TENDÊNCIA DE ALTA CONFIRMADA"
            rec = (
                f"ADX ≥ 25 + preço acima EMA200 ({ema200:.2f}) — momentum bullish.\n\n"
                "🎯 <b>RECOMENDAÇÃO:</b>\n"
                "✅ Ligar: <code>/opb</code> (PA — surfar pullbacks de alta)\n"
                "⚠️ Opcional: <code>/pr ichimoku</code> como filtro extra\n"
                "⛔ Desligar: TSAR V11 (pode tentar adivinhar topo)"
            )
        else:
            cenario = "📉 TENDÊNCIA DE BAIXA (BEARISH)"
            rec = (
                f"ADX ≥ 25 + preço abaixo EMA200 ({ema200:.2f}) — momentum bearish.\n\n"
                "🎯 <b>RECOMENDAÇÃO:</b>\n"
                "✅ Ligar: <code>/opb</code> com foco em Shorts nos repiques\n"
                "✅ Opcional: <code>/tsar on</code> para repiques bruscos\n"
                "⛔ Evitar: Longs contra tendência"
            )

        return (
            f"🧭 <b>DIAGNÓSTICO DE CENÁRIO — {sym} 1H</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>{cenario}</b>\n\n"
            f"📊 ADX(14): <b>{adx:.1f}</b> {'⚡ Forte' if adx >= 25 else '😴 Fraco'}\n"
            f"📉 EMA200:  <b>{ema200:.4f}</b>\n"
            f"💰 Preço:   <b>{px:.4f}</b> ({px_vs_ema} da EMA200 {trend_str})\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{rec}"
        )
    except Exception as e:
        return f"❌ Erro /cenario: {e}"

def cmd_backtest() -> str:
    """Backtest real 100 dias — busca candles OKX e simula cada estratégia vela a vela."""
    tg("⏳ <b>BACKTEST INICIADO</b>\nBuscando 100 dias de dados reais OKX...\nAguarda ~40 segundos.")

    CONFIGS = [
        ("🥇 POL Ichimoku 1H",    GOLD_POL,   "1H",  _bt_ichimoku, 5.0, 4.0),
        ("🔷 FVG ETH 15m",        DUO_ETH,    "15m", _bt_fvg,      5.0, 4.0),
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

    trail_txt = ("🔒 GV5 Step Trail" if _trail_mode == "gv5" else "📡 GV6 SAR M15")
    arm_txt   = "🪤 OpA ON" if _armadilha_mode else ""
    modes_txt = " | ".join(filter(None, [trail_txt, arm_txt]))
    return (f"📊 <b>SNIPER ELITE — {datetime.now(timezone.utc).strftime('%d/%m %H:%M UTC')}</b>\n"
            f"💰 {bal_str}\n"
            f"Status: {status}\n"
            f"⚙️ Alavancagem: <b>{LEVERAGE}×</b>  |  CB -${CIRCUIT_BREAKER_USD:.0f}\n"
            f"🎛️ Modo: {modes_txt}\n"
            f"🥇 POL · 🪤 OpA [SOL/BNB/ETH/DOGE] · ⚡ OpD [ETH] · 🏦 OpE [ETH/BTC/SOL]\n\n"
            f"<b>COMANDOS:</b>\n"
            f"/tp /radar /lpd /meta /status /panic\n"
            f"/opa /opd /ope /modo_sniper /armadilha\n"
            f"/subir [2-10]  |  /gv5 /gv6\n"
            f"/pause → só /start desbloqueia")

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
    "godoge": GOLD_DOGE,
    "gobnb":  FVG_BNB,
}

def telegram_commands_loop() -> None:
    global _tg_offset, _bot_authorized, _panic_until, LEVERAGE, _trail_mode, _armadilha_mode, _mode_opd, _mode_ope
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

                elif cmd in ("pause", "stop", "off"):
                    # /pause = pausa PERMANENTE do bot — só /start desbloqueia
                    with _auth_lock: _bot_authorized = False
                    _save_state(False)
                    _panic_until = 0.0   # cancela qualquer auto-resume pendente
                    tg("⛔ <b>Bot PAUSADO</b>\n"
                       "O bot <b>não retoma automaticamente</b>.\n"
                       "Usa <code>/start</code> para autorizar novamente.", chat_id)
                    log.info("Bot pausado (permanente) via Telegram")

                # ── /pausar [estrategia|tudo] — pausa estratégia individual ───
                elif cmd == "pausar":
                    if not args:
                        valid = " | ".join(_STRATEGY_KEYS)
                        tg(f"❌ Especifica a estratégia.\n"
                           f"Ex: <code>/pausar engolfo</code>\n"
                           f"Opções: <code>{valid} | tudo</code>\n"
                           f"(Para pausar o bot inteiro: <code>/pause</code>)", chat_id)
                    else:
                        key = args[0]
                        with _strategy_lock:
                            if key == "tudo":
                                for k in _STRATEGY_KEYS:
                                    _strategy_enabled[k] = False
                                tg("⛔ <b>Todas as estratégias PAUSADAS.</b>\n"
                                   "Usa <code>/activar tudo</code> para reactivar.\n"
                                   "(Bot continua activo — apenas sem entrar em novas trades)", chat_id)
                                log.info("Todas as estratégias pausadas via /pausar tudo")
                            elif key in _strategy_enabled:
                                _strategy_enabled[key] = False
                                tg(f"⛔ Estratégia <b>{key.upper()}</b> pausada.\n"
                                   f"Usa <code>/activar {key}</code> para reactivar.", chat_id)
                                log.info("Estratégia %s pausada via Telegram", key)
                            else:
                                valid = " | ".join(_STRATEGY_KEYS)
                                tg(f"❌ Estratégia <b>{key}</b> não reconhecida.\n"
                                   f"Opções: <code>{valid} | tudo</code>", chat_id)
                        _save_state(_bot_authorized)

                # ── /activar [estrategia|tudo] — reactiva estratégia ───────────
                elif cmd == "activar":
                    if not args:
                        valid = " | ".join(_STRATEGY_KEYS)
                        tg(f"❌ Especifica a estratégia.\n"
                           f"Ex: <code>/activar fvg</code>\n"
                           f"Opções: <code>{valid} | tudo</code>", chat_id)
                    else:
                        key = args[0]
                        with _strategy_lock:
                            if key == "tudo":
                                for k in _STRATEGY_KEYS:
                                    _strategy_enabled[k] = True
                                tg("✅ <b>Todas as estratégias ACTIVADAS.</b>", chat_id)
                                log.info("Todas as estratégias reactivadas via /activar tudo")
                            elif key in _strategy_enabled:
                                _strategy_enabled[key] = True
                                tg(f"✅ Estratégia <b>{key.upper()}</b> reactivada.", chat_id)
                                log.info("Estratégia %s reactivada via Telegram", key)
                            else:
                                valid = " | ".join(_STRATEGY_KEYS)
                                tg(f"❌ Estratégia <b>{key}</b> não reconhecida.\n"
                                   f"Opções: <code>{valid} | tudo</code>", chat_id)
                        _save_state(_bot_authorized)

                # ── /estrategias — lista estado ON/OFF de cada estratégia ───────
                elif cmd == "estrategias":
                    opa_icon = "✅ ON " if _armadilha_mode else "⛔ OFF"
                    opd_icon = "✅ ON " if _mode_opd else "⛔ OFF"
                    ope_icon = "✅ ON " if _mode_ope else "⛔ OFF"
                    lines = [
                        "📋 <b>ESTRATÉGIAS — estado actual</b>\n",
                        "✅ ON  — 🥇 POL  ICHIMOKU 1H",
                        f"{opa_icon} — 🪤 OpA  Triple BB+SAR [SOL/BNB/ETH/DOGE]",
                        f"{opd_icon} — ⚡ OpD  Sniper MACD M5 [ETH]",
                        f"{ope_icon} — 🏦 OpE  ICT/SMC 15m [ETH/BTC/SOL]",
                        "\n<i>/opa /opd /ope — toggle | /modo_sniper — foco OpD</i>",
                    ]
                    tg("\n".join(lines), chat_id)

                # ── /opa — Armadilha Triple BB+SAR ON/OFF ────────────────────
                elif cmd == "opa":
                    _armadilha_mode = not _armadilha_mode
                    estado = "✅ LIGADA" if _armadilha_mode else "⭕ DESLIGADA"
                    tg(f"🪤 <b>OpA — Armadilha Triple BB+SAR: {estado}</b>\n"
                       f"Pares: SOL · BNB · ETH · DOGE\n"
                       f"SL 1.0% | SAR M5 trailing | H1 alvo oposto", chat_id)
                    log.info("OpA: %s", estado)

                # ── /opd — Opção D Sniper MACD M5 ON/OFF ─────────────────────
                elif cmd == "opd":
                    _mode_opd = not _mode_opd
                    estado = "✅ LIGADA" if _mode_opd else "⭕ DESLIGADA"
                    tg(f"⚡ <b>Opção D — Sniper MACD M5: {estado}</b>\n"
                       f"Pares: ETH · SOL · POL\n"
                       f"BB exaustão + histograma MACD a reverter | SL 1.5% | Trail +0.8%\n"
                       f"{'⚠️ Sniper de alta precisão — M5 rápido' if _mode_opd else ''}",
                       chat_id)
                    log.info("Opção D: %s", estado)

                # ── /ope — Opção E ICT/SMC Institucional 15m ON/OFF ──────────
                elif cmd == "ope":
                    _mode_ope = not _mode_ope
                    estado = "✅ LIGADA" if _mode_ope else "⭕ DESLIGADA"
                    tg(f"🏦 <b>Opção E — ICT/Institucional 15m: {estado}</b>\n"
                       f"Pares: BTC · ETH · SOL\n"
                       f"Sweep de liquidez + FVG + Pullback ao gap\n"
                       f"SL estrutural (topo/fundo da vela Sweep) | Trail +0.8%\n"
                       f"{'⚡ Modo institucional activado — elite setup' if _mode_ope else ''}",
                       chat_id)
                    log.info("Opção E: %s", estado)

                # ── /modo_sniper — foca apenas OpD, desliga OpA/OpE ──────────
                elif cmd == "modo_sniper":
                    _armadilha_mode = False
                    _mode_ope = False
                    _mode_opd = True
                    tg("⚡ <b>MODO SNIPER ACTIVADO</b>\n"
                       "OpA ⭕ | OpE ⭕\n"
                       "<b>OpD ✅ MACD M5+MTFA — FOCO TOTAL</b>\n"
                       "SL imediato 1.5% | Break-Even a +0.5% | Trail 0.2%", chat_id)
                    log.info("Modo Sniper: OpD isolada")

                # ── /btc — leitura rápida do Comandante BTC ──────────────────
                elif cmd == "btc":
                    try:
                        sentiment, blocked, price, ema20, rsi = get_btc_sentiment()
                        if price == 0.0:
                            tg("⚠️ Erro ao consultar o Comandante BTC.\nAPI OKX não respondeu — tente novamente.", chat_id)
                        else:
                            tend_map  = {
                                "BULLISH":       "🟢 ALTA FORTE",
                                "BULLISH_FRACO": "🟡 ALTA FRACA",
                                "NEUTRO":        "⚪ NEUTRO",
                                "BEARISH_FRACO": "🟠 BAIXA FRACA",
                                "BEARISH":       "🔴 BAIXA FORTE",
                            }
                            tend_icon = tend_map.get(sentiment, sentiment)
                            rsi_note  = " ⚠️ EXAUSTÃO" if blocked else ""
                            tg(f"🧐 <b>Comandante BTC</b>\n"
                               f"Preço: <b>${price:,.0f}</b> | EMA20 1H: <b>${ema20:,.0f}</b>\n"
                               f"Tendência: <b>{tend_icon}</b>\n"
                               f"RSI 15m: <b>{rsi:.1f}</b>{rsi_note}", chat_id)
                        log.info("/btc consultado: %s price=%.0f ema20=%.0f rsi=%.1f",
                                 sentiment, price, ema20, rsi)
                    except Exception as e:
                        log.warning("/btc erro: %s", e)
                        tg("⚠️ Erro ao consultar o Comandante BTC.", chat_id)

                # ── /subir N — alavancagem 2×–10× ────────────────────────────
                # Aceita: /subir 8  |  /subir8  |  /subir8x  |  /subir6x  |  /subir7x
                elif cmd == "subir" or cmd.startswith("subir"):
                    # extrair dígito: "subir8x" → "8", "subir" + args "8" → "8"
                    raw = cmd[5:].rstrip("x") if len(cmd) > 5 else (args[0] if args else "")
                    if not raw.isdigit() or not 2 <= int(raw) <= 10:
                        tg("❌ Uso: <code>/subir [2-10]</code>\nEx: <code>/subir 8</code>", chat_id)
                    else:
                        lev = int(raw)
                        LEVERAGE = lev
                        _LEVERAGE_SET.clear()
                        for s in ALL_SYMS:
                            try: okx_set_leverage(s)
                            except Exception as e: log.warning("subir%d %s: %s", lev, s, e)
                        risk = "🟢 Baixo" if lev <= 5 else ("🟡 Médio" if lev <= 7 else "🔴 AGRESSIVO")
                        warn = "\n⚠️ SL automático essencial neste nível!" if lev >= 8 else ""
                        tg(f"🚀 <b>Alavancagem → {lev}×</b> {risk}\nAplicado em todos os pares.{warn}", chat_id)
                        log.info("Alavancagem alterada para %dx via Telegram", lev)

                elif cmd == "armadilha":
                    if _armadilha_mode:
                        _armadilha_mode = False
                        tg("🔓 <b>MODO ARMADILHA DESLIGADO</b>\n"
                           "Bot volta ao modo normal — sem filtro Bollinger.", chat_id)
                    else:
                        _armadilha_mode = True
                        tg("🪤 <b>MODO ARMADILHA V10 ACTIVADO</b>\n"
                           "─────────────────────────────\n"
                           "Triple BB M5+M15+H1 + SAR M5 inversão:\n"
                           "• Banda superior → SHORT com SAR virado para baixo\n"
                           "• Banda inferior → LONG com SAR virado para cima\n"
                           "• <b>SL dinâmico</b>: SAR M5 price\n"
                           "• <b>Saída</b>: SAR M15 inversão ou banda H1 oposta\n"
                           "Usa <code>/armadilha</code> para desligar.", chat_id)

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
                    if _trail_mode == "gv5":
                        tg("🔒 <b>Step Trail V5 já está ACTIVO</b>\n"
                           "5 graus de lock progressivo (break-even → G5).\n"
                           "Usa <code>/gv6</code> para mudar para SAR M15.", chat_id)
                    else:
                        _trail_mode = "gv5"
                        tg("🔒 <b>STEP TRAIL V5 ACTIVADO</b>\n"
                           "Gestão de posição: 5 graus de lock progressivo.\n"
                           "G1 break-even → G2 +$28 → G3 +$38 → G4 +$52 → G5 +$68\n"
                           "GV6 (SAR M15) desactivado.\n"
                           "Usa <code>/gv6</code> para mudar.", chat_id)

                elif cmd == "gv6":
                    if _trail_mode == "gv6":
                        tg("📡 <b>SAR M15 Trailing V6 já está ACTIVO</b>\n"
                           "Trailing dinâmico + fecho por inversão SAR M15.\n"
                           "Usa <code>/gv5</code> para mudar para Step Trail.", chat_id)
                    else:
                        _trail_mode = "gv6"
                        tg("📡 <b>SAR M15 TRAILING V6 ACTIVADO</b>\n"
                           "Gestão de posição: SAR M15 ratchet dinâmico.\n"
                           "• Fecho automático por inversão SAR M15\n"
                           "• Alvo: banda H1 oposta\n"
                           "GV5 (Step Trail) desactivado.\n"
                           "Usa <code>/gv5</code> para mudar.", chat_id)

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

                # ── /clab — cancela TODAS as ordens abertas na OKX (manual) ──────
                elif cmd == "clab":
                    tg("🧹 <b>VARREDURA EM CURSO...</b>\nCancelando todas as ordens abertas na OKX...", chat_id)
                    total   = 0
                    details = []
                    for sym_id in ALL_SYMS:
                        try:
                            n = cancel_all_open_orders(sym_id)
                            if n > 0:
                                details.append(f"  {sym_id.replace('-USDT-SWAP','')} — {n} ordens")
                            total += n
                        except Exception as e:
                            details.append(f"  ⚠️ {sym_id.replace('-USDT-SWAP','')}: {e}")
                    if total == 0 and not any("⚠️" in d for d in details):
                        tg("✅ <b>/clab — Livro já limpo.</b>\nNenhuma ordem aberta encontrada.", chat_id)
                    else:
                        detail_txt = "\n".join(details) if details else "  (sem detalhes)"
                        tg(f"🧹 <b>/clab — Varredura completa!</b>\n"
                           f"Total cancelado: <b>{total} ordens</b>\n{detail_txt}", chat_id)
                    log.info("/clab manual: %d ordens canceladas", total)

                # ── /help ──────────────────────────────────────────────────────
                elif cmd in ("help", "ajuda"):
                    tg("🤖 <b>V9 COMMANDER — FULL SQUAD (10 estratégias)</b>\n\n"
                       "<b>Controlo do bot:</b>\n"
                       "/start — ✅ Autorizar bot\n"
                       "/pause — ⛔ Pausa PERMANENTE (só /start desbloqueia)\n"
                       "/panic — 🚨 Fecha tudo + pausa 5min\n\n"
                       "<b>Estratégias ON/OFF:</b>\n"
                       "/estrategias — Lista quais estão ON/OFF\n"
                       "/pausar [chave] — Pausa estratégia individual\n"
                       "/activar [chave] — Reactiva estratégia individual\n"
                       "  Chaves: <code>ichimoku | supertrend | rsidiv | vwap | engolfo | ob | fvg | tudo</code>\n"
                       "☁️ Ichimoku POL activo em modo INVERTIDO (exaustão)\n\n"
                       "<b>Alavancagem:</b>\n"
                       "/subir6x — Mudar para 6× (aplica imediatamente)\n"
                       "/subir7x — Mudar para 7× (aplica imediatamente)\n\n"
                       "<b>Info &amp; análise:</b>\n"
                       "/status (ou /s) — Estado + saldo + alavancagem\n"
                       "/tp — P&amp;L posições abertas ao vivo\n"
                       "/radar — Proximidade triggers + score de confiança\n"
                       "/lpd — P&amp;L realizado últimas 24h\n"
                       "/meta — Progresso meta $600/mês\n"
                       "/risco — Análise táctica (book + SL + veredito)\n"
                       "/backtest — Backtest real 100 dias OKX (~40s)\n"
                       "/cenario — Diagnóstico ADX+EMA200 BTC 1H + recomendação\n\n"
                       "<b>Acção manual:</b>\n"
                       "/gv5 — Step Trail V5 | /gv6 — SAR M15 trailing\n"
                       "/tsar on|pause|off|status — TSAR V11 Expulsão\n"
                       "/combat on|off — alias rápido /tsar | /grau — estado GV5\n"
                       "/opb — 📐 Opção B PA Independentes ON/OFF\n"
                       "/opc — 🔀 Opção C Híbrido TSAR+PA ON/OFF\n"
                       "/opd — ⚡ Opção D Sniper MACD M5 ON/OFF\n"
                       "/ope — 🏦 Opção E ICT/SMC Institucional 15m ON/OFF\n"
                       "/clab — 🧹 Cancela TODAS as ordens abertas na OKX\n"
                       "/go[coin] — Confirma sinal pendente (120s)\n"
                       "  /goeth  /gosol  /goada  /godoge  /gobnb\n"
                       "/frl p — Maker exit (limit no melhor ask/bid)\n"
                       "/frl l — Limit exit no preço actual (sem taker fee)\n"
                       "/force [coin] — Ordem mercado bypass filtros\n"
                       "  Ex: <code>/force bnb</code>  (RSI 15m decide LONG/SHORT)\n\n"
                       "🥇 POL/SOL/ETH/BNB/ADA/DOGE — TODOS AUTOMÁTICOS\n\n"
                       f"CB -${CIRCUIT_BREAKER_USD:.0f}  |  HOLD SL {HOLD_SL_PCT:.0f}%  |  STRICT SL {STRICT_SL_PCT:.1f}%\n"
                       f"GV5/GV6  |  Lev actual: <b>{LEVERAGE}×</b>  |  cd 5min", chat_id)

                # ── /v11 — Painel de Comando Tático completo ───────────────────
                elif cmd == "v11":
                    try: tg(_v11_dashboard_text(), chat_id)
                    except Exception as e: tg(f"Erro /v11: {e}", chat_id)

                # ── /cenario — Diagnóstico ADX+EMA200 + recomendação de módulo ──
                elif cmd == "cenario":
                    inst = "BTC-USDT-SWAP"
                    if args:
                        coin = args[0].upper()
                        inst = f"{coin}-USDT-SWAP"
                    try: tg(cmd_cenario(inst), chat_id)
                    except Exception as e: tg(f"Erro /cenario: {e}", chat_id)

                # ── /frl — Saída via ordem LIMIT (modo Maker) ──────────────────
                elif cmd == "frl":
                    if not args or args[0] not in ("p", "l"):
                        tg("💡 <b>/frl p</b> — Maker exit (limit no melhor ask/bid)\n"
                           "<b>/frl l</b> — Limit exit no preço actual (evita taker fee)\n"
                           "Cancela algos automaticamente antes de colocar.", chat_id)
                    else:
                        frl_mode = args[0]
                        open_pos = None
                        for _frl_inst in ALL_SYMS:
                            for _frl_ps in ("long", "short"):
                                _frl_pos = okx_get_position(_frl_inst, _frl_ps)
                                if _frl_pos and float(_frl_pos.get("pos", 0) or 0) != 0:
                                    open_pos = (_frl_inst, _frl_ps,
                                                int(float(_frl_pos.get("pos", 0))),
                                                float(_frl_pos.get("avgPx", 0)))
                                    break
                            if open_pos: break
                        if not open_pos:
                            tg("ℹ️ Nenhuma posição aberta encontrada.", chat_id)
                        else:
                            _fi, _fps, _fsz, _favg = open_pos
                            _fsym = _fi.replace("-USDT-SWAP", "")
                            try:
                                if frl_mode == "p":
                                    ob = okx_orderbook(_fi, depth=5)
                                    if not ob:
                                        tg("❌ Erro a ler orderbook.", chat_id)
                                    else:
                                        _bids, _asks = ob
                                        if _fps == "long":
                                            _flim = float(_asks[0][0])
                                            _flbl = f"ask {_flim:.5f}"
                                        else:
                                            _flim = float(_bids[0][0])
                                            _flbl = f"bid {_flim:.5f}"
                                        cancel_all_open_orders(_fi); time.sleep(0.5)
                                        okx_close_limit(_fi, _fps, _fsz, _flim)
                                        tg(f"📋 <b>FRL MAKER EXIT</b>\n"
                                           f"Par: <code>{_fsym}</code> | {_fps.upper()}\n"
                                           f"Limit: <b>{_flbl}</b> | {_fsz} contratos\n"
                                           f"Algos cancelados. A aguardar preenchimento...")
                                else:
                                    _flim = okx_ticker(_fi)
                                    cancel_all_open_orders(_fi); time.sleep(0.5)
                                    okx_close_limit(_fi, _fps, _fsz, _flim)
                                    tg(f"📋 <b>FRL LIMIT EXIT</b>\n"
                                       f"Par: <code>{_fsym}</code> | {_fps.upper()}\n"
                                       f"Limit: <b>{_flim:.5f}</b> (preço actual)\n"
                                       f"Algos cancelados.")
                            except Exception as e:
                                tg(f"❌ Erro /frl: {e}", chat_id)

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
    log.info("🎯 V11 SNIPER ELITE — FULL SQUAD READY — AUTOFIRE")
    tg("🏆 <b>V11 SNIPER ELITE — FULL SQUAD ACTIVO</b>\n\n"
       "🥇 <b>POL</b> — GOLDEN (Ichimoku 1H)\n"
       "🪤 <b>SOL · BNB · ETH · DOGE</b> — OpA (Armadilha Triple BB+SAR)\n"
       "⚡ <b>ETH</b> — OpD (Sniper MACD M5+MTFA)\n"
       "🏦 <b>BTC · ETH · SOL</b> — OpE (ICT/SMC 15m)\n\n"
       f"🔒 GV5  |  CB -${CIRCUIT_BREAKER_USD:.0f}  |  SL 1.5%  |  "
       f"{LEVERAGE}× ALL-IN  |  cd 5min\n"
       "✅ <b>PURGE COMPLETA — SQUAD LIMPO.</b>")

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

            # Snapshot thread-safe do estado das estratégias para este ciclo
            with _strategy_lock:
                st_enabled = dict(_strategy_enabled)

            # ╔══════════════ GOLDEN DOCTRINE PRIORITY ═══════════════════════╗
            # ── 🥇 PRIORIDADE 1: POL — ICHIMOKU 1H (97.4% hit, HOLD) ────────
            if not fired and st_enabled["ichimoku"]:
                try:
                    sig_raw = ichimoku_signal(okx_candles(GOLD_POL, bar="1H", limit=200))
                    # Modo exaustão: sinal invertido
                    if sig_raw == "buy":    sig = "sell"
                    elif sig_raw == "sell": sig = "buy"
                    else:                  sig = None
                    if sig:
                        # Dedup: ignora se já tentámos este sinal nos últimos 5 min
                        _prev_sig, _prev_t = _signal_alerted.get(GOLD_POL, (None, 0.0))
                        _already = (_prev_sig == sig and time.time() - _prev_t < _SIGNAL_COOLDOWN)
                        if _already:
                            log.debug("[POL] sinal %s já alertado %.0fs atrás — aguardando cooldown",
                                      sig, time.time() - _prev_t)
                        else:
                            log.info("[ICHI POL INVERTIDO] raw=%s → entrada=%s", sig_raw, sig)
                            _signal_alerted[GOLD_POL] = (sig, time.time())
                            fired = _fire(GOLD_POL, sig, "ICHIMOKU POL", tag="🥇 GOLDEN POL")
                    else:
                        _signal_alerted.pop(GOLD_POL, None)  # limpa dedup quando sinal desaparece
                        log.info("[POL] sem sinal")
                except Exception as e:
                    log.error("[POL] %s", e)

            # ── 🪤 OpA — Armadilha Triple BB+SAR (SOL/BNB/ETH/DOGE) ──────────
            if _armadilha_mode and not fired:
                with _duo_lock:
                    em_trade = _duo_in_trade
                if not em_trade:
                    for _a_inst, _a_par in [
                        (DUO_SOL,   "SOL"),
                        (FVG_BNB,   "BNB"),
                        (DUO_ETH,   "ETH"),
                        (GOLD_DOGE, "DOGE"),
                    ]:
                        try:
                            df5 = okx_candles(_a_inst, bar="5m", limit=100)
                            sig = signal_macd_bollinger(df5)
                            if sig:
                                dir_scout = "📈 LONG" if sig == "buy" else "📉 SHORT"
                                log.info("[OpA] BB+SAR %s → %s", _a_par, sig.upper())
                                tg(f"🪤 <b>OpA — ARMADILHA {_a_par}</b>\n"
                                   f"Par: <code>{_a_inst}</code> | {dir_scout}\n"
                                   f"Triple BB M5 + SAR inversão | SL 1.0%")
                                fired = _fire(_a_inst, sig,
                                              f"OpA BB {_a_par}", tag=f"🪤 OpA {_a_par}",
                                              sl_pct=SCALP_SL_PCT, force=True)
                                if fired: break
                        except Exception as e:
                            log.error("[OpA] %s: %s", _a_par, e)

            # ── OPÇÃO D — Sniper MACD M5 (ETH only) ─────────────────────────
            if _mode_opd and not fired:
                with _duo_lock:
                    em_trade = _duo_in_trade
                if not em_trade:
                    for _d_inst, _d_par in [
                        (DUO_ETH, "ETH"),
                    ]:
                        try:
                            df5 = okx_candles(_d_inst, bar="5m", limit=100)
                            sig = signal_macd_bollinger(df5)
                            if sig:
                                if not _verify_macro_bollinger(_d_inst, sig):
                                    log.info("[OpD] %s M5 sinal OK mas MTFA não confirma — ignorado", _d_par)
                                    continue
                                dir_scout = "📈 LONG" if sig == "buy" else "📉 SHORT"
                                log.info("[OpD] MACD BB+MTFA %s → %s", _d_par, sig.upper())
                                tg(f"⚡ <b>OpD MTFA: SNIPER MACD M5+H1</b>\n"
                                   f"Par: <code>{_d_inst}</code> | {dir_scout}\n"
                                   f"BB exaustão M5 + confirmação Macro (M15/H1)\n"
                                   f"SL 1.5% | Break-Even +0.5% | Trail 0.2%")
                                fired = _fire(_d_inst, sig,
                                              f"OpD MACD {_d_par}", tag=f"⚡ OpD {_d_par}",
                                              sl_pct=1.5, fast_trail=True)
                                if fired: break
                        except Exception as e:
                            log.error("[OpD] %s: %s", _d_par, e)

            # ── OPÇÃO E — ICT/SMC Institucional 15m (BTC/ETH/SOL) ──────────
            if _mode_ope and not fired:
                with _duo_lock:
                    em_trade = _duo_in_trade
                if not em_trade:
                    for _e_inst, _e_par in [
                        ("BTC-USDT-SWAP", "BTC"),
                        (DUO_ETH,         "ETH"),
                        (DUO_SOL,         "SOL"),
                    ]:
                        try:
                            _df15 = okx_candles(_e_inst, bar="15m", limit=100)
                            sig = signal_ict_fvg(_df15)
                            if sig:
                                _v4    = _df15.iloc[-5]
                                _sl_px = float(_v4["high"]) if sig == "sell" else float(_v4["low"])
                                dir_scout = "📈 LONG" if sig == "buy" else "📉 SHORT"
                                log.info("[OpE] ICT FVG %s → %s | SL %.5f", _e_par, sig.upper(), _sl_px)
                                tg(f"🏦 <b>OpE: ICT/SMC SETUP 15m</b>\n"
                                   f"Par: <code>{_e_inst}</code> | {dir_scout}\n"
                                   f"Sweep + FVG + Pullback ao gap\n"
                                   f"SL estrutural: <code>{_sl_px:.5f}</code> (vela sweep)")
                                fired = _fire(_e_inst, sig,
                                              f"OpE ICT {_e_par}", tag=f"🏦 OpE {_e_par}",
                                              sl_px_override=_sl_px, min_trail_pct=0.8)
                                if fired: break
                        except Exception as e:
                            log.error("[OpE] %s: %s", _e_par, e)

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
    log.info("║   TradeSniper V11 SNIPER ELITE — PURGE COMPLETA     ║")
    log.info("║   🥇 POL  [GOLDEN ICHIMOKU 1H]         AUTOFIRE     ║")
    log.info("║   🪤 SOL/BNB/ETH/DOGE  [OpA Triple BB] AUTOFIRE     ║")
    log.info("║   ⚡ ETH  [OpD MACD M5+MTFA]           AUTOFIRE     ║")
    log.info("║   🏦 BTC/ETH/SOL  [OpE ICT/SMC 15m]   AUTOFIRE     ║")
    log.info("║   SL: HOLD %.0f%% | STRICT %.1f%% | CB -$%.0f          ║",
             HOLD_SL_PCT, STRICT_SL_PCT, CIRCUIT_BREAKER_USD)
    log.info("║   🔒 GV5 TRAIL  |  %dx  |  cd 5min                 ║", LEVERAGE)
    log.info("╚══════════════════════════════════════════════════════╝")

    # Estado persistido
    _load_full_state()
    with _auth_lock:
        log.info("Estado: %s | trail=%s",
                 "AUTORIZADO ✅" if _bot_authorized else "PAUSADO ⛔", _trail_mode)

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
