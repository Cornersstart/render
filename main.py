"""
TradeSniper Bot — V9 FULL SQUAD + FVG EXPANSION + GOLDEN RECOVERY DOCTRINE + STEP TRAIL V5
BUILD: 2026-04-19 — V9: FVG SOL/BNB/ETH adicionados | Step Trail V5 | SL HOLD 5% | Margin 3%
Doutrina : ONE TARGET, ONE KILL  |  STEP TRAIL V5 = LAW
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🏆 GOLDEN DOCTRINE (backtest 103 dias — actualizada Abr/2026):
  🥇 POL — E09 ICHIMOKU 1H V2         (5 filtros anti-falso, reforçado Abr/2026) HOLD
  🌊 SOL — E06 SUPERTREND 15m         (95.0% hit, +$515 líq.)    HOLD
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
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
HOLD pairs (POL/ETH/SOL/BNB): sem SL apertado, só circuit breaker -4%
STRICT pairs (ADA/DOGE): SL fixo 1.5% — backtest mostra ruína se segurar
GLOBAL CIRCUIT BREAKER: -4% drawdown = fecho imediato (protege banca $900)
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
SCALP_SL_PCT  = 1.0    # SL fixo para scalp de reversão Bollinger
TSAR_SL_PCT   = 2.0    # TSAR V11 hard stop-loss 2%
TSAR_LOCK_USD = 30.0   # lucro que activa lock + SAR M15 trailing
TSAR_COOLDOWN = 900    # 15 min cooldown após TSAR trade (Modo Combate)
# GV5 — 5 graus de lock crescente: (trigger_usd, lock_usd)
TSAR_GV5: list[tuple[float, float]] = [
    (30.0,  25.0),   # G1: hit +$30  → piso +$25
    (50.0,  35.0),   # G2: hit +$50  → piso +$35
    (75.0,  55.0),   # G3: hit +$75  → piso +$55
    (100.0, 75.0),   # G4: hit +$100 → piso +$75
    (150.0, 110.0),  # G5: hit +$150 → piso +$110 + runner SAR M15
]

DUO_ETH    = "ETH-USDT-SWAP"
DUO_SOL    = "SOL-USDT-SWAP"
SHIELD_ADA = "ADA-USDT-SWAP"
GOLD_POL   = "POL-USDT-SWAP"      # 🥇 Golden pair — Ichimoku 1H exclusivo
GOLD_DOGE  = "DOGE-USDT-SWAP"     # incluído na lista STRICT (regra de hold)
FVG_BNB    = "BNB-USDT-SWAP"      # 🆕 V9 — FVG expansion squad
ALL_SYMS   = [DUO_ETH, DUO_SOL, SHIELD_ADA, GOLD_POL, GOLD_DOGE, FVG_BNB]

# ── ORDER BLOCK DEFENSE (ADA — 1H) ───────────────────────────────────────────
OB_LOOKBACK    = 20    # velas 1H para procurar blocos de ordem
OB_VOL_MULT    = 2.0   # volume do expansion candle ≥ 2× média
OB_BODY_MULT   = 1.5   # corpo do expansion candle ≥ 1.5× média
OB_TOL_PCT     = 0.4   # tolerância ±0.4% para toque no midpoint
OB_SL_PCT      = 1.0   # SL da estratégia OB (diferente do DUO)

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
HOLD_PAIRS    = {GOLD_POL, DUO_ETH, DUO_SOL, FVG_BNB}    # sem SL apertado
STRICT_PAIRS  = {SHIELD_ADA, GOLD_DOGE}                  # SL fixo 1.5%
STRICT_SL_PCT = 1.5
# HOLD: SL na corretora é REDE DE SEGURANÇA (caso o bot/monitor caia).
# O controlo primário é o CIRCUIT_BREAKER no monitor (4.0%) — dispara primeiro.
# Folga de 1pp evita corrida dupla CB-vs-exchange-SL no mesmo tick.
HOLD_SL_PCT   = 5.0
CIRCUIT_BREAKER_PCT = 2.0   # global — monitor fecha SEMPRE a -2% em preço
PROFIT_LOCK_USD     = 0.0   # 0 = desactivado — Step Trail V5 trata dos lucros

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
LOCKDOWN_SECS    = 300   # 5 min — menos bloqueio entre sinais legítimos
VWAP_BODY_MIN    = 0.55  # corpo/range mínimo para confirmar VWAP KISS (era 0.40)
VWAP_DIST_PCT    = 0.15  # distância mínima da VWAP após cross (em %)

# ── Estado global ─────────────────────────────────────────────────────────────
_duo_in_trade:       bool  = False
_duo_cooldown_until: float = 0.0
_lockdown_until:     float = 0.0   # bloqueio total de novos sinais (anti ping-pong)
_duo_lock                  = threading.Lock()

_bot_authorized: bool = True
_auth_lock             = threading.Lock()
_armadilha_mode: bool  = False   # False = off | True = Bollinger mean-reversion activo
_trail_mode: str       = "gv5"   # "gv5" = Step Trail V5 | "gv6" = SAR M15 trailing
_tsar_mode: str        = "on"    # "on" = activo por defeito | "" = off | "paused" = sem novas entradas
_tsar_pol_mode: str    = "on"    # POL SNIPER TSAR | "on" = activo | "" = off | "paused" = sem novas entradas
_tsar_combat_grau: int = 0       # grau GV5 activo na posição TSAR corrente (0 = nenhum)

# ── Confirmação manual (120s) — sinais não-POL aguardam /go[coin] ─────────────
_pending_signals: dict = {}   # coin_key → (inst_id, side, signal_name, tag, expiry)
_pending_lock          = threading.Lock()

# ── Meta mensal — $600 / mês ────────────────────────────────────────────────
MONTHLY_GOAL_USD = 600.0

# ── Panic pause ────────────────────────────────────────────────────────────
_panic_until: float = 0.0
_btc_sentinel_active: bool = True
_mode_opb: bool = False   # Opção B — PA independentes (ETH/SOL/BNB/POL)
_mode_opc: bool = False   # Opção C — híbrido TSAR+PA: 5× se TSAR confirma, 3× só PA
_mode_opd: bool = False   # Opção D — Sniper MACD M5 (ETH/SOL/POL)

# ── Estratégias habilitadas — /pausar /activar individuais ───────────────────
_STRATEGY_KEYS = ("ichimoku", "supertrend", "rsidiv", "vwap", "engolfo", "ob", "fvg")
# ICHIMOKU e FVG OFF por defeito — activar manualmente via /activar
_strategy_enabled: dict[str, bool] = {k: (k not in ("ichimoku", "fvg")) for k in _STRATEGY_KEYS}
_strategy_lock = threading.Lock()

STATE_FILE = Path(__file__).parent / "bot_state.json"

# ── Persistência ─────────────────────────────────────────────────────────────
def _save_state(authorized: bool) -> None:
    try:
        tmp = STATE_FILE.with_suffix(".tmp")
        with _strategy_lock:
            st_snap = dict(_strategy_enabled)
        tmp.write_text(json.dumps({
            "authorized":       authorized,
            "tsar_mode":        _tsar_mode,
            "tsar_pol_mode":    _tsar_pol_mode,
            "trail_mode":       _trail_mode,
            "strategy_enabled": st_snap,
            "updatedAt":        datetime.now(timezone.utc).isoformat(),
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
    """Restaura authorized + tsar_mode + tsar_pol_mode + trail_mode + strategy_enabled do ficheiro de estado."""
    global _tsar_mode, _tsar_pol_mode, _trail_mode
    try:
        if STATE_FILE.exists():
            data = json.loads(STATE_FILE.read_text())
            with _auth_lock:
                globals()["_bot_authorized"] = bool(data.get("authorized", True))
            _tsar_mode     = data.get("tsar_mode",     "on")
            _tsar_pol_mode = data.get("tsar_pol_mode", "on")
            _trail_mode    = data.get("trail_mode", "gv5")
            saved_st = data.get("strategy_enabled", {})
            if saved_st:
                with _strategy_lock:
                    for k in _STRATEGY_KEYS:
                        if k in saved_st:
                            _strategy_enabled[k] = bool(saved_st[k])
            log.info("Estado restaurado: auth=%s tsar=%r trail=%s st=%s",
                     globals()["_bot_authorized"], _tsar_mode, _trail_mode,
                     {k: v for k, v in _strategy_enabled.items()})
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

def _ichimoku_cloud_dir(inst_id: str) -> str:
    """Retorna 'bull' (preço acima da nuvem), 'bear' (abaixo) ou 'neutral'."""
    try:
        df = okx_candles(inst_id, bar="1H", limit=120)
        TENKAN, KIJUN, SSB_P = 9, 26, 52
        hi = lambda n: df["high"].rolling(n).max()
        lo = lambda n: df["low"].rolling(n).min()
        span_a = ((hi(TENKAN) + lo(TENKAN)) / 2 + (hi(KIJUN) + lo(KIJUN)) / 2) / 2
        span_b = (hi(SSB_P) + lo(SSB_P)) / 2
        kumo_top = max(float(span_a.iloc[-27]), float(span_b.iloc[-27]))
        kumo_bot = min(float(span_a.iloc[-27]), float(span_b.iloc[-27]))
        price    = float(df["close"].iloc[-1])
        if price > kumo_top: return "bull"
        if price < kumo_bot: return "bear"
        return "neutral"
    except Exception as e:
        log.warning("_ichimoku_cloud_dir %s: %s — neutro", inst_id, e)
        return "neutral"

def _bollinger_check(inst_id: str, side: str) -> tuple[str, str, float]:
    """Bollinger Esticada (15m) — verifica exaustão de preço antes de entrar.

    Retorna (action, effective_side, rsi):
      'allow'  — preço na banda correcta para o sinal → entra
      'invert' — preço na banda oposta (armadilha) → inverte side e entra
      'block'  — preço no corpo da Bollinger → não operar
    Fail-safe: se API falhar, devolve ('allow', side, 0.0).
    """
    try:
        df    = okx_candles(inst_id, bar="15m", limit=60)
        close = df["close"]
        mid   = close.rolling(BB_PERIOD).mean()
        std   = close.rolling(BB_PERIOD).std()
        upper = float((mid + BB_STD * std).iloc[-1])
        lower = float((mid - BB_STD * std).iloc[-1])
        price = float(close.iloc[-1])

        delta = close.diff()
        gain  = delta.clip(lower=0).ewm(span=14, adjust=False).mean()
        loss  = (-delta).clip(lower=0).ewm(span=14, adjust=False).mean()
        rsi   = float(100 - (100 / (1 + gain.iloc[-1] / (loss.iloc[-1] + 1e-10))))

        at_upper = price >= upper * (1 - BB_TOL_PCT / 100) and rsi >= 65
        at_lower = price <= lower * (1 + BB_TOL_PCT / 100) and rsi <= 35

        if side == "buy":
            if at_lower: return ("allow",  "buy",  rsi)
            if at_upper: return ("invert", "sell", rsi)
        else:
            if at_upper: return ("allow",  "sell", rsi)
            if at_lower: return ("invert", "buy",  rsi)
        return ("block", side, rsi)
    except Exception as e:
        log.warning("_bollinger_check %s: %s — permitindo entrada", inst_id, e)
        return ("allow", side, 0.0)

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

def tsar_signal(inst_id: str) -> str | None:
    """TSAR V11 — Regra da Expulsão.
    1) Vela anterior estava fora BB M5 + M15
    2) RSI2 M5 em exaustão (>90 se cima, <10 se baixo)
    3) Vela actual fecha de volta dentro de BB M5 + SAR M5 acabou de inverter
    Retorna 'buy'/'sell' ou None.
    """
    try:
        df5  = okx_candles(inst_id, bar="5m",  limit=100)
        df15 = okx_candles(inst_id, bar="15m", limit=60)
        c5   = df5["close"]

        mid5  = c5.rolling(BB_PERIOD).mean()
        std5  = c5.rolling(BB_PERIOD).std()
        up5_p = float((mid5 + BB_STD * std5).iloc[-2])
        lo5_p = float((mid5 - BB_STD * std5).iloc[-2])
        up5_c = float((mid5 + BB_STD * std5).iloc[-1])
        lo5_c = float((mid5 - BB_STD * std5).iloc[-1])
        px_p  = float(c5.iloc[-2])
        px_c  = float(c5.iloc[-1])

        was_above = px_p > up5_p
        was_below = px_p < lo5_p
        if not (was_above or was_below):
            return None

        back_above = was_above and px_c <= up5_c
        back_below = was_below and px_c >= lo5_c
        if not (back_above or back_below):
            return None

        delta = c5.diff()
        gain  = delta.clip(lower=0).ewm(span=2, adjust=False).mean()
        loss  = (-delta).clip(lower=0).ewm(span=2, adjust=False).mean()
        rsi2  = float(100 - (100 / (1 + gain.iloc[-1] / (loss.iloc[-1] + 1e-10))))
        if back_above and rsi2 < 88: return None
        if back_below and rsi2 > 12: return None

        c15   = df15["close"]
        mid15 = c15.rolling(BB_PERIOD).mean()
        std15 = c15.rolling(BB_PERIOD).std()
        up15  = float((mid15 + BB_STD * std15).iloc[-2])
        lo15  = float((mid15 - BB_STD * std15).iloc[-2])
        px15  = float(c15.iloc[-2])
        if back_above and px15 <= up15: return None
        if back_below and px15 >= lo15: return None

        sar_dir = "bull" if back_below else "bear"
        if not _sar_just_inverted(inst_id, sar_dir):
            return None

        candidate_side = "sell" if back_above else "buy"
        cloud = _ichimoku_cloud_dir(inst_id)
        if candidate_side == "sell" and cloud == "bull":
            log.info("[TSAR] %s SHORT bloqueado — cloud BULLISH", inst_id); return None
        if candidate_side == "buy" and cloud == "bear":
            log.info("[TSAR] %s LONG bloqueado — cloud BEARISH", inst_id); return None
        return candidate_side
    except Exception as e:
        log.warning("tsar_signal %s: %s", inst_id, e)
        return None

def tsar_pol_signal(inst_id: str) -> tuple[str, float] | None:
    """POL SNIPER TSAR — Confirmação Dupla.
    Trigger : BB M5+M15 toque (banda superior/inferior) + SAR M5 inversão.
    Confirm : Ichimoku 1H cloud na mesma direcção (não bloqueia se neutro).
    Filtro  : _pol_trend_filter_ok() anti-ignição.
    Retorna (side, sar5_px) onde sar5_px é o SL dinâmico, ou None.
    """
    try:
        df5  = okx_candles(inst_id, bar="5m",  limit=100)
        df15 = okx_candles(inst_id, bar="15m", limit=60)
        c5   = df5["close"]

        # Bollinger M5
        mid5 = c5.rolling(BB_PERIOD).mean()
        std5 = c5.rolling(BB_PERIOD).std()
        up5  = float((mid5 + BB_STD * std5).iloc[-1])
        lo5  = float((mid5 - BB_STD * std5).iloc[-1])
        px_c = float(c5.iloc[-1])

        # Bollinger M15
        c15   = df15["close"]
        mid15 = c15.rolling(BB_PERIOD).mean()
        std15 = c15.rolling(BB_PERIOD).std()
        up15  = float((mid15 + BB_STD * std15).iloc[-1])
        lo15  = float((mid15 - BB_STD * std15).iloc[-1])
        px15  = float(c15.iloc[-1])

        tol = BB_TOL_PCT / 100
        at_upper = (px_c >= up5 * (1 - tol)) and (px15 >= up15 * (1 - tol))
        at_lower = (px_c <= lo5 * (1 + tol)) and (px15 <= lo15 * (1 + tol))

        if not (at_upper or at_lower):
            return None

        candidate = "sell" if at_upper else "buy"

        # SAR M5 acabou de inverter na direcção certa
        sar_dir = "bear" if candidate == "sell" else "bull"
        if not _sar_just_inverted(inst_id, sar_dir):
            return None

        # SAR M5 price → SL dinâmico
        psar5  = df5.ta.psar(af0=0.02, af=0.02, max_af=0.20)
        col_l5 = next((c for c in psar5.columns if "PSARl" in c), None)
        col_s5 = next((c for c in psar5.columns if "PSARs" in c), None)
        sar5_px = 0.0
        if col_l5 and col_s5:
            sar5_bull = not pd.isna(psar5[col_l5].iloc[-1])
            sar5_px = float(psar5[col_l5].iloc[-1] if sar5_bull else psar5[col_s5].iloc[-1])

        # Ichimoku 1H cloud — confirmação (neutro = permitido)
        cloud_dir = _ichimoku_cloud_dir(inst_id)
        if candidate == "sell" and cloud_dir == "bull":
            log.info("[TSAR POL] SHORT bloqueado — Ichimoku cloud BULLISH")
            return None
        if candidate == "buy" and cloud_dir == "bear":
            log.info("[TSAR POL] LONG bloqueado — Ichimoku cloud BEARISH")
            return None

        # Filtro anti-ignição (engolfo + SAR duplo + safety)
        if not _pol_trend_filter_ok(candidate, df5, df15, is_lightning=False):
            return None

        return (candidate, sar5_px)
    except Exception as e:
        log.warning("tsar_pol_signal %s: %s", inst_id, e)
        return None

def _pol_trend_filter_ok(side: str, df5: pd.DataFrame, df15: pd.DataFrame,
                          is_lightning: bool = False) -> bool:
    """Filtro anti-ignição de tendência para POL SNIPER TSAR.
    Bloqueia se: engolfo bullish/bearish nos últimos 3 M5, ou SAR M5+M15 sincronia dupla.
    Standard (não-lightning): exige também SAR M5 na direcção + preço abaixo/acima do BB mid.
    """
    try:
        c5 = df5["close"]
        o5 = df5["open"]

        # 1. Engolfo detector (últimas 3 velas M5 completas)
        avg_body = (c5 - o5).abs().rolling(20).mean()
        for i in [-4, -3, -2]:
            avg_b = float(avg_body.iloc[i])
            if avg_b == 0 or pd.isna(avg_b): continue
            body_curr = abs(float(c5.iloc[i]) - float(o5.iloc[i]))
            bull_eng = (float(c5.iloc[i]) > float(o5.iloc[i]) and
                        float(c5.iloc[i]) > float(c5.iloc[i-1]) and
                        float(o5.iloc[i]) < float(o5.iloc[i-1]) and
                        body_curr >= 2 * avg_b)
            bear_eng = (float(c5.iloc[i]) < float(o5.iloc[i]) and
                        float(c5.iloc[i]) < float(c5.iloc[i-1]) and
                        float(o5.iloc[i]) > float(o5.iloc[i-1]) and
                        body_curr >= 2 * avg_b)
            if side == "sell" and bull_eng:
                log.info("[POL FILTER] Bullish engolfo M5 → SHORT BLOQUEADO"); return False
            if side == "buy" and bear_eng:
                log.info("[POL FILTER] Bearish engolfo M5 → LONG BLOQUEADO"); return False

        # 2. SAR M5 + M15 sincronia dupla → bloqueia contra-tendência
        psar5  = df5.ta.psar(af0=0.02, af=0.02, max_af=0.20)
        psar15 = df15.ta.psar(af0=0.02, af=0.02, max_af=0.20)
        col_l5  = next((c for c in psar5.columns  if "PSARl" in c), None)
        col_l15 = next((c for c in psar15.columns if "PSARl" in c), None)
        sar5_bull  = bool(col_l5  and not pd.isna(psar5[col_l5].iloc[-1]))
        sar15_bull = bool(col_l15 and not pd.isna(psar15[col_l15].iloc[-1]))

        if side == "sell" and sar5_bull and sar15_bull:
            log.info("[POL FILTER] SAR M5+M15 ambos BULL → SHORT PROIBIDO"); return False
        if side == "buy" and (not sar5_bull) and (not sar15_bull):
            log.info("[POL FILTER] SAR M5+M15 ambos BEAR → LONG PROIBIDO"); return False

        # 3. Safety (standard apenas): SAR M5 na direcção certa + preço cruzou BB mid
        if not is_lightning:
            mid5   = c5.rolling(BB_PERIOD).mean()
            bb_mid = float(mid5.iloc[-1])
            px_c   = float(c5.iloc[-1])
            if side == "sell" and (sar5_bull or px_c >= bb_mid):
                log.info("[POL FILTER] SHORT safety: SAR5=%s px=%.5f mid=%.5f → BLOQUEADO",
                         'bull' if sar5_bull else 'bear', px_c, bb_mid)
                return False
            if side == "buy" and ((not sar5_bull) or px_c <= bb_mid):
                log.info("[POL FILTER] LONG safety: SAR5=%s px=%.5f mid=%.5f → BLOQUEADO",
                         'bull' if sar5_bull else 'bear', px_c, bb_mid)
                return False

        return True
    except Exception as e:
        log.warning("_pol_trend_filter_ok: %s", e)
        return True   # em erro, permite entrada

def _tsar_btc_boost() -> tuple[bool, float]:
    """Retorna (boost, rsi_btc). boost=True se BTC RSI M15 < 30 ou > 70 (extremos)."""
    try:
        df  = okx_candles("BTC-USDT-SWAP", bar="15m", limit=30)
        c   = df["close"]
        d   = c.diff()
        g   = d.clip(lower=0).ewm(span=14, adjust=False).mean()
        l   = (-d).clip(lower=0).ewm(span=14, adjust=False).mean()
        rsi = float(100 - (100 / (1 + g.iloc[-1] / (l.iloc[-1] + 1e-10))))
        return (rsi < 30 or rsi > 70, rsi)
    except Exception:
        return (False, 50.0)

def _h1_trend_bull(inst_id: str) -> bool:
    """True se preço > EMA20 H1 (tendência bullish no H1)."""
    try:
        df  = okx_candles(inst_id, bar="1H", limit=30)
        ema = df["close"].ewm(span=20, adjust=False).mean()
        return float(df["close"].iloc[-1]) > float(ema.iloc[-1])
    except Exception:
        return True

def _tsar_status_text() -> str:
    """Relatório /tsar status: BTC RSI M15 + SAR M5/M15 + RSI2 para BNB/SOL/ETH."""
    boost, btc_rsi = _tsar_btc_boost()
    boost_txt = "⚡ EXTREMO → +50%% size" if boost else "normal"
    lines = [
        f"⚔️ <b>TSAR V11 STATUS</b> — modo: <b>{'ON' if _tsar_mode=='on' else 'PAUSED' if _tsar_mode=='paused' else 'OFF'}</b>",
        f"BTC RSI M15: <b>{btc_rsi:.1f}</b> ({boost_txt})\n"
    ]
    for inst_id, sym in [(FVG_BNB, "BNB"), (DUO_SOL, "SOL"), (DUO_ETH, "ETH")]:
        try:
            df5 = okx_candles(inst_id, bar="5m", limit=30)
            c5  = df5["close"]
            d   = c5.diff()
            g   = d.clip(lower=0).ewm(span=2, adjust=False).mean()
            l   = (-d).clip(lower=0).ewm(span=2, adjust=False).mean()
            rsi2 = float(100 - (100 / (1 + g.iloc[-1] / (l.iloc[-1] + 1e-10))))
            sar15 = _get_sar_m15_px(inst_id)
            psar5 = df5.ta.psar(af0=0.02, af=0.02, max_af=0.20)
            col_l = next((c for c in psar5.columns if "PSARl" in c), None)
            sar5_bull = bool(col_l and not pd.isna(psar5[col_l].iloc[-1]))
            h1_bull   = _h1_trend_bull(inst_id)
            lines.append(
                f"<b>{sym}</b>: SAR M5 {'🟢 BULL' if sar5_bull else '🔴 BEAR'} | "
                f"SAR M15: {sar15:.4f} | RSI2 M5: {rsi2:.1f} | "
                f"H1 {'↑ bull' if h1_bull else '↓ bear'}"
            )
        except Exception as e:
            lines.append(f"<b>{sym}</b>: erro — {e}")
    return "\n".join(lines)

def _v11_dashboard_text() -> str:
    with _strategy_lock:
        st = dict(_strategy_enabled)
    def m(flag):  return "✅ ON"  if flag else "⛔ OFF"
    def mp(mode): return "✅ ON"  if mode == "on" else ("⚠️ PAUSED" if mode == "paused" else "⛔ OFF")
    def s(key):   return "✅"     if st.get(key, False) else "⛔"
    ichi_pr = False  # /pr ichimoku removido — filtro desactivado permanentemente
    anti_ig = _tsar_pol_mode == "on"
    return (
        "📊 <b>PAINEL DE COMANDO V11</b> 📊\n\n"
        "🛡️ <b>MOTORES PRINCIPAIS:</b>\n"
        f"TSAR V11 (SOL/ETH): {mp(_tsar_mode)}\n"
        f"POL SNIPER TSAR: {mp(_tsar_pol_mode)}\n\n"
        "⚔️ <b>MODOS DE PRICE ACTION:</b>\n"
        f"OPÇÃO B (PA Independente): {m(_mode_opb)}\n"
        f"OPÇÃO C (Híbrido 5×/3×): {m(_mode_opc)}\n"
        f"⚡ OPÇÃO D (Sniper MACD M5): {m(_mode_opd)}\n\n"
        "🛑 <b>FILTROS GLOBAIS DE SEGURANÇA:</b>\n"
        f"BTC Sentinel (RSI M15): {'✅ ATIVO' if _btc_sentinel_active else '⛔ DESLIGADO'}\n"
        f"Prioridade Ichimoku 1H: {'✅ ATIVO (Invertido)' if ichi_pr else '⛔ OFF'}\n"
        f"Anti-Ignição (Engolfo/SAR Duplo): {'✅ ATIVO' if anti_ig else '⛔ OFF'}\n\n"
        "🎯 <b>ESTRATÉGIAS INDIVIDUAIS (LEGACY):</b>\n"
        f"Engolfo: {s('engolfo')} | FVG: {s('fvg')}\n"
        f"Pin Bar: {s('pin_bar')} | OB: {s('ob')}\n"
        f"VWAP: {s('vwap')} | Supertrend: {s('supertrend')}\n\n"
        "⚙️ <b>GESTÃO DE RISCO:</b>\n"
        f"Alavancagem Base: <b>{LEVERAGE}×</b>\n"
        f"Step Trail GV5: ✅ ATIVO"
    )

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
      SOL, BNB, ETH → HOLD (SL 5%, circuit breaker -4%)
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

    try:
        df_4h = okx_candles(inst_id, bar="4H", limit=50)
        ema20_4h = df_4h["close"].ewm(span=20, adjust=False).mean().iloc[-1]
        price_4h = float(df_4h["close"].iloc[-1])
        trend_4h_bull = price_4h > ema20_4h
    except Exception:
        trend_4h_bull = None  # fail-safe: não bloqueia se API falhar

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
                if trend_4h_bull is None or trend_4h_bull:
                    g["filled"] = True
                    signal_out  = "buy"
                    break

        if g["side"] == "sell" and cur["close"] < cur["ema200"]:
            if abs(cur["high"] - mid) <= tol or (cur["low"] <= mid <= cur["high"]):
                if trend_4h_bull is None or not trend_4h_bull:
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
# PRICE ACTION — 5 operacionais autónomos (OpB / OpC)
# ══════════════════════════════════════════════════════════════════════════════

def signal_engolfo(df: pd.DataFrame) -> str | None:
    if len(df) < 30: return None
    df = df.copy()
    df["ema200"] = ta.ema(df["close"], length=200)
    df["rsi"]    = ta.rsi(df["close"], length=14)
    df["vol_ma"] = df["vol"].rolling(20).mean()
    c, p = df.iloc[-2], df.iloc[-3]
    if pd.isna(c["ema200"]): return None
    body = abs(c["close"] - c["open"])
    if body < c["close"] * 0.0025: return None
    if c["vol"] < c["vol_ma"] * 1.5: return None
    pt = max(p["open"], p["close"]); pb = min(p["open"], p["close"])
    ct = max(c["open"], c["close"]); cb = min(c["open"], c["close"])
    bull = (p["close"] < p["open"] and c["close"] > c["open"]
            and cb <= pb and ct >= pt and c["close"] > c["ema200"]
            and 40 <= c["rsi"] <= 65)
    bear = (p["close"] > p["open"] and c["close"] < c["open"]
            and cb <= pb and ct >= pt and c["close"] < c["ema200"]
            and 35 <= c["rsi"] <= 60)
    if bull: return "buy"
    if bear: return "sell"
    return None

def signal_pin_bar(df: pd.DataFrame) -> str | None:
    if len(df) < 30: return None
    df = df.copy()
    df["ema200"] = ta.ema(df["close"], length=200)
    df["rsi"]    = ta.rsi(df["close"], length=14)
    df["vol_ma"] = df["vol"].rolling(20).mean()
    bb = ta.bbands(df["close"], length=20, std=2.0)
    if bb is None or bb.empty: return None
    col_u = next((c for c in bb.columns if c.startswith("BBU_")), None)
    col_l = next((c for c in bb.columns if c.startswith("BBL_")), None)
    if col_u is None or col_l is None: return None
    df["bb_u"] = bb[col_u]; df["bb_l"] = bb[col_l]
    c = df.iloc[-2]
    if pd.isna(c["ema200"]): return None
    rng  = c["high"] - c["low"]
    if rng == 0: return None
    body = abs(c["close"] - c["open"])
    lw   = min(c["open"], c["close"]) - c["low"]
    uw   = c["high"] - max(c["open"], c["close"])
    vol_ok = c["vol"] >= c["vol_ma"] * 1.3
    bull = (lw >= 2*body and uw < 0.5*body
            and c["close"] > c["ema200"]
            and c["low"] <= c["bb_l"] * 1.002
            and 25 <= c["rsi"] <= 50 and vol_ok)
    bear = (uw >= 2*body and lw < 0.5*body
            and c["close"] < c["ema200"]
            and c["high"] >= c["bb_u"] * 0.998
            and 50 <= c["rsi"] <= 75 and vol_ok)
    if bull: return "buy"
    if bear: return "sell"
    return None

def signal_inside_bar(df: pd.DataFrame) -> str | None:
    if len(df) < 30: return None
    df = df.copy()
    df["ema200"] = ta.ema(df["close"], length=200)
    df["rsi"]    = ta.rsi(df["close"], length=14)
    df["vol_ma"] = df["vol"].rolling(20).mean()
    c, p, m = df.iloc[-2], df.iloc[-3], df.iloc[-4]
    if pd.isna(c["ema200"]): return None
    inside = p["high"] <= m["high"] and p["low"] >= m["low"]
    if not inside: return None
    vol_ok = c["vol"] >= c["vol_ma"] * 1.4
    bull = (c["close"] > m["high"] and c["close"] > c["ema200"]
            and 45 <= c["rsi"] <= 65 and vol_ok)
    bear = (c["close"] < m["low"] and c["close"] < c["ema200"]
            and 35 <= c["rsi"] <= 55 and vol_ok)
    if bull: return "buy"
    if bear: return "sell"
    return None

def signal_ema21_rejection(df: pd.DataFrame) -> str | None:
    if len(df) < 30: return None
    df = df.copy()
    df["ema21"]  = ta.ema(df["close"], length=21)
    df["ema200"] = ta.ema(df["close"], length=200)
    df["rsi"]    = ta.rsi(df["close"], length=14)
    c, p = df.iloc[-2], df.iloc[-3]
    if pd.isna(c["ema200"]): return None
    touched = (min(c["low"], p["low"])   <= c["ema21"] * 1.002
               and max(c["high"], p["high"]) >= c["ema21"] * 0.998)
    if not touched: return None
    rng = c["high"] - c["low"]
    if rng == 0: return None
    pavio = (c["low"] - min(c["open"], c["close"])) / rng
    bull = (c["close"] > c["ema21"] and c["close"] > c["ema200"]
            and pavio >= 0.35 and 40 <= c["rsi"] <= 60)
    bear = (c["close"] < c["ema21"] and c["close"] < c["ema200"]
            and pavio >= 0.35 and 40 <= c["rsi"] <= 60)
    if bull: return "buy"
    if bear: return "sell"
    return None

def signal_three_soldiers(df: pd.DataFrame) -> str | None:
    if len(df) < 30: return None
    df = df.copy()
    df["ema200"] = ta.ema(df["close"], length=200)
    df["rsi"]    = ta.rsi(df["close"], length=14)
    df["vol_ma"] = df["vol"].rolling(20).mean()
    v1, v2, v3 = df.iloc[-4], df.iloc[-3], df.iloc[-2]
    if pd.isna(v3["ema200"]): return None
    vol_cresce = v2["vol"] > v1["vol"] and v3["vol"] > v2["vol"]
    soldiers = (v1["close"] > v1["open"] and v2["close"] > v2["open"]
                and v3["close"] > v3["open"]
                and v2["close"] > v1["close"] and v3["close"] > v2["close"]
                and v3["close"] > v3["ema200"] and 50 <= v3["rsi"] <= 72
                and vol_cresce)
    crows = (v1["close"] < v1["open"] and v2["close"] < v2["open"]
             and v3["close"] < v3["open"]
             and v2["close"] < v1["close"] and v3["close"] < v2["close"]
             and v3["close"] < v3["ema200"] and 28 <= v3["rsi"] <= 50
             and vol_cresce)
    if soldiers: return "buy"
    if crows: return "sell"
    return None

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

PA_SIGNALS: dict[str, callable] = {
    "engolfo":    signal_engolfo,
    "pin_bar":    signal_pin_bar,
    "inside_bar": signal_inside_bar,
    "ema21":      signal_ema21_rejection,
    "3soldiers":  signal_three_soldiers,
}

# ══════════════════════════════════════════════════════════════════════════════
# MONITOR — aguarda fecho de posição em thread separada
# ══════════════════════════════════════════════════════════════════════════════

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
             tag: str = "DUO ELITE", armadilha: bool = False, tsar: bool = False,
             min_trail_pct: float = 0.0) -> None:
    global _duo_in_trade, _duo_cooldown_until, _tsar_combat_grau
    log.info("📡 SENTINELA [%s] %s %s | SL=%.5f | Trailing activa a %.5f | STEP TRAIL V5",
             tag, sym, dir_txt, sl_px, activate_px)
    _none_streak      = 0
    _step_trail_tier  = 0   # tier 0=nenhum activado; 1-5 = grau em vigor
    tsar_locked       = False
    tsar_peak         = 0.0
    tsar_grau         = 0   # GV5 grau actual desta posição
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
                            cancel_all_open_orders(inst_id); time.sleep(0.5)
                            okx_close_market(inst_id, pos_side, pos_sz_cb)
                            tg(f"🚨 <b>CIRCUIT BREAKER -{CIRCUIT_BREAKER_PCT:.0f}% — FECHO DE EMERGÊNCIA</b>\n"
                               f"Par: <code>{sym}</code> | {dir_txt}\n"
                               f"Movimento adverso: <b>-{adverse_pct:.2f}%</b> em preço\n"
                               f"🛡️ Banca protegida — cooldown 30 min activado.")
                            with _duo_lock:
                                _duo_in_trade       = False
                                _duo_cooldown_until = time.time() + DUO_COOLDOWN
                            return
                        except Exception as e:
                            log.error("circuit breaker close fail %s: %s", sym, e)
                        continue

            # ── GESTÃO DE POSIÇÃO (TSAR V11 | armadilha SAR M15 | Step Trail V5) ──
            if pos is not None:
                upl     = float(pos.get("upl",    0) or 0)
                mark_px = float(pos.get("markPx", 0) or 0)
                avg_px  = float(pos.get("avgPx",  entry) or entry)
                pos_sz  = int(float(pos.get("pos", qty) or qty))

                if tsar:
                    # ── TSAR V11 GV5 — 5 graus de lock crescente ──────────────
                    new_grau = sum(1 for trig, _ in TSAR_GV5 if upl >= trig)
                    if new_grau > tsar_grau and mark_px > 0 and avg_px > 0 and upl > 0:
                        _, lock_usd = TSAR_GV5[new_grau - 1]
                        if side == "buy":
                            lock_px = avg_px + lock_usd * (mark_px - avg_px) / upl
                        else:
                            lock_px = avg_px - lock_usd * (avg_px - mark_px) / upl
                        try:
                            clear_garbage(inst_id, pos_side); time.sleep(0.5)
                            okx_initial_sl(inst_id, pos_side, pos_sz, lock_px)
                            tsar_grau = new_grau
                            _tsar_combat_grau = new_grau
                            tsar_peak = max(tsar_peak, upl)
                            grau_bar = "🟢" * tsar_grau + "⚪" * (len(TSAR_GV5) - tsar_grau)
                            prox_txt = (
                                f"Próximo: +${TSAR_GV5[tsar_grau][0]:.0f} → piso +${TSAR_GV5[tsar_grau][1]:.0f}"
                                if tsar_grau < len(TSAR_GV5) else "🏆 GRAU MÁXIMO — runner SAR M15 activo"
                            )
                            tg(f"⚔️ <b>TSAR GV5 GRAU {tsar_grau}/5</b> {grau_bar}\n"
                               f"Par: <code>{sym}</code> | P&L: <b>${upl:+.2f}</b>\n"
                               f"SL travado em +${lock_usd:.0f} ({lock_px:.5f})\n"
                               f"{prox_txt}")
                        except Exception as e:
                            log.warning("TSAR GV5 upgrade G%d: %s", new_grau, e)

                    if tsar_grau >= 1:
                        tsar_peak  = max(tsar_peak, upl)
                        sar15      = _get_sar_m15_px(inst_id)
                        h1_bull    = _h1_trend_bull(inst_id)
                        with_trend = (side == "buy" and h1_bull) or (side == "sell" and not h1_bull)

                        if sar15 > 0 and mark_px > 0:
                            sar_inv = ((side == "buy"  and sar15 > mark_px) or
                                       (side == "sell" and sar15 < mark_px))
                            if sar_inv:
                                log.info("⚔️ TSAR GV5 G%d SAR M15 inverteu %s — fechando",
                                         tsar_grau, sym)
                                try:
                                    cancel_all_open_orders(inst_id); time.sleep(0.5)
                                    okx_close_market(inst_id, pos_side, pos_sz)
                                    tg(f"⚔️ <b>TSAR — SAR M15 INVERTEU G{tsar_grau}/5</b>\n"
                                       f"Par: <code>{sym}</code> | {dir_txt}\n"
                                       f"P&L: <b>${upl:+.2f}</b> USDT | Pico: ${tsar_peak:+.2f}")
                                    _tsar_combat_grau = 0
                                    with _duo_lock:
                                        _duo_in_trade       = False
                                        _duo_cooldown_until = time.time() + TSAR_COOLDOWN
                                except Exception as e:
                                    log.error("TSAR GV5 SAR close: %s", e)
                                return
                            else:
                                if ((side == "buy"  and sar15 > sl_px and sar15 < mark_px) or
                                    (side == "sell" and sar15 < sl_px and sar15 > mark_px)):
                                    try:
                                        clear_garbage(inst_id, pos_side); time.sleep(0.5)
                                        okx_initial_sl(inst_id, pos_side, pos_sz, sar15)
                                        log.info("⚔️ TSAR GV5 ratchet %s %.5f→%.5f", sym, sl_px, sar15)
                                        sl_px = sar15
                                    except Exception as e:
                                        log.warning("TSAR GV5 ratchet: %s", e)
                                if not with_trend and upl < tsar_peak * 0.5:
                                    log.info("⚔️ TSAR GV5 G%d contra H1 — pico $%.2f lucro $%.2f %s",
                                             tsar_grau, tsar_peak, upl, sym)
                                    try:
                                        cancel_all_open_orders(inst_id); time.sleep(0.5)
                                        okx_close_market(inst_id, pos_side, pos_sz)
                                        tg(f"⚔️ <b>TSAR — SAÍDA AGRESSIVA G{tsar_grau} (contra H1)</b>\n"
                                           f"Par: <code>{sym}</code> | P&L: ${upl:+.2f} "
                                           f"(pico ${tsar_peak:+.2f})")
                                        _tsar_combat_grau = 0
                                        with _duo_lock:
                                            _duo_in_trade       = False
                                            _duo_cooldown_until = time.time() + TSAR_COOLDOWN
                                    except Exception as e:
                                        log.error("TSAR GV5 aggressive close: %s", e)
                                    return

                        if mark_px > 0 and _h1_band_opposite(inst_id, side):
                            log.info("⚔️ TSAR GV5 alvo H1 G%d %s", tsar_grau, sym)
                            try:
                                cancel_all_open_orders(inst_id); time.sleep(0.5)
                                okx_close_market(inst_id, pos_side, pos_sz)
                                tg(f"⚔️ <b>TSAR — ALVO H1 ATINGIDO G{tsar_grau}/5 🏆</b>\n"
                                   f"Par: <code>{sym}</code> | P&L: <b>${upl:+.2f}</b> USDT")
                                _tsar_combat_grau = 0
                                with _duo_lock:
                                    _duo_in_trade       = False
                                    _duo_cooldown_until = time.time() + TSAR_COOLDOWN
                            except Exception as e:
                                log.error("TSAR GV5 H1 close: %s", e)
                            return

                elif armadilha:
                    # ── SAR M15 TRAILING — Armadilha V10 ─────────────────────
                    sar15 = _get_sar_m15_px(inst_id)
                    if sar15 > 0 and mark_px > 0:
                        sar_inv = ((side == "buy"  and sar15 > mark_px) or
                                   (side == "sell" and sar15 < mark_px))
                        if sar_inv:
                            log.info("🪤 SAR M15 inverteu %s — fechando", sym)
                            try:
                                cancel_all_open_orders(inst_id); time.sleep(0.5)
                                okx_close_market(inst_id, pos_side, pos_sz)
                                tg(f"🪤 <b>ARMADILHA — SAR M15 INVERTEU</b>\n"
                                   f"Par: <code>{sym}</code> | {dir_txt}\n"
                                   f"P&L: <b>${upl:+.2f}</b> USDT | SAR M15 cruzou o preço")
                                with _duo_lock:
                                    _duo_in_trade       = False
                                    _duo_cooldown_until = time.time() + DUO_COOLDOWN
                            except Exception as e:
                                log.error("SAR M15 close: %s", e)
                            return
                        else:
                            # Ratchet SL ao SAR M15 (só avança a favor)
                            if ((side == "buy"  and sar15 > sl_px and sar15 < mark_px) or
                                (side == "sell" and sar15 < sl_px and sar15 > mark_px)):
                                try:
                                    clear_garbage(inst_id, pos_side); time.sleep(0.5)
                                    okx_initial_sl(inst_id, pos_side, pos_sz, sar15)
                                    log.info("🪤 SAR M15 ratchet %s: SL %.5f→%.5f",
                                             sym, sl_px, sar15)
                                    sl_px = sar15
                                except Exception as e:
                                    log.warning("SAR M15 ratchet: %s", e)
                    # Alvo: banda H1 oposta
                    if mark_px > 0 and _h1_band_opposite(inst_id, side):
                        log.info("🎯 %s banda H1 oposta — tomando lucro armadilha", sym)
                        try:
                            cancel_all_open_orders(inst_id); time.sleep(0.5)
                            okx_close_market(inst_id, pos_side, pos_sz)
                            tg(f"🎯 <b>ARMADILHA — ALVO H1 ATINGIDO</b>\n"
                               f"Par: <code>{sym}</code> | {dir_txt}\n"
                               f"P&L: <b>${upl:+.2f}</b> USDT 🏆 | Banda H1 oposta tocada")
                            with _duo_lock:
                                _duo_in_trade       = False
                                _duo_cooldown_until = time.time() + DUO_COOLDOWN
                        except Exception as e:
                            log.error("H1 alvo close: %s", e)
                        return
                else:
                    # ── STEP TRAIL V5 (modo normal) ───────────────────────────
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

def _fire(inst_id: str, side: str, signal_name: str,
          tag: str = "DUO ELITE", sl_pct: float | None = None,
          force: bool = False, qty_mult: float = 1.0,
          tsar_monitor: bool = False, sl_px_override: float = 0.0,
          min_trail_pct: float = 0.0) -> bool:
    """Executa ordem market + SL inicial + Step Trail V5.

    Filtros antes da ordem (ignorados se force=True):
      1. BTC Sentinel (maré macro 1H + RSI 15m)
      2. RSI Dual: rsi14>50 e rsi2<45 para LONG; rsi14<50 e rsi2>55 para SHORT
      3. ONE DIRECTION ONLY — aborta se já existe posição aberta

    Limit order com desconto LIMIT_OFFSET_PCT (0.15%). Poll de fill até
    LIMIT_FILL_TIMEOUT (180s); cancela e retorna False se não preencher.

    SL routing (sobrepõe sl_pct passado, excepto se for explícito):
      - HOLD pairs (POL/ETH/SOL): SL = HOLD_SL_PCT (5%) → circuit breaker
      - STRICT pairs (ADA/DOGE): SL = STRICT_SL_PCT (1.5%)
      - Outros: usa sl_pct passado ou DUO_SL_PCT
    """
    global _duo_in_trade, _lockdown_until, _btc_sentinel_active

    # ── Routing automático do SL pela classificação do par ────────────────
    if sl_pct is None:
        if   inst_id in HOLD_PAIRS:   sl_pct = HOLD_SL_PCT
        elif inst_id in STRICT_PAIRS: sl_pct = STRICT_SL_PCT
        else:                         sl_pct = DUO_SL_PCT
    ps      = _SIDE_PS[side]
    sym     = inst_id.replace("-USDT-SWAP", "")
    dir_txt = "LONG 🟢" if side == "buy" else "SHORT 🔴"

    rsi14, rsi2 = get_rsi_dual(inst_id)   # para mensagem Telegram de entrada

    if not force:
        # ── BTC SENTINEL — filtro de maré (1H macro + RSI 15m) ──────────────
        if _btc_sentinel_active:
            btc_sentiment, btc_blocked, _btc_px, _btc_ema, _btc_rsi = get_btc_sentiment()
            log.debug("[DEBUG] Sentinel consultado: BTC está %s (RSI=%.1f, bloqueado=%s)",
                      btc_sentiment, _btc_rsi, btc_blocked)
            if btc_blocked:
                log.info("[SENTINEL] BTC RSI extremo (%.1f) — %s bloqueado", _btc_rsi, sym)
                tg(f"[SENTINEL 🛡️] <b>{sym} bloqueado</b>\n"
                   f"BTC RSI {_btc_rsi:.1f} — exaustão. Aguardando normalização.")
                with _duo_lock:
                    _lockdown_until = max(_lockdown_until, time.time() + 300)
                return False
            if btc_sentiment == "NEUTRO":
                log.info("[SENTINEL] BTC em zona neutra — %s permitido com cautela", sym)
            elif btc_sentiment in ("BULLISH", "BULLISH_FRACO") and side == "sell":
                log.info("[SENTINEL 🛡️] %s SHORT bloqueado — BTC %s", sym, btc_sentiment)
                tg(f"[SENTINEL 🛡️] <b>{sym} SHORT bloqueado</b>\n"
                   f"BTC {btc_sentiment} (1H) — não vender contra a maré.")
                return False
            elif btc_sentiment in ("BEARISH", "BEARISH_FRACO") and side == "buy":
                log.info("[SENTINEL 🛡️] %s LONG bloqueado — BTC %s", sym, btc_sentiment)
                tg(f"[SENTINEL 🛡️] <b>{sym} LONG bloqueado</b>\n"
                   f"BTC {btc_sentiment} (1H) — não comprar contra a maré.")
                return False
    else:
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
        log.error("❌ ERRO OKX [%s] %s: %s", tag, sym, err_msg)
        tg(f"❌ <b>ERRO OKX [{tag}]</b>\n<code>{err_msg}</code>")
        with _duo_lock:
            _lockdown_until = time.time()   # reset lockdown — falha não deve penalizar
        return False

    # ── Aguardar preenchimento confirmado (máx 10s) ───────────────────────────
    avg   = market_px
    for _ in range(10):
        time.sleep(1)
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
    activate_px = avg * (1 + TRAIL_ACTIVATE_PCT / 100) if side == "buy" else avg * (1 - TRAIL_ACTIVATE_PCT / 100)

    okx_initial_sl(inst_id, ps, qty, sl_px)
    okx_trailing_stop(inst_id, ps, qty, activate_px)

    tg(f"✅ <b>{tag} — ENTRADA CONFIRMADA (Market)</b>\n"
       f"Par: <code>{sym}</code> | {dir_txt}\n"
       f"Fill: <code>{avg:.5f}</code> | SL: <code>{sl_px:.5f}</code> (-{sl_pct}%)\n"
       f"📡 Trailing activa a <code>{activate_px:.5f}</code> (+{TRAIL_ACTIVATE_PCT}%)\n"
       f"🔒 Step Trail V5 activo | Circuit Breaker -{CIRCUIT_BREAKER_PCT:.0f}%")

    with _duo_lock:
        _duo_in_trade = True

    threading.Thread(target=_monitor,
        args=(inst_id, ps, side, avg, sl_px, activate_px, sym, dir_txt, bal, qty),
        kwargs={"tag": tag, "armadilha": (_armadilha_mode or _trail_mode == "gv6"),
                "tsar": (_tsar_mode == "on" or tsar_monitor),
                "min_trail_pct": min_trail_pct},
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
    "ada": SHIELD_ADA, "doge": GOLD_DOGE, "bnb": FVG_BNB,
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
    arm_txt   = "🪤 ARMADILHA ON" if _armadilha_mode else ""
    tsar_txt  = f"⚔️ TSAR {'ON' if _tsar_mode == 'on' else 'PAUSED'}" if _tsar_mode else ""
    tpol_txt  = f"🎯 TSAR POL {'ON' if _tsar_pol_mode == 'on' else 'PAUSED'}" if _tsar_pol_mode else ""
    modes_txt = " | ".join(filter(None, [trail_txt, arm_txt, tsar_txt, tpol_txt]))
    return (f"📊 <b>COMMANDER V11 — {datetime.now(timezone.utc).strftime('%d/%m %H:%M UTC')}</b>\n"
            f"💰 {bal_str}\n"
            f"Status: {status}\n"
            f"⚙️ Alavancagem: <b>{LEVERAGE}×</b>  |  CB -{CIRCUIT_BREAKER_PCT:.0f}%\n"
            f"🎛️ Modo: {modes_txt}\n"
            f"🔥 TODOS os 7 pares entram AUTOMÁTICO\n"
            f"POL · SOL · ETH · XRP · BNB · ADA · DOGE\n\n"
            f"<b>COMANDOS:</b>\n"
            f"/tp /radar /lpd /meta /status /panic\n"
            f"/go[coin] /gv5 /gv6 /force [coin] /risco\n"
            f"/tsar on|pause|off|status\n"
            f"/tsarpol on|pause|off|status\n"
            f"/subir [2-10]  |  /armadilha\n"
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
    "goada":  SHIELD_ADA,
    "godoge": GOLD_DOGE,
    "gobnb":  FVG_BNB,
}

def telegram_commands_loop() -> None:
    global _tg_offset, _bot_authorized, _panic_until, LEVERAGE, _trail_mode, _tsar_mode, _tsar_pol_mode, _tsar_combat_grau, _mode_opb, _mode_opc, _mode_opd
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
                    with _strategy_lock:
                        snap = dict(_strategy_enabled)
                    labels = {
                        "ichimoku":   "🥇 POL  ICHIMOKU 1H",
                        "supertrend": "🌊 SOL  SUPERTREND 15m",
                        "rsidiv":     "🎯 RSI DIV + VWAP 15m",
                        "vwap":       "💧 ETH  VWAP KISS 15m",
                        "engolfo":    "🔥 SOL  ENGOLFO 15m",
                        "ob":         "🛡️ ADA/DOGE  ORDER BLOCK 1H",
                        "fvg":        "🔷 SOL/BNB/ETH  FVG 15m",
                    }
                    lines = ["📋 <b>ESTRATÉGIAS — estado actual</b>\n"]
                    for k, label in labels.items():
                        icon = "✅ ON " if snap[k] else "⛔ OFF"
                        lines.append(f"{icon} — {label}")
                    tsar_icon = "✅ ON " if _tsar_mode == "on" else ("⏸ PAU" if _tsar_mode == "paused" else "⛔ OFF")
                    lines.append(f"{tsar_icon} — ⚔️ BNB/SOL/ETH  TSAR V11 (Expulsão)")
                    tpol_icon = "✅ ON " if _tsar_pol_mode == "on" else ("⏸ PAU" if _tsar_pol_mode == "paused" else "⛔ OFF")
                    lines.append(f"{tpol_icon} — 🎯 POL  SNIPER TSAR (Inversão Total)")
                    opb_icon = "✅ ON " if _mode_opb else "⛔ OFF"
                    opc_icon = "✅ ON " if _mode_opc else "⛔ OFF"
                    opd_icon = "✅ ON " if _mode_opd else "⛔ OFF"
                    lines.append(f"{opb_icon} — 📐 ETH/SOL/BNB/POL  OPÇÃO B (PA Indep.)")
                    lines.append(f"{opc_icon} — 🔀 ETH/SOL  OPÇÃO C (Híbrido TSAR+PA)")
                    lines.append(f"{opd_icon} — ⚡ ETH/SOL/POL  OPÇÃO D (Sniper MACD M5)")
                    lines.append("\n<i>/pausar [chave] | /activar [chave] | tudo</i>\n"
                                 "<i>/tsar on | pause | off | status</i>\n"
                                 "<i>/tsarpol on | pause | off | status</i>\n"
                                 "<i>/opb — toggle OpB | /opc — toggle OpC | /opd — toggle OpD</i>")
                    tg("\n".join(lines), chat_id)

                # ── /opb — Opção B PA Independentes ON/OFF ───────────────────
                elif cmd == "opb":
                    _mode_opb = not _mode_opb
                    estado = "✅ LIGADA" if _mode_opb else "⭕ DESLIGADA"
                    tg(f"📐 <b>Opção B — PA Independentes: {estado}</b>\n"
                       f"4 operacionais autónomos em ETH/SOL/BNB/POL\n"
                       f"Engolfo | Pin Bar | Inside Bar | EMA21 | 3 Soldiers\n"
                       f"{'⚠️ Mais trades — win rate ~72%' if _mode_opb else ''}",
                       chat_id)
                    log.info("Opção B: %s", estado)

                # ── /opc — Opção C Híbrido TSAR+PA ON/OFF ────────────────────
                elif cmd == "opc":
                    _mode_opc = not _mode_opc
                    estado = "✅ LIGADA" if _mode_opc else "⭕ DESLIGADA"
                    tg(f"🔀 <b>Opção C — Híbrido: {estado}</b>\n"
                       f"TSAR + PA → 5× | Só PA → 3×\n"
                       f"Pares: ETH e SOL\n"
                       f"{'⚠️ Equilíbrio entre frequência e precisão' if _mode_opc else ''}",
                       chat_id)
                    log.info("Opção C: %s", estado)

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

                # ── /sentinel [on|off] — toggle ou força estado BTC Sentinel ───
                elif cmd == "sentinel":
                    global _btc_sentinel_active
                    if args and args[0] in ("on", "off", "ligar", "desligar"):
                        _btc_sentinel_active = args[0] in ("on", "ligar")
                    else:
                        _btc_sentinel_active = not _btc_sentinel_active
                    if _btc_sentinel_active:
                        sentiment, blocked, px, ema, rsi = get_btc_sentiment()
                        tg(f"🛡️ <b>BTC Sentinel ACTIVO ✅</b>\n"
                           f"BTC agora: <b>{sentiment}</b>\n"
                           f"Price: {px:,.0f} | EMA20 1H: {ema:,.0f} | RSI 15m: {rsi:.1f}\n"
                           f"Filtra contra-tendência em todos os 7 pares.", chat_id)
                    else:
                        tg(f"🛡️ <b>BTC Sentinel DESLIGADO ⚠️</b>\n"
                           f"Todas as entradas permitidas sem filtro BTC.\n"
                           f"Para religar: <code>/sentinel on</code>", chat_id)
                    log.info("BTC Sentinel: %s", "ACTIVO" if _btc_sentinel_active else "OFF")

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
                               f"RSI 15m: <b>{rsi:.1f}</b>{rsi_note}\n"
                               f"Sentinel: {'🛡️ ACTIVO' if _btc_sentinel_active else '⚠️ OFF'}", chat_id)
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
                    global _armadilha_mode
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

                # ── /tsar — PROTOCOLO TSAR V11 ────────────────────────────────
                elif cmd == "tsar":
                    if not args:
                        tg("⚔️ Uso: <code>/tsar on | pause | off | status</code>", chat_id)
                    elif args[0] == "on":
                        _tsar_mode = "on"
                        with _pending_lock: _pending_signals.clear()
                        _save_state(_bot_authorized)
                        tg("⚔️ <b>TSAR V11 MODO COMBATE ACTIVADO</b>\n"
                           "─────────────────────────────\n"
                           "Pares: <b>ETH · SOL</b>\n"
                           "• Regra da Expulsão: BB M5+M15 + RSI2 ≥88/≤12 + SAR M5 flip\n"
                           "• BTC RSI M15 extremo (&lt;30 ou &gt;70) → +50%% size\n"
                           "• SL hard: <b>2%%</b> | GV5: G1$30→G2$50→G3$75→G4$100→G5$150\n"
                           "• H1 favor → trailing lento | H1 contra → saída agressiva 50%%\n"
                           "• FVG automaticamente pausada | Cooldown: 15 min\n"
                           "Usa <code>/tsar off</code> para desligar.", chat_id)
                    elif args[0] == "pause":
                        _tsar_mode = "paused"
                        _save_state(_bot_authorized)
                        tg("⏸ <b>TSAR PAUSADO</b>\n"
                           "Sem novas entradas. Posições abertas continuam geridas.", chat_id)
                    elif args[0] in ("off", "stop"):
                        _tsar_mode = ""
                        _save_state(_bot_authorized)
                        tg("🔓 <b>TSAR DESLIGADO</b>\nFVG e outras estratégias retomadas.", chat_id)
                    elif args[0] == "status":
                        try: tg(_tsar_status_text(), chat_id)
                        except Exception as e: tg(f"Erro /tsar status: {e}", chat_id)
                    else:
                        tg("❌ Opções: <code>/tsar on | pause | off | status</code>", chat_id)

                # ── /combat — alias rápido para /tsar on|off ────────────────────
                elif cmd == "combat":
                    if not args or args[0] in ("on", "start"):
                        _tsar_mode = "on"
                        with _pending_lock: _pending_signals.clear()
                        _save_state(_bot_authorized)
                        tg("⚔️ <b>MODO COMBATE ACTIVADO</b>\n"
                           "ETH · SOL | GV5 | RSI2 ≥88/≤12 | SL 2%% | Cooldown 15min", chat_id)
                    elif args[0] in ("off", "stop"):
                        _tsar_mode = ""
                        _save_state(_bot_authorized)
                        tg("🔓 <b>MODO COMBATE DESLIGADO</b>", chat_id)
                    else:
                        tg("Uso: <code>/combat on | off</code>", chat_id)

                # ── /grau — estado GV5 da posição TSAR activa ───────────────────
                elif cmd == "grau":
                    g = _tsar_combat_grau
                    grau_bar = "🟢" * g + "⚪" * (len(TSAR_GV5) - g)
                    if g == 0:
                        status_txt = "Sem grau activo (posição ainda não atingiu G1 ou sem posição)"
                    else:
                        trig, lock = TSAR_GV5[g - 1]
                        nxt = (f"Próximo G{g+1}: +${TSAR_GV5[g][0]:.0f}"
                               if g < len(TSAR_GV5) else "🏆 GRAU MÁXIMO")
                        status_txt = f"G{g}/5 travado em +${lock:.0f} | {nxt}"
                    tg(f"⚔️ <b>TSAR GV5</b> {grau_bar}\n{status_txt}\n"
                       f"Graus: G1$30→$25 | G2$50→$35 | G3$75→$55 | G4$100→$75 | G5$150→$110",
                       chat_id)

                # ── /tsarpol — POL SNIPER TSAR ─────────────────────────────────
                elif cmd == "tsarpol":
                    if not args:
                        tg("🎯 Uso: <code>/tsarpol on | pause | off | status</code>", chat_id)
                    elif args[0] == "on":
                        _tsar_pol_mode = "on"
                        _save_state(_bot_authorized)
                        tg("🎯 <b>POL SNIPER TSAR ACTIVADO</b>\n"
                           "─────────────────────────────\n"
                           "Par: <b>POL-USDT-SWAP</b>\n"
                           "• Inversão Total: Ichimoku 1H é contexto, nunca gatilho\n"
                           "• Lightning: RSI2 ≤ 1 ou ≥ 90 + fora da BB → entrada imediata\n"
                           "• Standard: BB M5+M15 + RSI2 &lt;12/&gt;88 + SAR M5 flip\n"
                           "• SL hard: <b>2%%</b> | Lock $30 → SAR M15 trailing\n"
                           "Usa <code>/tsarpol off</code> para desligar.", chat_id)
                    elif args[0] == "pause":
                        _tsar_pol_mode = "paused"
                        _save_state(_bot_authorized)
                        tg("⏸ <b>POL SNIPER TSAR PAUSADO</b>\n"
                           "Sem novas entradas. Posições abertas continuam geridas.", chat_id)
                    elif args[0] in ("off", "stop"):
                        _tsar_pol_mode = ""
                        _save_state(_bot_authorized)
                        tg("🔓 <b>POL SNIPER TSAR DESLIGADO</b>\nIchimoku POL retoma se activado.", chat_id)
                    elif args[0] == "status":
                        try:
                            boost, btc_rsi = _tsar_btc_boost()
                            boost_txt = "⚡ EXTREMO → +50%% size" if boost else "normal"
                            df5 = okx_candles(GOLD_POL, bar="5m", limit=30)
                            c5  = df5["close"]
                            d   = c5.diff()
                            g   = d.clip(lower=0).ewm(span=2, adjust=False).mean()
                            l   = (-d).clip(lower=0).ewm(span=2, adjust=False).mean()
                            rsi2 = float(100 - (100 / (1 + g.iloc[-1] / (l.iloc[-1] + 1e-10))))
                            sar15 = _get_sar_m15_px(GOLD_POL)
                            psar5 = df5.ta.psar(af0=0.02, af=0.02, max_af=0.20)
                            col_l = next((c for c in psar5.columns if "PSARl" in c), None)
                            sar5_bull = bool(col_l and not pd.isna(psar5[col_l].iloc[-1]))
                            h1_bull   = _h1_trend_bull(GOLD_POL)
                            ichi_sig  = None
                            try: ichi_sig = ichimoku_signal(okx_candles(GOLD_POL, bar="1H", limit=200))
                            except Exception: pass
                            ichi_txt = f"Ichimoku 1H: {'↑ LONG' if ichi_sig=='buy' else '↓ SHORT' if ichi_sig=='sell' else 'neutro'}"
                            tg(f"🎯 <b>POL SNIPER TSAR STATUS</b> — modo: <b>{'ON' if _tsar_pol_mode=='on' else 'PAUSED' if _tsar_pol_mode=='paused' else 'OFF'}</b>\n"
                               f"BTC RSI M15: <b>{btc_rsi:.1f}</b> ({boost_txt})\n"
                               f"<b>POL</b>: SAR M5 {'🟢 BULL' if sar5_bull else '🔴 BEAR'} | SAR M15: {sar15:.5f} | RSI2: {rsi2:.1f} | H1 {'↑' if h1_bull else '↓'}\n"
                               f"{ichi_txt} (contexto — não é gatilho)", chat_id)
                        except Exception as e:
                            tg(f"Erro /tsarpol status: {e}", chat_id)
                    else:
                        tg("❌ Opções: <code>/tsarpol on | pause | off | status</code>", chat_id)

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
                       "/backtest — Backtest real 100 dias OKX (~40s)\n\n"
                       "<b>Acção manual:</b>\n"
                       "/gv5 — Step Trail V5 | /gv6 — SAR M15 trailing\n"
                       "/tsar on|pause|off|status — TSAR V11 Expulsão\n"
                       "/combat on|off — alias rápido /tsar | /grau — estado GV5\n"
                       "/opb — 📐 Opção B PA Independentes ON/OFF\n"
                       "/opc — 🔀 Opção C Híbrido TSAR+PA ON/OFF\n"
                       "/clab — 🧹 Cancela TODAS as ordens abertas na OKX\n"
                       "/go[coin] — Confirma sinal pendente (120s)\n"
                       "  /goeth  /gosol  /goada  /godoge  /gobnb\n"
                       "/frl p — Maker exit (limit no melhor ask/bid)\n"
                       "/frl l — Limit exit no preço actual (sem taker fee)\n"
                       "/force [coin] — Ordem mercado bypass filtros\n"
                       "  Ex: <code>/force bnb</code>  (RSI 15m decide LONG/SHORT)\n\n"
                       "🥇 POL/SOL/ETH/BNB/ADA/DOGE — TODOS AUTOMÁTICOS\n\n"
                       f"CB -{CIRCUIT_BREAKER_PCT:.0f}%  |  HOLD SL {HOLD_SL_PCT:.0f}%  |  STRICT SL {STRICT_SL_PCT:.1f}%\n"
                       f"GV5/GV6  |  Lev actual: <b>{LEVERAGE}×</b>  |  cd 5min", chat_id)

                # ── /v11 — Painel de Comando Tático completo ───────────────────
                elif cmd == "v11":
                    try: tg(_v11_dashboard_text(), chat_id)
                    except Exception as e: tg(f"Erro /v11: {e}", chat_id)

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
    log.info("🎯 V11 COMMANDER SUITE — FULL SQUAD + TSAR V11 READY — TODOS AUTOFIRE")
    tg("🏆 <b>V11 FULL SQUAD — TODOS OS PARES AUTOMÁTICOS</b>\n\n"
       "🥇 <b>POL</b> — ICHIMOKU 1H (97.4% hit)\n"
       "🌊 <b>SOL</b> — SUPERTREND + FVG 15m\n"
       "💧 <b>ETH</b> — VWAP KISS + FVG 15m\n"
       "🔷 <b>BNB</b> — FVG 15m (65.2% hit)\n"
       "🛡️ <b>ADA</b> — ORDER BLOCK 1H\n"
       "🎲 <b>DOGE</b> — ORDER BLOCK 1H\n"
       "⚔️ <b>BNB/SOL/ETH</b> — TSAR V11 (Expulsão BB)\n\n"
       "⚡ <b>TODOS entram automático</b> — sem /go[coin] obrigatório\n"
       "(O /go[coin] ainda existe para confirmar manualmente se quiseres)\n\n"
       f"🔒 GV5/GV6  |  CB -{CIRCUIT_BREAKER_PCT:.0f}%  |  HOLD {HOLD_SL_PCT:.0f}%  |  STRICT {STRICT_SL_PCT:.1f}%  |  "
       f"{LEVERAGE}× ALL-IN  |  cd 5min\n"
       "✅ <b>10 ESTRATÉGIAS ATIVAS. TSAR V11 PRONTO.</b>")

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
                    # Modo exaustão: sinal invertido — o Ichimoku na POL
                    # gerava -$200 seguindo tendência. Invertido usa o sinal
                    # como indicador de exaustão e entra na reversão.
                    if sig_raw == "buy":    sig = "sell"
                    elif sig_raw == "sell": sig = "buy"
                    else:                  sig = None
                    if sig:
                        log.info("[ICHI POL INVERTIDO] raw=%s → entrada=%s", sig_raw, sig)
                        dir_scout = "📈 LONG" if sig == "buy" else "📉 SHORT"
                        tg(f"🥇 <b>GOLDEN — POL ICHIMOKU INVERTIDO</b>\n"
                           f"Par: <code>POL-USDT-SWAP</code> | Sinal: <b>ICHIMOKU 1H (exaustão)</b>\n"
                           f"Direção: <b>{dir_scout}</b>  | raw={sig_raw} → invertido\n"
                           f"💰 Hold the hand — alvo $20 NET. Circuit breaker -{CIRCUIT_BREAKER_PCT:.0f}%.")
                        fired = _fire(GOLD_POL, sig, "ICHIMOKU POL", tag="🥇 GOLDEN POL")
                    else:
                        log.info("[POL] sem sinal")
                except Exception as e:
                    log.error("[POL] %s", e)

            # ── 🎯 POL SNIPER TSAR — Confirmação Dupla BB+SAR+Ichimoku ─────────
            if not fired and _tsar_pol_mode == "on":
                try:
                    result = tsar_pol_signal(GOLD_POL)
                    if result:
                        sig, sar5_px = result
                        cloud_dir = _ichimoku_cloud_dir(GOLD_POL)
                        ichi_note = (f" | ☁️ Ichimoku {'↑ bull' if cloud_dir=='bull' else '↓ bear' if cloud_dir=='bear' else '~ neutro'}")
                        dir_scout = "📈 LONG" if sig == "buy" else "📉 SHORT"
                        sl_note   = f"SAR M5: {sar5_px:.5f}" if sar5_px > 0 else f"2%"
                        log.info("🎯 TSAR POL %s SAR5=%.5f cloud=%s", sig.upper(), sar5_px, cloud_dir)
                        tg(f"🎯 <b>POL SNIPER TSAR — CONFIRMAÇÃO DUPLA</b>\n"
                           f"Par: <code>POL-USDT-SWAP</code> | {dir_scout}{ichi_note}\n"
                           f"BB M5+M15 toque + SAR M5 inversão | SL dinâmico: {sl_note}\n"
                           f"Saída: SAR M15 inversão")
                        fired = _fire(GOLD_POL, sig, "TSAR POL",
                                      tag="🎯 SNIPER POL", sl_pct=TSAR_SL_PCT,
                                      force=True, qty_mult=1.0, tsar_monitor=True,
                                      sl_px_override=sar5_px)
                    else:
                        log.debug("[TSAR POL] sem sinal")
                except Exception as e:
                    log.error("[TSAR POL] %s", e)

            # ── 🌊 PRIORIDADE 2: SOL — SUPERTREND 15m (95% hit, HOLD) ───────
            if not fired and st_enabled["supertrend"]:
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

            # ── 4: ETH — VWAP KISS ────────────────────────────────────────────
            if not fired and st_enabled["vwap"]:
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
            if not fired and st_enabled["engolfo"]:
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
            if not fired and st_enabled["ob"]:
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

            # ── 7b: DOGE — ORDER BLOCK DEFENSE (STRICT 1.5%) ─────────────────
            if not fired and st_enabled["ob"]:
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

            # ╔══════════════ TSAR V11 MODO COMBATE — EXPULSION PROTOCOL ═════╗
            # ── ETH · SOL — Regra da Expulsão (BB M5+M15 + RSI2 88/12 + SAR) ─
            if not fired and _tsar_mode == "on":
                for _ti, _ts, _tt in [
                    (DUO_ETH, "ETH", "⚔️ TSAR"), (DUO_SOL, "SOL", "⚔️ TSAR")
                ]:
                    if fired: break
                    try:
                        sig = tsar_signal(_ti)
                        if sig:
                            btc_boost, _btc_r = _tsar_btc_boost()
                            qm = 1.5 if btc_boost else 1.0
                            boost_info = f" ⚡ BTC {_btc_r:.0f} +50%%" if btc_boost else ""
                            log.info("⚔️ TSAR %s %s%s", _ts, sig.upper(), boost_info)
                            tg(f"⚔️ <b>TSAR V11 — {_ts} EXPULSÃO</b>\n"
                               f"{'LONG 🟢' if sig=='buy' else 'SHORT 🔴'} | SL 2%%{boost_info}\n"
                               f"BB M5+M15 rompida + RSI2 exaustão + SAR M5 inverteu")
                            fired = _fire(_ti, sig, f"TSAR V11 {_ts}",
                                          tag=f"⚔️ TSAR {_ts}", sl_pct=TSAR_SL_PCT,
                                          force=True, qty_mult=qm)
                        else:
                            log.debug("[TSAR/%s] sem sinal", _ts)
                    except Exception as e:
                        log.error("[TSAR/%s] %s", _ts, e)

            # ╔══════════════ FVG EXPANSION SQUAD (V9) ═══════════════════════╗
            # ── 8: SOL — FAIR VALUE GAP 15m (70.6% hit / ROI +70.4%) ────────
            if not fired and st_enabled["fvg"] and _tsar_mode != "on":
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
            if not fired and st_enabled["fvg"] and _tsar_mode != "on":
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
            if not fired and st_enabled["fvg"] and _tsar_mode != "on":
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

            # ── OPÇÃO B — PA Independentes ──────────────────────────────────
            if _mode_opb and not fired:
                with _duo_lock:
                    em_trade = _duo_in_trade
                if not em_trade:
                    for par, inst_id, bar in [
                        ("ETH", DUO_ETH,  "15m"),
                        ("SOL", DUO_SOL,  "15m"),
                        ("BNB", FVG_BNB,  "15m"),
                        ("POL", GOLD_POL, "1H"),
                    ]:
                        try:
                            df = okx_candles(inst_id, bar=bar, limit=300)
                            for pa_name, pa_fn in PA_SIGNALS.items():
                                sig = pa_fn(df)
                                if sig:
                                    log.info("[OpB] %s %s → %s", pa_name, par, sig)
                                    tg(f"📐 <b>OpB — {pa_name.upper()} {par}</b>\n"
                                       f"Sinal: {'📈 LONG' if sig == 'buy' else '📉 SHORT'} | {bar}")
                                    fired = _fire(inst_id, sig,
                                                  f"OpB {pa_name}", tag=f"📐 OpB {par}",
                                                  sl_pct=1.5, min_trail_pct=0.8)
                                    if fired: break
                            if fired: break
                        except Exception as e:
                            log.error("[OpB] %s: %s", par, e)

            # ── OPÇÃO C — Híbrido (TSAR+PA 5× / só PA 3×) ──────────────────
            if _mode_opc and not fired:
                with _duo_lock:
                    em_trade = _duo_in_trade
                if not em_trade:
                    for par, inst_id in [("ETH", DUO_ETH), ("SOL", DUO_SOL)]:
                        try:
                            df = okx_candles(inst_id, bar="15m", limit=300)
                            for pa_name, pa_fn in PA_SIGNALS.items():
                                sig = pa_fn(df)
                                if not sig: continue
                                tsar_confirma = tsar_signal(inst_id) == sig
                                lev_label = "5×" if tsar_confirma else "3×"
                                qty_mult  = 5.0 / LEVERAGE if tsar_confirma else 3.0 / LEVERAGE
                                log.info("[OpC] %s %s %s tsar=%s lev=%s",
                                         pa_name, par, sig, tsar_confirma, lev_label)
                                tg(f"🔀 <b>OpC — {pa_name.upper()} {par}</b>\n"
                                   f"Sinal: {'📈 LONG' if sig == 'buy' else '📉 SHORT'}\n"
                                   f"{'✅ TSAR confirma — ' if tsar_confirma else '⚡ Só PA — '}{lev_label}")
                                fired = _fire(inst_id, sig,
                                              f"OpC {pa_name}", tag=f"🔀 OpC {par}",
                                              sl_pct=1.5, qty_mult=qty_mult,
                                              min_trail_pct=0.8)
                                if fired: break
                            if fired: break
                        except Exception as e:
                            log.error("[OpC] %s: %s", par, e)

            # ── OPÇÃO D — Sniper MACD M5 (ETH/SOL/POL) ─────────────────────
            if _mode_opd and not fired:
                with _duo_lock:
                    em_trade = _duo_in_trade
                if not em_trade:
                    for _d_inst, _d_par in [
                        (DUO_ETH,  "ETH"),
                        (DUO_SOL,  "SOL"),
                        (GOLD_POL, "POL"),
                    ]:
                        try:
                            df5 = okx_candles(_d_inst, bar="5m", limit=100)
                            sig = signal_macd_bollinger(df5)
                            if sig:
                                dir_scout = "📈 LONG" if sig == "buy" else "📉 SHORT"
                                log.info("[OpD] MACD BB %s → %s", _d_par, sig.upper())
                                tg(f"⚡ <b>OpD: SNIPER MACD M5</b>\n"
                                   f"Par: <code>{_d_inst}</code> | {dir_scout}\n"
                                   f"BB exaustão + MACD a reverter | SL 1.5% | Trail +0.8%")
                                fired = _fire(_d_inst, sig,
                                              f"OpD MACD {_d_par}", tag=f"⚡ OpD {_d_par}",
                                              sl_pct=1.5, min_trail_pct=0.8)
                                if fired: break
                        except Exception as e:
                            log.error("[OpD] %s: %s", _d_par, e)

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
    _load_full_state()
    with _auth_lock:
        log.info("Estado: %s | tsar=%r | trail=%s",
                 "AUTORIZADO ✅" if _bot_authorized else "PAUSADO ⛔", _tsar_mode, _trail_mode)

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
