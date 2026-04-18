# ═══════════════════════════════════════════════════════════════════════════
# TradeSniper AI PRO — V8 FULL SQUAD + GOLDEN RECOVERY DOCTRINE
# Imagem imutável para Render / qualquer host Docker (Fly.io, Railway, etc.)
# ═══════════════════════════════════════════════════════════════════════════
FROM python:3.12-slim

# Boas práticas Python em container
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=UTC

# Dependências de sistema mínimas (pandas/numpy precisam de libgomp; tzdata para UTC)
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        ca-certificates \
        tzdata \
        libgomp1 \
        curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache de dependências: copia só requirements primeiro
COPY requirements.txt .
RUN pip install --upgrade pip \
 && pip install -r requirements.txt

# Copia o código da aplicação
COPY main.py .
# bot_state.json é regenerado se não existir — não copiamos para evitar overwrite

# Render injecta $PORT automaticamente; default 10000 se rodar local/Docker simples
ENV PORT=10000
EXPOSE 10000

# Healthcheck Docker (Render usa o seu próprio, mas serve para hosts genéricos)
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD curl -fsS "http://localhost:${PORT}/" || exit 1

# Entrada — sem buffering, sem shell extra
CMD ["python", "-u", "main.py"]
