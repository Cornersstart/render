# ═══════════════════════════════════════════════════════════════════════════
# TradeSniper V9 FULL SQUAD — Render / Docker
# ═══════════════════════════════════════════════════════════════════════════
FROM python:3.12-slim

# Boas práticas Python em container
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=UTC

# Dependências de sistema (libgomp1 necessário para pandas-ta/numpy)
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

# Copia o código
COPY main.py .

# Render injecta $PORT automaticamente
ENV PORT=10000
EXPOSE 10000

# Healthcheck — confirma que o health server responde
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD curl -fsS "http://localhost:${PORT}/" || exit 1

CMD ["python", "-u", "main.py"]
