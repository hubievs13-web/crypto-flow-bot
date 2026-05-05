FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Copy minimal files first for better layer caching.
COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --upgrade pip && pip install .

# Default config; can be overridden by mounting a volume or rebuilding.
COPY config.yaml ./config.yaml

# Persistent state + logs live under /app/state (mount the Fly volume there).
ENV CRYPTO_FLOW_BOT_CONFIG=/app/config.yaml \
    CRYPTO_FLOW_BOT_STATE_DIR=/app/state \
    CRYPTO_FLOW_BOT_LOG_DIR=/app/state/logs

# /app/state is the mount point for the persistent volume on Fly. Pre-create the
# directory so the bot can write before the volume mounts (e.g. local runs).
RUN mkdir -p /app/state /app/state/logs

CMD ["python", "-m", "crypto_flow_bot"]
