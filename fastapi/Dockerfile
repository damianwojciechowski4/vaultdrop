FROM python:3.12-slim

# Install Node.js (for Claude CLI) and git
RUN apt-get update && apt-get install -y \
    curl git nodejs npm \
    && rm -rf /var/lib/apt/lists/*

# Install Claude CLI
RUN npm install -g @anthropic-ai/claude-code

# Install defuddle (used by save-url skill)
RUN npm install -g defuddle-cli 2>/dev/null || true

RUN useradd -m -u 1000 -s /bin/bash vaultdrop && \
    mkdir -p /data && \
    chown vaultdrop:vaultdrop /data

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

USER vaultdrop
ENV HOME=/home/vaultdrop

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
