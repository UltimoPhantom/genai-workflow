FROM python:3.11-slim

# Install piper-tts binary
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install piper for ARM64 (Apple Silicon / aarch64)
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "aarch64" ]; then \
        PIPER_URL="https://github.com/rhasspy/piper/releases/download/2023.11.14-2/piper_linux_aarch64.tar.gz"; \
    else \
        PIPER_URL="https://github.com/rhasspy/piper/releases/download/2023.11.14-2/piper_linux_x86_64.tar.gz"; \
    fi && \
    curl -L "$PIPER_URL" | tar -xz -C /usr/local && \
    ln -s /usr/local/piper/piper /usr/local/bin/piper

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

EXPOSE 5050
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "5050"]
