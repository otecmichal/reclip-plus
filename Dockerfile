# Stage 1: Get the Deno binary
FROM denoland/deno:bin AS deno-bin

# Stage 2: Final runtime image
FROM python:3.12-slim

# Copy the Deno binary from Stage 1
COPY --from=deno-bin /deno /usr/local/bin/deno

# Install ffmpeg
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Verify that Deno and yt-dlp-ejs are correctly installed and functional
RUN deno --version && python -c "import yt_dlp_ejs" && yt-dlp --version

COPY . .

EXPOSE 8899
ENV HOST=0.0.0.0
CMD ["python", "app.py"]
