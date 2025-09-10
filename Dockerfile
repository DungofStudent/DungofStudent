FROM python:3.12-slim

# Cài dependencies cơ bản
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy code
WORKDIR /app
COPY . .

# Cài requirements
RUN pip install --no-cache-dir -r requirements.txt

# Run bot
CMD ["python", "crypto_research_bot_final.py"]
