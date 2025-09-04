# Base image Python
FROM python:3.11-slim

# Làm việc trong thư mục /app
WORKDIR /app

# Copy requirements trước (tối ưu cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ source code
COPY . .

# Expose cổng Flask (nếu cần health check)
EXPOSE 8080

# Chạy bot
CMD ["python", "crypto_research_bot_final.py"]
