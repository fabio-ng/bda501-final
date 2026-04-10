FROM python:3.11-slim

WORKDIR /app

# curl needed for Docker healthcheck
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create checkpoint directory
RUN mkdir -p checkpoints

EXPOSE 8001

CMD ["python", "main.py"]
