FROM python:3.9-slim

COPY ./app /app 
WORKDIR /app  

# Установка системных зависимостей для Kafka
RUN apt-get update && apt-get install -y \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"] 