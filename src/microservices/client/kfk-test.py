#!/usr/bin/env python3
import sys
import os
from confluent_kafka import Producer, Consumer, KafkaException

def check_kafka_connection(brokers):
    print(f"🔍 Проверка подключения к Kafka: {brokers}")
    
    try:
        # Проверка Producer
        producer = Producer({'bootstrap.servers': brokers, 'socket.timeout.ms': 5000})
        metadata = producer.list_topics(timeout=5.0)
        print("✅ Producer подключен успешно")
        
        # Проверка топиков
        topics = ['movie-events', 'user-events', 'payment-events']
        for topic in topics:
            if topic in metadata.topics:
                print(f"✅ Топик '{topic}' существует")
            else:
                print(f"⚠️  Топик '{topic}' не существует")
        
        producer.flush(2.0)
        
        # Проверка Consumer
        consumer = Consumer({
            'bootstrap.servers': brokers,
            'group.id': 'test-group',
            'auto.offset.reset': 'earliest'
        })
        print("✅ Consumer подключен успешно")
        consumer.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
        return False

if __name__ == "__main__":
    brokers = os.getenv('KAFKA_BROKERS', 'kafka:9092')
    success = check_kafka_connection(brokers)
    sys.exit(0 if success else 1)