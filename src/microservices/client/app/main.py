import os
import json
import logging
import signal
import sys
from datetime import datetime
from typing import Dict, Any, List
from confluent_kafka import Consumer, KafkaException, TopicPartition, OFFSET_BEGINNING

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('kafka-test-consumer')

class KafkaTestConsumer:
    def __init__(self, brokers: str, group_id: str = 'test-consumer-group'):
        self.conf = {
            'bootstrap.servers': brokers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'max.poll.interval.ms': 300000,
            'session.timeout.ms': 10000
        }
        self.consumer = Consumer(self.conf)
        self.running = False
        self.topics = ['movie-events', 'user-events', 'payment-events']
        
    def subscribe_to_all_topics(self):
        """Подписаться на все топики системы"""
        logger.info(f"Подписка на топики: {self.topics}")
        self.consumer.subscribe(self.topics)
        
    def subscribe_to_topic(self, topic: str):
        """Подписаться на конкретный топик"""
        logger.info(f"Подписка на топик: {topic}")
        self.consumer.subscribe([topic])
        
    def consume_messages(self, max_messages: int = None):
        """Потреблять сообщения из Kafka"""
        self.running = True
        message_count = 0
        
        logger.info("Начало потребления сообщений...")
        
        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaException._PARTITION_EOF:
                        logger.info(f"Достигнут конец партиции {msg.topic()}[{msg.partition()}]")
                    else:
                        logger.error(f"Ошибка потребителя: {msg.error()}")
                    continue
                
                # Обработка сообщения
                self.process_message(msg)
                message_count += 1
                
                # Ручное подтверждение обработки
                self.consumer.commit(asynchronous=False)
                
                # Остановка после максимального количества сообщений
                if max_messages and message_count >= max_messages:
                    logger.info(f"Достигнут лимит в {max_messages} сообщений")
                    break
                    
        except KeyboardInterrupt:
            logger.info("Получен сигнал прерывания")
        except Exception as e:
            logger.error(f"Ошибка при потреблении сообщений: {e}")
        finally:
            self.shutdown()
            
    def process_message(self, msg):
        """Обработать полученное сообщение"""
        try:
            # Парсинг JSON сообщения
            message_value = json.loads(msg.value().decode('utf-8'))
            
            # Форматирование вывода
            output = {
                'timestamp': datetime.now().isoformat(),
                'topic': msg.topic(),
                'partition': msg.partition(),
                'offset': msg.offset(),
                'key': msg.key().decode('utf-8') if msg.key() else None,
                'message': message_value
            }
            
            # Красивый вывод в консоль
            print("\n" + "="*80)
            print(f"ТОПИК: {msg.topic()}")
            print(f"ПАРТИЦИЯ: {msg.partition()}")
            print(f"СМЕЩЕНИЕ: {msg.offset()}")
            print(f"КЛЮЧ: {output['key']}")
            print(f"ВРЕМЯ ПОЛУЧЕНИЯ: {output['timestamp']}")
            print("-" * 80)
            print("СОДЕРЖИМОЕ:")
            print(json.dumps(message_value, indent=2, ensure_ascii=False))
            print("="*80 + "\n")
            
            # Логирование
            logger.info(f"Получено сообщение из {msg.topic()}[{msg.partition()}]@{msg.offset()}")
            
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка парсинга JSON: {e}")
            logger.error(f"Сырое сообщение: {msg.value()}")
        except Exception as e:
            logger.error(f"Ошибка обработки сообщения: {e}")
            
    def seek_to_beginning(self):
        """Переместиться к началу всех партиций"""
        for topic in self.topics:
            try:
                # Получить метаданные топика
                metadata = self.consumer.list_topics(topic, timeout=10)
                if topic in metadata.topics:
                    for partition in metadata.topics[topic].partitions.values():
                        tp = TopicPartition(topic, partition.id, OFFSET_BEGINNING)
                        self.consumer.seek(tp)
                        logger.info(f"Перемещено к началу: {topic}[{partition.id}]")
            except Exception as e:
                logger.error(f"Ошибка перемещения к началу топика {topic}: {e}")
                
    def get_topic_metadata(self):
        """Получить метаданные топиков"""
        try:
            metadata = self.consumer.list_topics(timeout=10)
            print("\nМЕТАДАННЫЕ ТОПИКОВ:")
            print("-" * 50)
            for topic_name, topic_metadata in metadata.topics.items():
                if topic_name in self.topics:
                    print(f"Топик: {topic_name}")
                    print(f"  Партиций: {len(topic_metadata.partitions)}")
                    for partition_id, partition in topic_metadata.partitions.items():
                        print(f"  Партиция {partition_id}: лидер {partition.leader}")
                    print()
        except Exception as e:
            logger.error(f"Ошибка получения метаданных: {e}")
            
    def shutdown(self):
        """Корректное завершение работы"""
        self.running = False
        logger.info("Завершение работы потребителя...")
        self.consumer.close()
        
    def signal_handler(self, sig, frame):
        """Обработчик сигналов для graceful shutdown"""
        logger.info("Получен сигнал завершения")
        self.shutdown()
        sys.exit(0)

def main():
    # Конфигурация из переменных окружения
    kafka_brokers = os.getenv('KAFKA_BROKERS', 'localhost:9092')
    consume_all = os.getenv('CONSUME_ALL', 'false').lower() == 'true'
    max_messages = int(os.getenv('MAX_MESSAGES', '0')) or None
    specific_topic = os.getenv('TOPIC')
    
    # Создание и настройка потребителя
    consumer = KafkaTestConsumer(kafka_brokers)
    
    # Обработка сигналов
    signal.signal(signal.SIGINT, consumer.signal_handler)
    signal.signal(signal.SIGTERM, consumer.signal_handler)
    
    try:
        # Показать метаданные топиков
        consumer.get_topic_metadata()
        
        # Подписка на топики
        if specific_topic:
            if specific_topic in consumer.topics:
                consumer.subscribe_to_topic(specific_topic)
                logger.info(f"Подписка только на топик: {specific_topic}")
            else:
                logger.warning(f"Топик {specific_topic} не найден. Доступные топики: {consumer.topics}")
                consumer.subscribe_to_all_topics()
        else:
            consumer.subscribe_to_all_topics()
        
        # Переместиться к началу если нужно
        if consume_all:
            consumer.seek_to_beginning()
            logger.info("Перемещение к началу всех партиций")
        
        # Начать потребление сообщений
        consumer.consume_messages(max_messages)
        
    except Exception as e:
        logger.error(f"Ошибка в main: {e}")
    finally:
        consumer.shutdown()

if __name__ == '__main__':
    print("=" * 60)
    print("🎬 CINEMAABYSS - ТЕСТОВЫЙ ПОТРЕБИТЕЛЬ KAFKA")
    print("=" * 60)
    print("Настройки:")
    print(f"  Kafka brokers: {os.getenv('KAFKA_BROKERS', 'localhost:9092')}")
    print(f"  Потреблять все: {os.getenv('CONSUME_ALL', 'true')}")
    print(f"  Макс. сообщений: {os.getenv('MAX_MESSAGES', 'неограничено')}")
    print(f"  Конкретный топик: {os.getenv('TOPIC', 'все')}")
    print("=" * 60)
    
    main()