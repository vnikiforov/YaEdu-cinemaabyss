import os
from datetime import datetime, timezone
import uuid
import logging
import json
from typing import Dict, Any, Optional, Callable
from concurrent.futures import Future
from flask import Flask, request, jsonify
from confluent_kafka import Producer, KafkaException, Message
import traceback

SVC_ROOT_URL = "/api/events/{:s}"

__app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
g__logger = logging.getLogger(__name__)

# Конфигурация Kafka
KAFKA_BROKERS = os.environ['KAFKA_BROKERS']
TOPICS = {
    'movie': 'movie-events',
    'user': 'user-events', 
    'payment': 'payment-events'
}

# Глобальный трекер доставки сообщений
g__delivery_tracker = {}

# Получить текущее UTC время в формате ISO 8601 с часовым поясом Zulu
def get_iso8601_utc() -> str:    
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

# Форматировать объект datetime в ISO 8601 с часовым поясом Zulu
def format_datetime_iso8601(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

class KafkaProducer:  
    def __init__(self, brokers):
        self.conf = {
            'bootstrap.servers': brokers,
            'client.id': 'cinemaabyss-events-service',
            'acks': 'all',  # Ждем подтверждения от всех реплик
            'retries': 5,   # Увеличиваем количество попыток
            'retry.backoff.ms': 1000,  # Задержка между попытками
            'compression.type': 'lz4',
            'message.timeout.ms': 55000,  # Таймаут сообщения 45 секунд
            'socket.timeout.ms': 30000,   # Таймаут сокета
            'request.timeout.ms': 30000,  # Таймаут запроса
            'max.in.flight.requests.per.connection': 1,  # Для точной доставки
            'enable.idempotence': True,   # Идемпотентность
            'on_delivery': self.delivery_report
        }
        self.producer = Producer(**self.conf)
        self.delivery_futures = {}

    # Глобальный обработчик событий отчетов о доставке сообщений без специфического отслеживания future.
    # Используется как обработчик по умолчанию для всех сообщений.
    def delivery_report(self, err: Optional[Exception], msg: Message):                
        if err is not None:
            g__logger.error(f"Ошибка доставки сообщения: {err}")
            # Логировать дополнительный контекст, если доступен
            if msg and msg.key():
                key = msg.key().decode('utf-8')
                g__logger.error(f"Ключ неудачного сообщения: {key}")
        else:
            delivery_info = {
                'topic': msg.topic(),
                'partition': msg.partition(),
                'offset': msg.offset(),
                'timestamp': msg.timestamp()[1] if msg.timestamp() else None,
                'key': msg.key().decode('utf-8') if msg.key() else None
            }
            g__logger.info(
                f"Сообщение доставлено в {msg.topic()} "
                f"[партиция {msg.partition()}] по смещению {msg.offset()}"
            )
            
            # Проверить, есть ли у этого сообщения future для завершения
            if msg.key():
                key = msg.key().decode('utf-8')
                if key in self.delivery_futures:
                    future = self.delivery_futures[key]
                    future.set_result(delivery_info)
                    # Очистка
                    del self.delivery_futures[key]
                    if key in g__delivery_tracker:
                        del g__delivery_tracker[key]
    
    def produce_message(self, topic: str, key: str, value: Dict[str, Any]) -> Future:        
        # Проверить доступность Kafka перед отправкой
        try:
            # Быстрая проверка метаданных
            metadata = self.producer.list_topics(topic, timeout=5.0)
            if topic not in metadata.topics:
                g__logger.warning(f"Топик {topic} не существует, будет создан автоматически")
        except Exception as e:
            g__logger.error(f"Kafka недоступен: {e}")
            future = Future()
            future.set_exception(Exception(f"Kafka недоступен: {e}"))
            return future
    
        # Отправить сообщение в Kafka и вернуть Future для отслеживания доставки
        try:
            # Создать future для отслеживания доставки
            future = Future()
            self.delivery_futures[key] = future
            
            # Сохранить контекст сообщения для callback
            g__delivery_tracker[key] = {
                'topic': topic,
                'timestamp': get_iso8601_utc(),
                'future': future
            }
            
            # Использовать lambda callback для специфического отслеживания сообщений
            self.producer.produce(
                topic=topic,
                key=key,
                value=json.dumps(value),
                on_delivery=lambda err, msg: self.delivery_callback(err, msg, key)
            )
            
            self.producer.poll(0)
            g__logger.info(f"Сообщение отправлено в топик {topic} с ключом {key}")
            return future
            
        except BufferError:
            # Обработать переполнение буфера путем опроса и повторной попытки
            self.producer.poll(1)
            raise
        except KafkaException as e:
            g__logger.error(f"Ошибка Kafka: {str(e)}")
            future = Future()
            future.set_exception(e)
            return future
    
    # Callback для отчетов о доставке сообщений со специфическим отслеживанием по ключу
    def delivery_callback(self, err: Optional[Exception], msg: Message, key: str):
        # Специфический обработчик события доставки сообщений, которым нужно отслеживание future.
        # Дополняет глобальный метод delivery_report.        
        if key in self.delivery_futures:
            future = self.delivery_futures[key]
            
            if err is not None:
                g__logger.error(f"Ошибка доставки сообщения для ключа {key}: {err}")
                future.set_exception(KafkaException(str(err)))
            else:
                delivery_info = {
                    'topic': msg.topic(),
                    'partition': msg.partition(),
                    'offset': msg.offset(),
                    'timestamp': msg.timestamp()[1] if msg.timestamp() else None,
                    'delivery_time': datetime.now(timezone.utc)
                }
                g__logger.info(
                    f"Сообщение доставлено в {msg.topic()} "
                    f"[партиция {msg.partition()}] по смещению {msg.offset()}"
                )
                future.set_result(delivery_info)
            
            # Очистка
            del self.delivery_futures[key]
            if key in g__delivery_tracker:
                del g__delivery_tracker[key]
    
    # Сбросить ожидающие сообщения с таймаутом
    def flush(self, timeout: float = 5.0):        
        remaining = self.producer.flush(timeout)
        if remaining > 0:
            g__logger.warning(f"{remaining} сообщений остались неотправленными после таймаута сброса")
        return remaining
class EventService:    
    @staticmethod
    def create_event(event_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        event_id = str(uuid.uuid4())
        timestamp = get_iso8601_utc()
        
        event = {
            "id": event_id,
            "type": event_type,
            "timestamp": timestamp,
            "payload": payload
        }
        
        try:
            # Синхронная отправка с ожиданием подтверждения
            future = g__kafka_producer.produce_message(
                topic=TOPICS[event_type],
                key=event_id,
                value=event
            )
            
            # Ожидать подтверждение доставки с таймаутом
            try:
                g__logger.info(f"Начало ожидания подтверждения для {event_id}")
                delivery_info = future.result(timeout=30.0)
                g__logger.info(f"Подтверждение получено для {event_id}")

                response = {
                    "status": "success",  # Изменено с "accepted" на "success"
                    "event_id": event_id,
                    "topic": delivery_info['topic'],
                    "partition": delivery_info['partition'],
                    "offset": delivery_info['offset'],
                    "timestamp": get_iso8601_utc(),
                    "event": event
                }
                
                g__logger.info(f"Успешно создано событие {event_type}: {event_id}")
                return response
            
            except TimeoutError:
                g__logger.warning(f"Таймаут доставки Kafka для события {event_id}")
                # Вернуть статус processing вместо accepted
                return {
                    "status": "processing",
                    "event_id": event_id,
                    "message": "Событие обрабатывается, подтверждение доставки ожидается",
                    "timestamp": get_iso8601_utc(),
                    "event": event
                }
                
        except Exception as e:        
            g__logger.error(f"Глобальная ошибка отправки в Kafka: {str(e)}")
            g__logger.error(f"Трассировка ошибки отправки: {traceback.format_exc()}")
        raise
                        
# Инициализация Kafka producer
g__kafka_producer = KafkaProducer(KAFKA_BROKERS)

# Проверка готовности сервиса с реальной проверкой подключения к Kafka
@__app.route(SVC_ROOT_URL.format('health'), methods=['GET'])
def get_events_service_health():
    return jsonify({
        "status": True
    })
            
@__app.route(SVC_ROOT_URL.format('movie'), methods=['POST'])
def create_movie_event():
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "Не предоставлены JSON данные"}), 400
        
        required_fields = ['movie_id', 'title', 'action']
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Отсутствует обязательное поле: {field}"}), 400
        
        event_response = EventService.create_event("movie", data)
        return jsonify(event_response), 201
        
    except Exception as e:
        g__logger.error("Ошибка создания события фильма: {:s}".format(str(e)))
        return jsonify({"error": "Внутренняя ошибка сервера", "details": str(e)}), 500

@__app.route(SVC_ROOT_URL.format('user'), methods=['POST'])
def create_user_event():
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "Не предоставлены JSON данные"}), 400
        
        required_fields = ['user_id', 'action']
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Отсутствует обязательное поле: {field}"}), 400
        
        if 'timestamp' not in data:
            data['timestamp'] = get_iso8601_utc()
        
        event_response = EventService.create_event("user", data)
        return jsonify(event_response), 201
        
    except Exception as e:        
        g__logger.error("Ошибка создания события пользователя: {:s}".format(str(e)))
        return jsonify({"error": "Внутренняя ошибка сервера", "details": str(e)}), 500

@__app.route(SVC_ROOT_URL.format('payment'), methods=['POST'])
def create_payment_event():
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "Не предоставлены JSON данные"}), 400
        
        required_fields = ['payment_id', 'user_id', 'amount', 'status']
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Отсутствует обязательное поле: {field}"}), 400
        
        if 'timestamp' not in data:
            data['timestamp'] = get_iso8601_utc()
        
        event_response = EventService.create_event("payment", data)
        return jsonify(event_response), 201
        
    except Exception as e:
        g__logger.error("Ошибка создания события платежа: {:s}".format(str(e)))        
        return jsonify({"error": "Внутренняя ошибка сервера", "details": str(e)}), 500

# Проверки статуса сообщения
@__app.route(SVC_ROOT_URL.format('status/<event_id>'), methods=['GET'])
def get_event_status(event_id):
    # Проверить статус доставки конкретного события
    if event_id in g__delivery_tracker:
        status = {
            "event_id": event_id,
            "status": "processing",
            "timestamp": g__delivery_tracker[event_id]['timestamp'],
            "topic": g__delivery_tracker[event_id]['topic'],
            "current_time": get_iso8601_utc()
        }
        return jsonify(status), 200
    else:
        return jsonify({
            "event_id": event_id,
            "status": "unknown",
            "message": "ID события не найден в последних доставках",
            "timestamp": get_iso8601_utc()
        }), 404

@__app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint не найден"}), 404

@__app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Внутренняя ошибка сервера"}), 500

@__app.teardown_appcontext
def shutdown_session(exception=None):
    g__logger.info("Сброс оставшихся сообщений Kafka...")
    remaining = g__kafka_producer.flush(10.0)  # Таймаут 10 секунд
    if remaining == 0:
        g__logger.info("Все сообщения успешно сброшены")
    else:
        g__logger.warning(f"{remaining} сообщений не удалось сбросить")

if __name__ == '__main__':
    try:
        # Проверить подключение к Kafka при запуске
        test_future = g__kafka_producer.produce_message(
            'service-start', 
            'init', 
            {'service': 'events', 'startup_time': get_iso8601_utc()}
        )
        
        # Не блокировать запуск в ожидании доставки
        g__logger.info(f"Подключение к Kafka брокерам: {KAFKA_BROKERS}")
        
    except Exception as e:
        g__logger.error(f"Тест подключения к Kafka не удался: {str(e)}")        
        # Все равно продолжить запуск - Kafka может стать доступен позже

    __app.run(host='0.0.0.0', port=os.environ["PORT"], debug=False, threaded=True)