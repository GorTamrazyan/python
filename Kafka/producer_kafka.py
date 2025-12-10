from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
import json
import avro.schema
import avro.io
import io
import uuid
import time
import logging
from datetime import datetime
from typing import Dict, Any

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ComprehensiveKafkaProducer:
    def __init__(self):
        self.bootstrap_servers = ['localhost:9092']
        
        # Создаем разные продюсеры для разных целей
        self.producers = self._create_producers()
        self.admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        
    def _create_producers(self):
        """Создает продюсеры разных типов"""
        producers = {}
        
        # 1. Обычный Producer (String serialization)
        producers['string'] = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            key_serializer=str.encode,
            value_serializer=lambda v: str(v).encode('utf-8'),
            acks='all',
            retries=3,
            batch_size=16384,
            linger_ms=5
        )
        
        # 2. JSON Producer
        producers['json'] = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            key_serializer=str.encode,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks=1
        )
        
        # 3. ByteArray Producer
        producers['bytes'] = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            key_serializer=str.encode,
            value_serializer=lambda v: v.encode('utf-8') if isinstance(v, str) else v,
            acks=0  # No acknowledgment
        )
        
        # 4. Transactional Producer
        producers['transactional'] = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            transactional_id='txn_producer_1',
            key_serializer=str.encode,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        
        # # 5. Producer с RoundRobin partitioner
        # producers['roundrobin'] = KafkaProducer(
        #     bootstrap_servers=self.bootstrap_servers,
        #     partitioner=RoundRobinPartitioner(),
        #     value_serializer=lambda v: json.dumps(v).encode('utf-8')
        # )
        
        return producers
    
    def create_topics(self):
        """Создает топики разных типов"""
        topics = [
            # Regular topic
            NewTopic(
                name='regular_topic',
                num_partitions=3,
                replication_factor=1,
                topic_configs={
                    'retention.ms': '604800000',  # 7 days
                    'cleanup.policy': 'delete'
                }
            ),
            # Compact topic (для логов изменений)
            NewTopic(
                name='compact_topic',
                num_partitions=2,
                replication_factor=1,
                topic_configs={
                    'cleanup.policy': 'compact',
                    'delete.retention.ms': '86400000'
                }
            ),
            # High-throughput topic
            NewTopic(
                name='high_volume_topic',
                num_partitions=6,
                replication_factor=1,
                topic_configs={
                    'compression.type': 'lz4',
                    'segment.bytes': '1073741824'  # 1GB
                }
            )
        ]
        
        try:
            self.admin_client.create_topics(new_topics=topics, validate_only=False)
            logger.info("Топики созданы успешно")
        except TopicAlreadyExistsError:
            logger.info("Топики уже существуют")
    
    def send_different_message_types(self):
        """Отправляет сообщения разных типов"""
        
        # 1. Keyed messages (гарантированный порядок)
        for i in range(10):
            key = f"user_{i % 3}"  # 3 разных ключа
            message = {
                "user_id": key,
                "action": "login",
                "timestamp": datetime.now().isoformat(),
                "sequence": i
            }
            future = self.producers['json'].send(
                'regular_topic',
                key=key,
                value=message
            )
            # Async callback
            future.add_callback(self._on_send_success, key, message)
            future.add_errback(self._on_send_error, key, message)
        
        # 2. Null-key messages (round-robin распределение)
        # 2. Null-key messages
        for i in range(5):
            message = {"message": f"broadcast_{i}", "type": "notification"}
            self.producers['json'].send('regular_topic', value=message)  # Օգտագործիր 'json' producer-ը
        
        # 3. Compact topic messages (только последнее значение для ключа)
        user_profiles = [
            {"user_id": "user_1", "name": "Alice", "email": "alice@example.com"},
            {"user_id": "user_2", "name": "Bob", "email": "bob@example.com"},
            {"user_id": "user_1", "name": "Alice Smith", "email": "alice.smith@example.com"},  # Обновление
        ]
        
        for profile in user_profiles:
            self.producers['json'].send(
                'compact_topic',
                key=profile['user_id'],
                value=profile
            )
        
        # 4. Transactional messages
        self._send_transactional_messages()
        
        # 5. High volume messages
        self._send_high_volume_messages()
    
    def _send_transactional_messages(self):
        """Отправка транзакционных сообщений"""
        producer = self.producers['transactional']
        
        try:
            producer.init_transactions()
            producer.begin_transaction()
            
            # Multiple messages in one transaction
            producer.send('regular_topic', key="txn_1", value={"action": "debit", "amount": 100})
            producer.send('compact_topic', key="user_1", value={"balance": 900})
            
            # Commit transaction
            producer.commit_transaction()
            logger.info("Транзакция завершена успешно")
            
        except Exception as e:
            producer.abort_transaction()
            logger.error(f"Транзакция отменена: {e}")
    
    def _send_high_volume_messages(self):
        """Отправка большого объема сообщений"""
        for i in range(100):
            message = {
                "metric": "cpu_usage",
                "value": 50 + (i % 30),
                "timestamp": datetime.now().isoformat(),
                "server": f"server_{i % 5}"
            }
            self.producers['bytes'].send(
                'high_volume_topic',
                key=f"server_{i % 5}",
                value=json.dumps(message)
            )
    
    def _on_send_success(self, key, message, metadata):
        logger.info(f"Сообщение отправлено: key={key}, partition={metadata.partition}, offset={metadata.offset}")
    
    def _on_send_error(self, key, message, exception):
        logger.error(f"Ошибка отправки: key={key}, error={exception}")
    
    def close_all(self):
        """Закрывает все продюсеры"""
        for name, producer in self.producers.items():
            producer.flush()
            producer.close()
        self.admin_client.close()

# Запуск producer
if __name__ == "__main__":
    producer = ComprehensiveKafkaProducer()
    producer.create_topics()
    producer.send_different_message_types()
    time.sleep(2)  # Даем время для отправки
    producer.close_all()