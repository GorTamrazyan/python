from kafka import KafkaConsumer, KafkaAdminClient, TopicPartition
from kafka.errors import NoBrokersAvailable, KafkaError
import json
import logging
import signal
import sys
import threading
from datetime import datetime
from typing import Dict, List, Callable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ComprehensiveKafkaConsumer:
    def __init__(self):
        self.bootstrap_servers = ['localhost:9092']
        self.shutdown = False
        self.consumers = {}
        
        # Обработка сигналов
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, signum, frame):
        logger.info("Получен сигнал остановки...")
        self.shutdown = True
    
    def create_consumers(self):
        """Создает консьюмеры разных типов"""
        
        # 1. Simple Consumer (assign specific partitions)
        self.consumers['simple'] = KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id=None  # No consumer group
        )
        # Assign specific partitions
        partitions = [TopicPartition('regular_topic', i) for i in range(3)]
        self.consumers['simple'].assign(partitions)
        
        # 2. High-Level Consumer (Consumer Group)
        self.consumers['high_level'] = KafkaConsumer(
            'regular_topic',
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='high_level_group',
            auto_offset_reset='latest',
            enable_auto_commit=True,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000
        )
        
        # 3. Compact Topic Consumer
        self.consumers['compact_reader'] = KafkaConsumer(
            'compact_topic',
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='compact_group',
            auto_offset_reset='earliest'
        )
        
        # 4. Manual Commit Consumer
        self.consumers['manual_commit'] = KafkaConsumer(
            'high_volume_topic',
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='manual_commit_group',
            enable_auto_commit=False,  # Ручное подтверждение
            auto_offset_reset='earliest'
        )
        
        # 5. Seek Consumer (для произвольного доступа)
        self.consumers['seek_consumer'] = KafkaConsumer(
            'regular_topic',
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='seek_group',
            enable_auto_commit=False
        )
    
    def start_all_consumers(self):
        """Запускает все консьюмеры в отдельных потоках"""
        threads = []
        
        consumer_methods = [
            (self.simple_consumer_loop, 'simple'),
            (self.high_level_consumer_loop, 'high_level'),
            (self.compact_topic_consumer_loop, 'compact_reader'),
            (self.manual_commit_consumer_loop, 'manual_commit'),
            (self.seek_consumer_demo, 'seek_consumer')
        ]
        
        for method, consumer_name in consumer_methods:
            thread = threading.Thread(
                target=method,
                args=(consumer_name,),
                daemon=True,
                name=f"consumer_{consumer_name}"
            )
            threads.append(thread)
            thread.start()
            logger.info(f"Запущен консьюмер: {consumer_name}")
        
        # Ожидание завершения
        for thread in threads:
            thread.join()
    
    def simple_consumer_loop(self, consumer_name):
        """Simple Consumer с назначенными партициями"""
        consumer = self.consumers[consumer_name]
        
        try:
            for message in consumer:
                if self.shutdown:
                    break
                    
                logger.info(f"[SIMPLE] Partition: {message.partition}, "
                           f"Offset: {message.offset}, "
                           f"Key: {message.key}, "
                           f"Value: {message.value}")
                
        except Exception as e:
            logger.error(f"Ошибка в simple consumer: {e}")
    
    def high_level_consumer_loop(self, consumer_name):
        """High-Level Consumer (Consumer Group)"""
        consumer = self.consumers[consumer_name]
        
        try:
            for message in consumer:
                if self.shutdown:
                    break
                
                # Разная логика обработки в зависимости от типа сообщения
                if message.key and message.key.startswith(b'user_'):
                    self._process_user_action(message)
                elif message.key and message.key.startswith(b'txn_'):
                    self._process_transaction(message)
                else:
                    self._process_general_message(message)
                    
        except Exception as e:
            logger.error(f"Ошибка в high-level consumer: {e}")
    
    def compact_topic_consumer_loop(self, consumer_name):
        """Consumer для compact топика (только последние значения)"""
        consumer = self.consumers[consumer_name]
        latest_values = {}
        
        try:
            for message in consumer:
                if self.shutdown:
                    break
                
                key = message.key.decode('utf-8') if message.key else 'null'
                latest_values[key] = message.value
                
                logger.info(f"[COMPACT] Key: {key}, Latest Value: {message.value}")
                logger.info(f"[COMPACT] Всего уникальных ключей: {len(latest_values)}")
                
        except Exception as e:
            logger.error(f"Ошибка в compact consumer: {e}")
    
    def manual_commit_consumer_loop(self, consumer_name):
        """Consumer с ручным подтверждением offset"""
        consumer = self.consumers[consumer_name]
        batch_size = 10
        message_count = 0
        
        try:
            for message in consumer:
                if self.shutdown:
                    break
                
                # Обработка сообщения
                success = self._process_metric_message(message)
                message_count += 1
                
                # Пакетное подтверждение
                if message_count >= batch_size:
                    consumer.commit()
                    logger.info(f"[MANUAL] Подтверждено {batch_size} сообщений")
                    message_count = 0
                    
        except Exception as e:
            logger.error(f"Ошибка в manual commit consumer: {e}")
        finally:
            # Final commit
            if message_count > 0:
                consumer.commit()
    
    def seek_consumer_demo(self, consumer_name):
        """Consumer с произвольным доступом к offset"""
        consumer = self.consumers[consumer_name]
        
        # Демонстрация seek - переход к определенному offset
        partitions = consumer.assignment()
        for partition in partitions:
            # Переход к началу
            consumer.seek_to_beginning(partition)
            # Или к конкретному offset
            # consumer.seek(partition, 5)
        
        end_offsets = consumer.end_offsets(partitions)
        logger.info(f"[SEEK] End offsets: {end_offsets}")
        
        try:
            for message in consumer:
                if self.shutdown:
                    break
                
                logger.info(f"[SEEK] Message at offset {message.offset}: {message.value}")
                
                # Демонстрация паузы и возобновления
                if message.offset % 20 == 0:
                    consumer.pause(*partitions)
                    logger.info("[SEEK] Потребление приостановлено на 5 секунд")
                    time.sleep(5)
                    consumer.resume(*partitions)
                    logger.info("[SEEK] Потребление возобновлено")
                    
        except Exception as e:
            logger.error(f"Ошибка в seek consumer: {e}")
    
    def _process_user_action(self, message):
        """Обработка действий пользователя"""
        logger.info(f"[USER] Обработка действия: {message.value}")
        # Имитация обработки
        time.sleep(0.1)
    
    def _process_transaction(self, message):
        """Обработка транзакций"""
        logger.info(f"[TXN] Обработка транзакции: {message.value}")
        time.sleep(0.2)
    
    def _process_general_message(self, message):
        """Обработка общих сообщений"""
        logger.info(f"[GENERAL] Сообщение: {message.value}")
    
    def _process_metric_message(self, message):
        """Обработка метрик"""
        try:
            metric_data = message.value
            logger.info(f"[METRIC] {metric_data['metric']}: {metric_data['value']}")
            return True
        except Exception as e:
            logger.error(f"Ошибка обработки метрики: {e}")
            return False
    
    def close_all(self):
        """Закрывает все консьюмеры"""
        for name, consumer in self.consumers.items():
            consumer.close()
        logger.info("Все консьюмеры закрыты")

# Запуск consumers
if __name__ == "__main__":
    import time
    
    consumer_system = ComprehensiveKafkaConsumer()
    consumer_system.create_consumers()
    
    try:
        consumer_system.start_all_consumers()
    except KeyboardInterrupt:
        logger.info("Остановка по запросу пользователя")
    finally:
        consumer_system.close_all()