# monitoring.py
from kafka import KafkaAdminClient
from kafka.admin import ConfigResource, ConfigResourceType
import json

class KafkaMonitor:
    def __init__(self, bootstrap_servers):
        self.admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    
    def get_topic_info(self, topic_name):
        """Получает информацию о топике"""
        try:
            # Описание топика
            resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
            configs = self.admin_client.describe_configs([resource])
            
            # Метаданные
            cluster_metadata = self.admin_client.describe_topics([topic_name])
            
            return {
                'config': configs[resource].resources[0],
                'metadata': cluster_metadata
            }
        except Exception as e:
            print(f"Ошибка получения информации о топике: {e}")
    
    def list_consumer_groups(self):
        """Список consumer groups"""
        return self.admin_client.list_consumer_groups()
    
    def get_consumer_group_offsets(self, group_id):
        """Offsets consumer group"""
        return self.admin_client.list_consumer_group_offsets(group_id)

# Использование
if __name__ == "__main__":
    monitor = KafkaMonitor(['localhost:9092'])
    
    # Мониторинг топиков
    topics = ['regular_topic', 'compact_topic', 'high_volume_topic']
    for topic in topics:
        info = monitor.get_topic_info(topic)
        print(f"=== {topic} ===")
        print(json.dumps(info, indent=2, default=str))