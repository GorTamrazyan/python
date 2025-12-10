import pika
import json
import time
from uuid import uuid4

class CompleteRabbitMQExample:
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost')
        )
        self.channel = self.connection.channel()
        
    def setup_all_exchanges_queues(self):
        """Ստեղծում է բոլոր տեսակի exchange-ներ և queue-ներ"""
        
        print("Ստեղծվում են բոլոր RabbitMQ տեսակները...")
        
        # === 1. ԲՈԼՈՐ EXCHANGE TYPES ===
        
        # 1.1 FANOUT - բոլորին
        self.channel.exchange_declare(
            exchange='fanout_exchange', 
            exchange_type='fanout'
        )
        
        # 1.2 DIRECT - ուղղորդված
        self.channel.exchange_declare(
            exchange='direct_exchange', 
            exchange_type='direct'
        )
        
        # 1.3 TOPIC - թեմայով
        self.channel.exchange_declare(
            exchange='topic_exchange', 
            exchange_type='topic'
        )
        
        # 1.4 HEADERS - վերնագրերով
        self.channel.exchange_declare(
            exchange='headers_exchange', 
            exchange_type='headers'
        )
        
        # === 2. ԲՈԼՈՐ QUEUE TYPES ===
        
        # 2.1 ՊԱՐԶ հերթ
        self.channel.queue_declare(queue='simple_queue')
        
        # 2.2 ԿԱՅՈՒՆ հերթ (durable)
        self.channel.queue_declare(
            queue='durable_queue',
            durable=True  # Կմնա RabbitMQ-ի վերագործարկումից հետո
        )
        
        # 2.3 ԲԱՑԱՌԻԿ հերթ (exclusive)
        self.channel.queue_declare(
            queue='exclusive_queue',
            exclusive=True  # Միայն այս կապի համար
        )
        
        # 2.4 ԱՎՏՈՄԱՏ ՋՆՋՎՈՂ հերթ (auto-delete)
        self.channel.queue_declare(
            queue='auto_delete_queue',
            auto_delete=True,  # Ջնջվում է երբ consumer չկա
            exclusive=True
        )
        
        # 2.5 ԼՐԱՑՈՒՑԻՉ ԱՐԳՈՒՄԵՆՏՆԵՐՈՎ
        self.channel.queue_declare(
            queue='advanced_queue',
            durable=True,
            arguments={
                'x-message-ttl': 30000,  # Հաղորդագրության կյանքը 30 վայրկյան
                'x-max-length': 100,     # Առավելագույն 100 հաղորդագրություն
                'x-dead-letter-exchange': 'dlx',  # Dead letter exchange
                'x-max-priority': 10     # Առաջնահերթություն 0-10
            }
        )
        
        # 2.6 DEAD LETTER QUEUE 
        self.channel.queue_declare(
            queue='dead_letter_queue',
            durable=True
        )
        self.channel.exchange_declare(exchange='dlx', exchange_type='fanout')
        self.channel.queue_bind(exchange='dlx', queue='dead_letter_queue')
        
        # === 3. QUEUE BINDINGS ===
        
        # 3.1 FANOUT binding - բոլորին
        fanout_queue = self.channel.queue_declare(queue='', exclusive=True).method.queue
        self.channel.queue_bind(exchange='fanout_exchange', queue=fanout_queue)
        
        # 3.2 DIRECT binding - ուղղորդված
        direct_queue = self.channel.queue_declare(queue='', exclusive=True).method.queue
        self.channel.queue_bind(
            exchange='direct_exchange', 
            queue=direct_queue, 
            routing_key='important'
        )
        
        # 3.3 TOPIC binding - թեմայով
        topic_queue = self.channel.queue_declare(queue='', exclusive=True).method.queue
        self.channel.queue_bind(
            exchange='topic_exchange', 
            queue=topic_queue, 
            routing_key='user.#'  # Բոլոր user հաղորդագրությունները
        )
        
        # 3.4 HEADERS binding - վերնագրերով
        headers_queue = self.channel.queue_declare(queue='', exclusive=True).method.queue
        self.channel.queue_bind(
            exchange='headers_exchange',
            queue=headers_queue,
            arguments={
                'x-match': 'all',  # 'all' = բոլոր պայմանները, 'any' = ցանկացածը
                'type': 'error',
                'priority': 'high'
            }
        )
        
        print(" Բոլոր տեսակները ստեղծված են")
        return {
            'fanout_queue': fanout_queue,
            'direct_queue': direct_queue,
            'topic_queue': topic_queue,
            'headers_queue': headers_queue
        }
    
    def send_all_message_types(self):
        """Ուղարկում է բոլոր տեսակի հաղորդագրություններ"""
        
        print("\n Ուղարկվում են բոլոր տեսակի հաղորդագրությունները...")
        
        # 1. ՊԱՐԶ հաղորդագրություն
        self.channel.basic_publish(
            exchange='',
            routing_key='simple_queue',
            body='Պարզ տեքստային հաղորդագրություն',
            properties=pika.BasicProperties(
                content_type='text/plain'
            )
        )
        print(" 1. Պարզ հաղորդագրություն ուղարկված է")
        
        # 2. JSON հաղորդագրություն
        message_data = {
            "event": "user_login",
            "user_id": "user_123",
            "timestamp": time.time(),
            "success": True
        }
        self.channel.basic_publish(
            exchange='',
            routing_key='durable_queue',
            body=json.dumps(message_data),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Persistent
                content_type='application/json',
                headers={'version': '1.0'}
            )
        )
        print(" 2. JSON հաղորդագրություն ուղարկված է (persistent)")
        
        # 3. FANOUT հաղորդագրություն
        self.channel.basic_publish(
            exchange='fanout_exchange',
            routing_key='',  # Դատարկ է fanout-ի համար
            body='Հաղորդագրություն բոլոր բաժանորդներին'
        )
        print(" 3. Fanout հաղորդագրություն ուղարկված է")
        
        # 4. DIRECT հաղորդագրություն
        self.channel.basic_publish(
            exchange='direct_exchange',
            routing_key='important',
            body='Կարևոր ուղղորդված հաղորդագրություն'
        )
        print(" 4. Direct հաղորդագրություն ուղարկված է (routing_key='important')")
        
        # 5. TOPIC հաղորդագրություն
        self.channel.basic_publish(
            exchange='topic_exchange',
            routing_key='user.login.success',
            body='Մուտքը հաջողվել է'
        )
        print("📨 5. Topic հաղորդագրություն ուղարկված է (user.login.success)")
        
        # 6. HEADERS հաղորդագրություն
        self.channel.basic_publish(
            exchange='headers_exchange',
            routing_key='',  # Դատարկ է headers-ի համար
            body='Սխալի հաղորդագրություն բարձր առաջնահերթությամբ',
            properties=pika.BasicProperties(
                headers={
                    'type': 'error',
                    'priority': 'high',
                    'source': 'api'
                }
            )
        )
        print("📨 6. Headers հաղորդագրություն ուղարկված է")
        
        # 7. ԱՌԱՋՆԱՀԵՐԹՈՒԹՅԱՆ հաղորդագրություն
        self.channel.basic_publish(
            exchange='',
            routing_key='advanced_queue',
            body='Բարձր առաջնահերթությամբ հաղորդագրություն',
            properties=pika.BasicProperties(
                delivery_mode=2,
                priority=9,  # 0-10 (10 = ամենաբարձր)
                expiration='10000'  # 10 վայրկյան
            )
        )
        print("📨 7. Առաջնահերթության հաղորդագրություն ուղարկված է (priority=9)")
        
        # 8. RPC (REQUEST-REPLY) հաղորդագրություն
        correlation_id = str(uuid4())
        reply_queue = self.channel.queue_declare(queue='', exclusive=True).method.queue
        
        self.channel.basic_publish(
            exchange='',
            routing_key='rpc_queue',
            body='RPC հարցում',
            properties=pika.BasicProperties(
                reply_to=reply_queue,
                correlation_id=correlation_id
            )
        )
        print(f" 8. RPC հաղորդագրություն ուղարկված է (correlation_id={correlation_id})")
        
        print("\n Բոլոր հաղորդագրությունների տեսակները ուղարկված են")
    
    def start_consumer(self, queues):
        """Սպառում է բոլոր հերթերից"""
        
        print("\n👂 Սպասում եմ հաղորդագրությունների...")
        
        def callback(ch, method, properties, body):
            print(f"\n{'='*60}")
            print(f"📥 ՍՏԱՑՎԱԾ ՀԱՂՈՐԴԱԳՐՈՒԹՅՈՒՆ")
            print(f"   Queue: {method.routing_key or properties.reply_to or 'N/A'}")
            
            if method.exchange:
                print(f"   Exchange: {method.exchange}")
                if method.routing_key:
                    print(f"   Routing Key: {method.routing_key}")
            
            if properties.headers:
                print(f"   Headers: {properties.headers}")
            
            if properties.priority is not None:
                print(f"   Priority: {properties.priority}")
            
            if properties.correlation_id:
                print(f"   Correlation ID: {properties.correlation_id}")
            
            print(f"   Body: {body.decode() if isinstance(body, bytes) else body}")
            print(f"{'='*60}")
            
            # ACK (հաստատում) եթե պետք է
            if not properties.reply_to:  # Եթե RPC պատասխան չէ
                ch.basic_ack(delivery_tag=method.delivery_tag)
            
            # RPC պատասխան
            if properties.reply_to:
                response = f"RPC պատասխան {properties.correlation_id}-ի համար"
                ch.basic_publish(
                    exchange='',
                    routing_key=properties.reply_to,
                    properties=pika.BasicProperties(
                        correlation_id=properties.correlation_id
                    ),
                    body=response
                )
                print(f"RPC պատասխան ուղարկված է")
        
        # Սպառում բոլոր queue-ներից
        for queue_name in [
            'simple_queue',
            'durable_queue', 
            'exclusive_queue',
            'auto_delete_queue',
            'advanced_queue',
            'dead_letter_queue',
            queues['fanout_queue'],
            queues['direct_queue'],
            queues['topic_queue'],
            queues['headers_queue']
        ]:
            self.channel.basic_consume(
                queue=queue_name,
                on_message_callback=callback,
                auto_ack=False
            )
        
        self.channel.start_consuming()
    
    def close(self):
        self.connection.close()

if __name__ == "__main__":
    rabbitmq = CompleteRabbitMQExample()
    
    # 1. Ստեղծել բոլոր տեսակները
    queues = rabbitmq.setup_all_exchanges_queues()
    
    # 2. Ուղարկել բոլոր հաղորդագրությունները
    rabbitmq.send_all_message_types()
    
    # 3. Սպառել հաղորդագրությունները (2 վայրկյան սպասելուց հետո)
    time.sleep(2)
    print("\n" + "="*60)
    print("ՍՊԱՌՈՂԻ ԳՈՐԾԱՐԿՈՒՄ")
    print("="*60)
    
    try:
        rabbitmq.start_consumer(queues)
    except KeyboardInterrupt:
        print("\n\n Դադարեցված է")
    finally:
        rabbitmq.close()