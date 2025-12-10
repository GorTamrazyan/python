import pika
import json

def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()
    
    # Ստեղծում ենք բոլոր queue-ները
    channel.queue_declare(queue='simple_queue')
    channel.queue_declare(queue='durable_queue', durable=True)
    
    # Ստեղծում ենք exchange-ները
    channel.exchange_declare(exchange='logs', exchange_type='fanout')
    channel.exchange_declare(exchange='direct_logs', exchange_type='direct')
    channel.exchange_declare(exchange='topic_logs', exchange_type='topic')
    
    # Ստեղծում ենք անհատական queue-ներ exchange-ների համար
    # 1. Fanout exchange (բոլորին)
    fanout_queue = channel.queue_declare(queue='', exclusive=True).method.queue
    channel.queue_bind(exchange='logs', queue=fanout_queue)
    
    # 2. Direct exchange (ուղղորդված)
    direct_queue = channel.queue_declare(queue='', exclusive=True).method.queue
    channel.queue_bind(exchange='direct_logs', queue=direct_queue, routing_key='error')
    channel.queue_bind(exchange='direct_logs', queue=direct_queue, routing_key='info')
    
    # 3. Topic exchange (թեմայով)
    topic_queue = channel.queue_declare(queue='', exclusive=True).method.queue
    channel.queue_bind(exchange='topic_logs', queue=topic_queue, routing_key='user.#')
    
    print(" [*] Սպասում եմ հաղորդագրությունների. Դադարեցնելու համար սեղմեք CTRL+C")
    
    def callback(ch, method, properties, body):
        try:
            # Փորձում ենք JSON decode անել
            data = json.loads(body)
            message = json.dumps(data, indent=2, ensure_ascii=False)
        except:
            message = body.decode('utf-8')
        
        print(f"📥 Ստացված [{method.exchange or method.routing_key}]: {message}")
        
        if not method.exchange:  # Եթե ուղղակի queue է
            ch.basic_ack(delivery_tag=method.delivery_tag)
    
    # Սպառում ենք բոլոր queue-ներից
    channel.basic_consume(queue='simple_queue', on_message_callback=callback, auto_ack=False)
    channel.basic_consume(queue='durable_queue', on_message_callback=callback, auto_ack=False)
    channel.basic_consume(queue=fanout_queue, on_message_callback=callback, auto_ack=True)
    channel.basic_consume(queue=direct_queue, on_message_callback=callback, auto_ack=True)
    channel.basic_consume(queue=topic_queue, on_message_callback=callback, auto_ack=True)
    
    channel.start_consuming()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n👋 Դադարեցված է")