import pika
# import json
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='hello')

def callback(ch, method, properties, body):
  print("[x] Received: %r" % (body,))

channel.basic_consume(on_message_callback=callback, queue='hello')
channel.start_consuming()