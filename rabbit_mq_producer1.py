import pika
import json, os
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_delete(queue="hello")
channel.queue_declare(queue='hello')
import csv
reader =  csv.DictReader(open('output.csv'))
data = []
for r in reader:
    data.append(r)
data=data+data+data+data+data

from datetime import datetime
print("Starting data transfer")
time_now = datetime.utcnow()
for body in data:
    channel.basic_publish(exchange='', routing_key='hello', body=json.dumps(body))
    
time_taken = (datetime.utcnow()-time_now).total_seconds()
print("end of data transfer")

connection.close()

header=["total_data",
           "time_taken",
           "docs_per_second"
            ]
csv_data = {
    "total_data": len(data),
    "time_taken": time_taken,
    "docs_per_second": len(data)/time_taken
}
with open("rabbit_mq.csv", "a") as f:
    writer = csv.DictWriter(
        f, fieldnames=header)
    if not os.path.isfile("rabbit_mq.csv"):
        writer.writeheader()   
    writer.writerow(csv_data)