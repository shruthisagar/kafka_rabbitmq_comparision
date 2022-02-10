import pika
import json, os

import psutil
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_delete(queue="homeAutomation")
channel.queue_declare(queue='homeAutomation')
import csv
reader =  csv.DictReader(open('output.csv'))
data = []
for r in reader:
    data.append(r)
data=data+data+data
from datetime import datetime
print("Starting data transfer")

memory_profiling = []
time_now = datetime.utcnow()
for index, body in enumerate(data):
    if index%1000 == 0:
        profiling_data = psutil.virtual_memory()._asdict()
        profiling_data['dataset'] = index
        profiling_data['buffer_error'] = False
        profiling_data['total_dataset'] = len(data)
        memory_profiling.append(profiling_data)
    channel.basic_publish(exchange='', routing_key='homeAutomation', body=json.dumps(body))
profiling_data = psutil.virtual_memory()._asdict()
profiling_data['dataset'] = index
profiling_data['buffer_error'] = False
profiling_data['total_dataset'] = len(data)
memory_profiling.append(profiling_data)
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
with open('./rabbit_mq/memory_profiling.csv', "a") as f:
            new_header = memory_profiling[0].keys()
            writer = csv.DictWriter(
                f, fieldnames=new_header)
            writer = csv.DictWriter(
            f, fieldnames=new_header)
            # writer.writeheader() 
            writer.writerows(memory_profiling)