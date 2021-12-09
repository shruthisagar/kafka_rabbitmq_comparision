from datetime import datetime
from time import sleep
import csv, os
from confluent_kafka import avro

from confluent_kafka.avro import AvroProducer
reader =  csv.DictReader(open('output.csv'))
data = []
for r in reader:
    data.append(r)
import traceback
data = data+data+data+data
value_schema = avro.load('schema/producer/ValueSchema.avsc')
key_schema = avro.load('schema/producer/KeySchema.avsc')


avroProducer = AvroProducer(
    {'bootstrap.servers': 'localhost:9092', 'schema.registry.url': 'http://127.0.0.1:8081'},
    default_key_schema=key_schema, default_value_schema=value_schema)

# for i in range(0, 100000):
#     value = {"name": "Yuva", "favorite_number": 10, "favorite_color": "green", "age": i}
#     avroProducer.produce(topic='my_topic', value=value, key=key, key_schema=key_schema, value_schema=value_schema)
#     sleep(0.01)
#     print(i)
buffer_clear_time=0
buffer_clear_count=0
from datetime import datetime
text_to_write="\n*******************************************\n"+str(datetime.utcnow())+" Starting data transfer\n"
print("Starting data transfer")
time_now = datetime.utcnow()
try:
    for index, i in enumerate(data):
        try:
            avroProducer.produce(topic='homeAutomation', value=i, key=i, key_schema=key_schema, value_schema=value_schema)
        except Exception as error:
            
            clear_start_time=datetime.utcnow() 
            sleep(1)
            avroProducer.poll(1)
            buffer_clear_time+= (datetime.utcnow()-clear_start_time).total_seconds()
            print(buffer_clear_time)
            buffer_clear_count+=1
            print(error, buffer_clear_count)
            avroProducer.produce(topic='homeAutomation', value=i, key=i, key_schema=key_schema, value_schema=value_schema)
    print("data transfer completed")
    time_taken = (datetime.utcnow()-time_now).total_seconds()
    actual_time_taken = time_taken-buffer_clear_time
    print(actual_time_taken, "actual_time_taken")
    print(time_taken, "time_taken")
    print(buffer_clear_time, "buffer_clear_time")
    csv_data = {
        "total_data": len(data),
        "buffer_clear_count":buffer_clear_count,
        "actual_docs_per_second":len(data)/time_taken,
        "overall_time_taken":time_taken,
        "buffer_clear_time":buffer_clear_time,
        "transfer_time_taken":actual_time_taken,
        "overall_docs_per_second":len(data)/actual_time_taken,
    }
    header=["total_data",
            "buffer_clear_count",
            "actual_docs_per_second",
            "overall_time_taken",
            "buffer_clear_time",
            "transfer_time_taken",
            "overall_docs_per_second"
            ]
    with open("kafka_avro.csv", "a") as f:
        writer = csv.DictWriter(
            f, fieldnames=header)
        writer = csv.DictWriter(
        f, fieldnames=header)
        if not os.path.isfile("kafka_avro.csv"):
            writer.writeheader() 
        writer.writerow(csv_data)
    avroProducer.flush(10) 
except Exception as error:
    print("final error----------------------")
    print(traceback.format_exc().splitlines())
