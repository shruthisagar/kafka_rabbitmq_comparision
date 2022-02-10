from datetime import datetime
from time import sleep
import csv, os, psutil
from confluent_kafka import avro

from confluent_kafka.avro import AvroProducer
reader =  csv.DictReader(open('output.csv'))
data = []
for r in reader:
    data.append(r)
data = data+data+data+data
import traceback
# data = data+data+data+data
value_schema = avro.load('schema/producer/ValueSchema.avsc')
key_schema = avro.load('schema/producer/KeySchema.avsc')


avroProducer = AvroProducer(
    {'bootstrap.servers': 'localhost:9092', 'schema.registry.url': 'http://127.0.0.1:8081'},
    default_key_schema=key_schema, default_value_schema=value_schema)

buffer_clear_time=0
buffer_clear_count=0
from datetime import datetime
text_to_write="\n*******************************************\n"+str(datetime.utcnow())+" Starting data transfer\n"
print("Starting data transfer")
memory_profiling = []
time_now = datetime.utcnow()
try:
    for index, i in enumerate(data):
        if index%1000 == 0:
                profiling_data = psutil.virtual_memory()._asdict()
                profiling_data['dataset'] = index
                profiling_data['buffer_error'] = False
                profiling_data['total_dataset'] = len(data)
                memory_profiling.append(profiling_data)
        try:
            avroProducer.produce(topic='homeAutomation', value=i, key=i, key_schema=key_schema, value_schema=value_schema)
        except Exception as error:
            profiling_data = psutil.virtual_memory()._asdict()
            profiling_data['total_dataset'] = len(data)
            profiling_data['dataset'] = index
            profiling_data['buffer_error'] = True
            memory_profiling.append(profiling_data)
            clear_start_time=datetime.utcnow() 
            sleep(1)
            avroProducer.poll(1)
            buffer_clear_time+= (datetime.utcnow()-clear_start_time).total_seconds()
            print(buffer_clear_time)
            buffer_clear_count+=1
            print(error, buffer_clear_count)
            avroProducer.produce(topic='homeAutomation', value=i, key=i, key_schema=key_schema, value_schema=value_schema)
    avroProducer.flush(10)
    profiling_data = psutil.virtual_memory()._asdict()
    profiling_data['dataset'] = index
    profiling_data['total_dataset'] = len(data)
    profiling_data['buffer_error'] = False
    memory_profiling.append(profiling_data)
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
    with open('./avro/memory_profiling.csv', "a") as f:
            new_header = memory_profiling[0].keys()
            writer = csv.DictWriter(
                f, fieldnames=new_header)
            writer = csv.DictWriter(
            f, fieldnames=new_header)
            # writer.writeheader() 
            writer.writerows(memory_profiling)
except Exception as error:
    print("final error----------------------")
    print(traceback.format_exc().splitlines())
