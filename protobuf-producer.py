from uuid import uuid4
import homeAutomation_pb2
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
import sys, psutil

def main():
    topic = "HomeAutomationProtoBuf"

    schema_registry_conf = {'url': 'http://127.0.0.1:8081'}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    protobuf_serializer = ProtobufSerializer(homeAutomation_pb2.HomeAutomation,
                                             schema_registry_client)

    producer_conf = {'bootstrap.servers': 'localhost:9092',
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': protobuf_serializer}

    producer = SerializingProducer(producer_conf)

   
    import csv
    reader =  csv.DictReader(open('output.csv'))
    data = []
    buffer_clear_time=0
    buffer_clear_count=0
    for r in reader:
        data.append(r)
    data = data+data+data+data
    from datetime import datetime
    
    from time import sleep
    print("starting")
    memory_profiling = []
    time_now = datetime.utcnow()
    for index, i in enumerate(data):
        kafka_data = homeAutomation_pb2.HomeAutomation(time=i["time"],
                use=i["use"],
                gen=i["gen"],
                Houseoverall=i["Houseoverall"],
                Dishwasher=i["Dishwasher"],
                Furnace1=i["Furnace1"],
                Furnace2=i["Furnace2"],
                Homeoffice=i["Homeoffice"],
                Fridge=i["Fridge"],
                Winecellar=i["Winecellar"],
                Garagedoor=i["Garagedoor"],
                Kitchen12=i["Kitchen12"],
                Kitchen14=i["Kitchen14"],
                Kitchen38=i["Kitchen38"],
                Barn=i["Barn"],
                Well=i["Well"],
                Microwave=i["Microwave"],
                Livingroom=i["Livingroom"],
                Solar=i["Solar"],
                temperature=i["temperature"],
                icon=i["icon"],
                humidity=i["humidity"],
                visibility=i["visibility"],
                summary=i["summary"],
                apparentTemperature=i["apparentTemperature"],
                pressure=i["pressure"],
                windSpeed=i["windSpeed"],
                cloudCover=i["cloudCover"],
                windBearing=i["windBearing"],
                precipIntensity=i["precipIntensity"],
                dewPoint=i["dewPoint"],
                precipProbability=i["precipProbability"],
                text_msg=i["text_msg"]
                )
        if index%1000 == 0:
                profiling_data = psutil.virtual_memory()._asdict()
                profiling_data['dataset'] = index
                profiling_data['buffer_error'] = False
                profiling_data['total_dataset'] = len(data)
                memory_profiling.append(profiling_data)
        try:
            producer.produce(topic=topic, key=str(uuid4()), value=kafka_data)
        except Exception as error:
            clear_start_time=datetime.utcnow() 
            sleep(1)
            profiling_data = psutil.virtual_memory()._asdict()
            profiling_data['total_dataset'] = len(data)
            profiling_data['dataset'] = index
            profiling_data['buffer_error'] = True
            memory_profiling.append(profiling_data)
            producer.poll(1)
            buffer_clear_time+= (datetime.utcnow()-clear_start_time).total_seconds()
            print(buffer_clear_time)
            buffer_clear_count+=1
            print(error, buffer_clear_count)
            producer.produce(topic=topic, key=str(uuid4()), value=kafka_data)
    
    profiling_data = psutil.virtual_memory()._asdict()
    profiling_data['dataset'] = index
    profiling_data['total_dataset'] = len(data)
    profiling_data['buffer_error'] = False
    memory_profiling.append(profiling_data)
    producer.flush(10)
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
        "dataset_size":sys.getsizeof(data)
    }
    header=["total_data",
            "buffer_clear_count",
            "actual_docs_per_second",
            "overall_time_taken",
            "buffer_clear_time",
            "transfer_time_taken",
            "overall_docs_per_second",
            'dataset_size'
            ]
    with open("kafka_protobuf.csv", "a") as f:
        writer = csv.DictWriter(
            f, fieldnames=header)
        writer = csv.DictWriter(
        f, fieldnames=header)
        # writer.writeheader() 
        writer.writerow(csv_data)

    with open('./protobuf/memory_profiling.csv', "a") as f:
            new_header = memory_profiling[0].keys()
            writer = csv.DictWriter(
                f, fieldnames=new_header)
            writer = csv.DictWriter(
            f, fieldnames=new_header)
            # writer.writeheader() 
            writer.writerows(memory_profiling)

if __name__ == '__main__':

    main()