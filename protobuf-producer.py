#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# This is a simple example of the SerializingProducer using protobuf.
#
# To regenerate Protobuf classes you must first install the protobuf
# compiler. Once installed you may call protoc directly or use make.
#
# See the protocol buffer docs for instructions on installing and using protoc.
# https://developers.google.com/protocol-buffers/docs/pythontutorial
#
# After installing protoc execute the following command from the examples
# directory to regenerate the user_pb2 module.
# `make`
#
import argparse
from uuid import uuid4


# Protobuf generated class; resides at ./user_pb2.py
import homeAutomation_pb2
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


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
    
    time_now = datetime.utcnow()
    for i in data:
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
        try:
            producer.produce(topic=topic, key=str(uuid4()), value=kafka_data)
        except Exception as error:
            clear_start_time=datetime.utcnow() 
            sleep(1)
            producer.poll(1)
            buffer_clear_time+= (datetime.utcnow()-clear_start_time).total_seconds()
            print(buffer_clear_time)
            buffer_clear_count+=1
            print(error, buffer_clear_count)
            producer.produce(topic=topic, key=str(uuid4()), value=kafka_data)
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
    }
    header=["total_data",
            "buffer_clear_count",
            "actual_docs_per_second",
            "overall_time_taken",
            "buffer_clear_time",
            "transfer_time_taken",
            "overall_docs_per_second"
            ]
    with open("kafka_protobuf.csv", "a") as f:
        writer = csv.DictWriter(
            f, fieldnames=header)
        writer = csv.DictWriter(
        f, fieldnames=header)
        import os
        if not os.path.isfile("kafka_protobuf.csv"):
            writer.writeheader() 
        writer.writerow(csv_data)



if __name__ == '__main__':

    main()