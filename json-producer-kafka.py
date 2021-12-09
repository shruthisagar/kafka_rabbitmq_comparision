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
# This is a simple example of the SerializingProducer using JSON.
#
from uuid import uuid4


from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer



def main():
    topic = "jsonSerializer"

    schema_str = """
        {
  "$schema": "http://json-schema.org/draft-04/schema#",
  "title": "HomeAutomation",
  "description": "A Home Automation Document",
  "type": "object",
  "properties": {
    "time": {
      "type": "string"
    },
    "use": {
      "type": "string"
    },
    "gen": {
      "type": "string"
    },
    "Houseoverall": {
      "type": "string"
    },
    "Dishwasher": {
      "type": "string"
    },
    "Furnace1": {
      "type": "string"
    },
    "Furnace2": {
      "type": "string"
    },
    "Homeoffice": {
      "type": "string"
    },
    "Fridge": {
      "type": "string"
    },
    "Winecellar": {
      "type": "string"
    },
    "Garagedoor": {
      "type": "string"
    },
    "Kitchen12": {
      "type": "string"
    },
    "Kitchen14": {
      "type": "string"
    },
    "Kitchen38": {
      "type": "string"
    },
    "Barn": {
      "type": "string"
    },
    "Well": {
      "type": "string"
    },
    "Microwave": {
      "type": "string"
    },
    "Livingroom": {
      "type": "string"
    },
    "Solar": {
      "type": "string"
    },
    "temperature": {
      "type": "string"
    },
    "icon": {
      "type": "string"
    },
    "humidity": {
      "type": "string"
    },
    "visibility": {
      "type": "string"
    },
    "summary": {
      "type": "string"
    },
    "apparentTemperature": {
      "type": "string"
    },
    "pressure": {
      "type": "string"
    },
    "windSpeed": {
      "type": "string"
    },
    "cloudCover": {
      "type": "string"
    },
    "windBearing": {
      "type": "string"
    },
    "precipIntensity": {
      "type": "string"
    },
    "dewPoint": {
      "type": "string"
    },
    "precipProbability": {
      "type": "string"
    },
    "text_msg": {
      "type": "string"
    }
  },
  "required": [
    "time",
    "use",
    "gen",
    "Houseoverall",
    "Dishwasher",
    "Furnace1",
    "Furnace2",
    "Homeoffice",
    "Fridge",
    "Winecellar",
    "Garagedoor",
    "Kitchen12",
    "Kitchen14",
    "Kitchen38",
    "Barn",
    "Well",
    "Microwave",
    "Livingroom",
    "Solar",
    "temperature",
    "icon",
    "humidity",
    "visibility",
    "summary",
    "apparentTemperature",
    "pressure",
    "windSpeed",
    "cloudCover",
    "windBearing",
    "precipIntensity",
    "dewPoint",
    "precipProbability",
    "text_msg"
  ]
}
    """
    schema_registry_conf = {'url': 'http://127.0.0.1:8081'}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    json_serializer = JSONSerializer(schema_str, schema_registry_client)

    producer_conf = {'bootstrap.servers':"localhost:9092",
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': json_serializer}

    producer = SerializingProducer(producer_conf)

        # Serve on_delivery callbacks from previous calls to produce()
    
    import csv
    reader =  csv.DictReader(open('output.csv'))
    data = []
    buffer_clear_time=0
    buffer_clear_count=0
    for r in reader:
        data.append(r)
    data = data[:100000]
    print("starting")
    from datetime import datetime
    
    from time import sleep
    time_now = datetime.utcnow()
    try:
        for i in data:
            producer.produce(topic=topic, key=str(uuid4()), value=i)
    except Exception as error:
        clear_start_time=datetime.utcnow() 
        sleep(1)
        producer.poll(1)
        buffer_clear_time+= (datetime.utcnow()-clear_start_time).total_seconds()
        print(buffer_clear_time)
        buffer_clear_count+=1
        print(error, buffer_clear_count)
        producer.produce(topic=topic, key=str(uuid4()), value=i)
    print("done")
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
    with open("kafka_json.csv", "a") as f:
        writer = csv.DictWriter(
            f, fieldnames=header)
        writer = csv.DictWriter(
        f, fieldnames=header)
        import os
        if not os.path.isfile("kafka_json.csv"):
            writer.writeheader() 
        writer.writerow(csv_data)


if __name__ == '__main__':

    main()