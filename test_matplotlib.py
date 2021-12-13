import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

avro_data = pd.read_csv("kafka_avro.csv")
json_data = pd.read_csv("kafka_json.csv")
protobuf_data = pd.read_csv("kafka_protobuf.csv") 
rabbit_mq_data = pd.read_csv("rabbit_mq.csv")
fig, (ax1, ax2) = plt.subplots(nrows=2, sharex=True)
plt.figure(figsize=(16, 6), dpi=300)
plt.subplot(1,2,1)
plt.xlim([100,3000000])
# plt.tight_layout()
# total_data,buffer_clear_count,actual_docs_per_second,overall_time_taken,buffer_clear_time,transfer_time_taken,overall_docs_per_second
plt.plot('total_data', 'overall_time_taken',  linestyle='--', marker='o',data=avro_data, label='Kafka Avro')
plt.plot('total_data', 'overall_time_taken',  linestyle='--', marker='o',data=json_data, label='Kafka JSON')
plt.plot('total_data', 'overall_time_taken',  linestyle='--', marker='o',data=protobuf_data, label="Kafka Protobuf")
plt.plot('total_data', 'overall_time_taken',  linestyle='--', marker='o',data=rabbit_mq_data, label="Rabbit MQ")
plt.legend()
plt.xlabel("total data")
plt.ylabel("total time taken in seconds")
plt.xticks(list(plt.xticks()[0]) + [3000000])
avro_data['dataset_size'] = (avro_data['dataset_size_bytes']/1024)/1024
json_data['dataset_size'] = (json_data['dataset_size_bytes']/1024)/1024
protobuf_data['dataset_size'] = (protobuf_data['dataset_size_bytes']/1024)/1024
rabbit_mq_data['dataset_size'] = (rabbit_mq_data['dataset_size_bytes']/1024)/1024
plt.subplot(1,2,2)
plt.plot('dataset_size', 'overall_time_taken',  linestyle='--', marker='o',data=avro_data, label='Kafka Avro')
plt.plot('dataset_size', 'overall_time_taken',  linestyle='--', marker='o',data=json_data, label='Kafka JSON')
plt.plot('dataset_size', 'overall_time_taken',  linestyle='--', marker='o',data=protobuf_data, label="Kafka Protobuf")
plt.plot('dataset_size', 'overall_time_taken',  linestyle='--', marker='o',data=rabbit_mq_data, label="Rabbit MQ")
plt.legend()
plt.xlabel("dataset size in mega bytes")
plt.ylabel("total time taken in seconds")



# show graph
plt.show()
plt.savefig("result_1.png")