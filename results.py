import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

avro_data = pd.read_csv("kafka_avro.csv")
json_data = pd.read_csv("kafka_json.csv")
protobuf_data = pd.read_csv("kafka_protobuf.csv") 
rabbit_mq_data = pd.read_csv("rabbit_mq.csv")

plt1 = plt.figure(figsize=(8, 6), dpi=300)
# plt.xlim([100,3000000])
import pdb
# pdb.set_trace()
# plt.tight_layout()
# total_data,buffer_clear_count,actual_docs_per_second,overall_time_taken,buffer_clear_time,transfer_time_taken,overall_docs_per_second
# plt.plot('total_data', 'overall_time_taken',  linestyle='--', marker='o',data=avro_data, label='Kafka Avro')
plt.plot(np.linspace(0, 100, 25000),'overall_docs_per_second', linestyle='--', marker='o',data=avro_data, label="Kafka Avro Docs Per Second")


# plt.plot('total_data', 'overall_time_taken',  linestyle='--', marker='o',data=json_data, label='Kafka JSON')
plt.plot(np.linspace(0, 100, 25000),'overall_docs_per_second', linestyle='--', marker='o',data=json_data, label="Kafka JSON Docs Per Second")
# plt.plot('total_data', 'overall_time_taken',  linestyle='--', marker='o',data=protobuf_data, label="Kafka Protobuf")
plt.plot(np.linspace(0, 100, 25000),'overall_docs_per_second', linestyle='--', marker='o',data=protobuf_data, label="Docs Kafka Protobuf Per Second")
# plt.plot('total_data', 'overall_time_taken',  linestyle='--', marker='o',data=rabbit_mq_data, label="Rabbit MQ")
plt.plot(np.linspace(0, 100, 25000),'overall_docs_per_second', linestyle='--', marker='o',data=rabbit_mq_data, label="Rabbit MQ Docs Per Second")
plt.legend()
plt.xlabel("total data")
plt.ylabel("documents per second")
plt.xticks(list(plt.xticks()[0]) + [3000000])

# show graph
plt.show()
plt.savefig("result1.png")