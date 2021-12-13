import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

avro_data = pd.read_csv("kafka_avro.csv")
json_data = pd.read_csv("kafka_json.csv")
protobuf_data = pd.read_csv("kafka_protobuf.csv") 
rabbit_mq_data = pd.read_csv("rabbit_mq.csv")
my_dict = dict(x=avro_data,y=json_data,z=protobuf_data,a=rabbit_mq_data)
# data = pd.DataFrame (my_dict)
fig, ax = plt.subplots()
data = pd.concat([avro_data, json_data, protobuf_data, rabbit_mq_data]).reset_index(drop=True)
ax= sns.lineplot(x='total_data', y='overall_time_taken', data=data)
ax1 = sns.lineplot(x='overall_time_taken', y='dataset_size_bytes', data=data)

plt.savefig("test.png")