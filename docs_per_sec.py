import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

avro_data = pd.read_csv("kafka_avro.csv")
json_data = pd.read_csv("kafka_json.csv")
protobuf_data = pd.read_csv("kafka_protobuf.csv") 
rabbit_mq_data = pd.read_csv("rabbit_mq.csv")
fig, ax = plt.subplots(1, 1, figsize=(12, 10), subplot_kw={'projection': '3d'})

# convenience wrapper for plotting function
def plot_3d(df, label):
    ax.plot(df.total_data, df.overall_time_taken, df.overall_docs_per_second, label=label, marker="o")
# reshape with melt(), then plot
# plot_3d(pd.melt(avro_data, id_vars='total_data', var_name='overall_time_taken', value_name='overall_docs_per_second'))
# plot_3d(pd.melt(json_data, id_vars='total_data', var_name='overall_time_taken', value_name='overall_docs_per_second'))
# plot_3d(pd.melt(protobuf_data, id_vars='total_data', var_name='overall_time_taken', value_name='overall_docs_per_second'))
# plot_3d(pd.melt(rabbit_mq_data, id_vars='total_data', var_name='overall_time_taken', value_name='overall_docs_per_second'))
plot_3d(avro_data, "Kafka Avro")
plot_3d(json_data, "Kafka JSON")
plot_3d(protobuf_data, "Kafka Protobuf")
plot_3d(rabbit_mq_data, "RabbitMQ")

# label axes
ax.set_xlabel('Data')
ax.set_ylabel('Time Taken in seconds')
ax.set_zlabel('Documents per second')

# optional view configurations
ax.elev = 10
ax.axim = 40
plt.legend()
plt.show()
plt.savefig("3d.png")