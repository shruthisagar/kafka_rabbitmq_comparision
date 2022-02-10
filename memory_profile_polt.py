import pandas as pd
import matplotlib.pyplot as plt

avro_df = pd.read_csv('./avro/memory_profiling.csv')
protobuf_df = pd.read_csv('./protobuf/memory_profiling.csv')
json_df = pd.read_csv('./json/memory_profiling.csv')
rabbit_mq_df = pd.read_csv('./rabbit_mq/memory_profiling.csv')
avro_df['type'] = 'avro'
protobuf_df['type'] = 'protobuf'
json_df['type'] = 'json'
rabbit_mq_df['type'] = 'rabbit_mq'
# overall_df = pd.DataFrame()
overall_df = pd.concat([avro_df, protobuf_df, json_df])[['percent','total_dataset','type']]
avro_group = avro_df.groupby(['total_dataset'],  as_index=False)
protobuf_group = protobuf_df.groupby(['total_dataset'],  as_index=False)
json_group = json_df.groupby(['total_dataset'],  as_index=False)
rabbit_mq_group = rabbit_mq_df.groupby(['total_dataset'],  as_index=False)

avro_result = {}
max_usage = {'avro':{},'json':{}, 'protobuf':{}, 'rabbit_mq':{}}
for _ in avro_group.groups:
  max_usage['avro'][_] = (avro_group.get_group(_)['percent'].max()-avro_group.get_group(_)['percent'].min())*avro_group.get_group(_)['total'].max()/(100*1024*1024)
  avro_result[_] = avro_group.get_group(_)['percent'].value_counts().to_dict()

protobuf_result = {}
for _ in protobuf_group.groups:
  max_usage['protobuf'][_] = (protobuf_group.get_group(_)['percent'].max()-protobuf_group.get_group(_)['percent'].min())*protobuf_group.get_group(_)['total'].max()/(100*1024*1024)
  protobuf_result[_] = protobuf_group.get_group(_)['percent'].value_counts().to_dict()

json_result = {}
for _ in json_group.groups:
  max_usage['json'][_] = (json_group.get_group(_)['percent'].max()-json_group.get_group(_)['percent'].min())*json_group.get_group(_)['total'].max()/(100*1024*1024)
  json_result[_] = json_group.get_group(_)['percent'].value_counts().to_dict()


rabbit_mq_result = {}
for _ in rabbit_mq_group.groups:
  max_usage['rabbit_mq'][_] = (rabbit_mq_group.get_group(_)['percent'].max()-rabbit_mq_group.get_group(_)['percent'].min())*rabbit_mq_group.get_group(_)['total'].max()/(100*1024*1024)
  rabbit_mq_result[_] = rabbit_mq_group.get_group(_)['percent'].value_counts().to_dict()



plt.plot('data_size', 'usage',data=pd.DataFrame(max_usage['protobuf'].items(), columns=['data_size', 'usage']), linestyle='--', marker='o', label='Kafka Protobuf Data usage')
plt.plot('data_size', 'usage',data=pd.DataFrame(max_usage['json'].items(), columns=['data_size', 'usage']), linestyle='--', marker='o', label='Kafka JSON Data usage')
plt.plot('data_size', 'usage',data=pd.DataFrame(max_usage['avro'].items(), columns=['data_size', 'usage']), linestyle='--', marker='o', label='Kafka Avro Data usage')
plt.plot('data_size', 'usage',data=pd.DataFrame(max_usage['rabbit_mq'].items(), columns=['data_size', 'usage']), linestyle='--', marker='o', label='Rabbit MQ Data usage')

plt.legend()
plt.xlabel("total data (records)")
plt.ylabel("total memory usage in MB")
plt.title("Memory consumption across Rabbit MQ\n and various Serialization Techniques of Kafka")
plt.savefig("max_usage.png")