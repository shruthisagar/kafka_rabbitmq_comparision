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
fig, ax = plt.subplots(nrows=2, ncols=2)
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

pd.DataFrame(json_result[100].items(), columns=['usage', 'count']).plot(x="usage", y="count", style='.-',subplots=True, grid=True, title="Sample Data (Unit)",
    layout=(1,1), sharex=True, sharey=False, legend=True,)
pd.DataFrame(protobuf_result[100].items(), columns=['usage', 'count']).plot(x="usage", y="count", style='.-',subplots=True, grid=True, title="Sample Data (Unit)",
    layout=(1,2), sharex=True, sharey=False, legend=True,)
pd.DataFrame(avro_result[100].items(), columns=['usage', 'count']).plot(x="usage", y="count", style='.-',subplots=True, grid=True, title="Sample Data (Unit)",
    layout=(2,1), sharex=True, sharey=False, legend=True,)
pd.DataFrame(rabbit_mq_result[100].items(), columns=['usage', 'count']).plot(x="usage", y="count", style='.-',subplots=True, grid=True, title="Sample Data (Unit)",
    layout=(2,2), sharex=True, sharey=False, legend=True,)
for ax in plt.gcf().axes:
    ax.legend(loc=1)
plt.savefig('100_plot.png')

 