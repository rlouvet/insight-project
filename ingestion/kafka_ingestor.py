from kafka import KafkaProducer
import json
import pandas as pd
import os

kafka_server = os.environ['KAFKA_SERVER']
kafka_port = os.environ['KAFKA_PORT']

producer = KafkaProducer(bootstrap_servers=':'.join([kafka_server, kafka_port]), value_serializer=lambda m: m.encode('ascii'))
df = pd.read_csv('../data_generator/generated_clickstreams.csv')

for i in range(len(df)):
	#print('record', i, ':', df.iloc[i].to_dict())
	message = df.iloc[i].to_json()
	
	print('record', i, '(raw message):', message)
	print('record', i, '(json.dumps):', json.dumps(message))
	print('type(message):', type(message))

	producer.send(topic='clickstreams-test', value=message)

producer.flush()