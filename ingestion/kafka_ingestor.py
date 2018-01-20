from kafka import KafkaProducer
import json
import pandas as pd

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda m: m.encode('ascii'))
df = pd.read_csv('../data_generator/generated_clickstreams.csv')

for i in range(len(df)):
	#print('record', i, ':', df.iloc[i].to_dict())
	message = df.iloc[i].to_json()
	
	print('record', i, '(raw message):', message)
	print('record', i, '(json.dumps):', json.dumps(message))
	print('type(message):', type(message))

	producer.send(topic='clickstreams-test-2', value=message)

producer.flush()