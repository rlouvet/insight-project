#!/usr/bin/env python

"""
[Customer Support Percolation] Data Generator
This script is part of the "Customer Support Percolation" project developped
during my Insight Fellowship program (NYC Jan 2018).
It can generate customer click-stream data that is used as main input for the data pipeline.
This data would be collected on the customer support website webservers.
"""
import sys, os, json, datetime, random
import argparse
import yaml
import numpy as np
import pandas as pd
from kafka import KafkaProducer

with open("config.yml", 'r') as ymlfile:
	cfg = yaml.load(ymlfile)

def timestamp_millisec():
    return int((datetime.datetime.utcnow() - datetime.datetime(1970, 1, 1)).total_seconds() * 1000)

class Generator:
	"""
	The main generator class
	"""

	def __init__(self, config):
		kafka_server = os.environ['KAFKA_SERVER']
		kafka_port = os.environ['KAFKA_PORT']
		self.producer = KafkaProducer(bootstrap_servers=':'.join([kafka_server, kafka_port]),
		 value_serializer=lambda m: m.encode('ascii'))

		sitemap_cfg = config['sitemap']
		walker_cfg = config['walker']
		self.page_list = range(sitemap_cfg['pages_num'])
		self.sitemap = ([[random.choice(self.page_list) #todo: point towards more realistic connections
			for link in range(sitemap_cfg['links_per_page'])]
			for page in self.page_list])
		self.resolved_probability = walker_cfg['resolved_probability']

		self.record_keys = ['epochtime','userid','pageid_origin','pageid_target','case_status']


	def generate_data(self, n_sessions, n_records, buffer_limit, destination):
		self.n_sessions = n_sessions
		self.n_records = n_records
		self.buffer_limit = buffer_limit
		self.destination = destination

		self.buff = list()
		self.session_states = [None]*n_sessions
		self.buff_counter = 0
		self.is_first_flush = True

		if self.n_records == 0:
			while(True):
				self.run_loop()
		else:
			for rec in range(n_records):
				self.run_loop()


	def generate_new_record(self, session_id):
		new_record = list()
		session_state = self.session_states[session_id]

		new_timestamp = timestamp_millisec()
		if session_state == None:
			new_pageid_origin = random.choice(self.page_list)
		else:
			new_pageid_origin = session_state

		new_pageid_target = random.choice(self.page_list)
		new_case_status = np.random.choice([True, False],
		  p=[self.resolved_probability, 1-self.resolved_probability])

		if new_case_status == True:
			self.session_states[session_id] = None
		else:
			self.session_states[session_id] = new_pageid_target

		new_record = [
			new_timestamp,
			session_id,
			new_pageid_origin,
			new_pageid_target,
			new_case_status
		]

		return new_record

	def run_loop(self):
		session_id = random.choice(range(self.n_sessions))
		self.buff.append(self.generate_new_record(session_id))
		self.buff_counter += 1

		if self.buff_counter % self.buffer_limit == 0:
			self.flush()

	def flush(self):
		if self.destination == 'kafka':
			print('sending to kafka')
			self.producer.send(topic='clickstreams-topic', value={'sample-key': 'sample-value'})
			self.producer.flush()
			#TODO: Need proper serialization technology here

			for record in self.buff:
				message = dict(zip(self.record_keys, map(str, record)))
				print('message:', str(message))
				print('type(message):', type(message))
				self.producer.send(topic='clickstreams-topic', value=json.dumps(message))
			self.producer.flush()
		else:
			df = pd.DataFrame(self.buff)
			df.columns = ['epochtime','userid','pageid_origin','pageid_target','case_status']
			df.to_csv(cfg['output_file_path'], mode='w' if self.is_first_flush else 'a',
			  header=self.is_first_flush, index=False)


		self.buff=list()
		self.is_first_flush = False

if __name__ == '__main__':

	parser = argparse.ArgumentParser(description='Generate messages to be processed by the data pipeline.')
	parser.add_argument('n_sessions', type=int,
	  help='number of user sessions to be generated')
	parser.add_argument('n_records', type=int,
	  help='number of records/messages to be generated')
	parser.add_argument('buffer_limit', type=int,
	  help='count threshold before flushing buffer into destination')
	parser.add_argument('destination', type=str,
	  help='where the data should be sent: kafka or CSV file')
	args = parser.parse_args()

	gen = Generator(cfg)
	gen.generate_data(
		n_sessions=args.n_sessions,
		n_records=args.n_records,
		buffer_limit=args.buffer_limit,
		destination=args.destination
		)
