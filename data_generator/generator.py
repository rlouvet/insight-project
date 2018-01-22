#!/usr/bin/env python

"""
[Customer Support Percolation] Data Generator
This script is part of the "Customer Support Percolation" project developped during my Insight Fellowship program (NYC Jan 2018).
It can generate customer click-stream data that is used as main input for the data pipeline.
This data would be collected on the customer support website webservers.
"""
import sys
import yaml
import random
import numpy as np
import pandas as pd

with open("config.yml", 'r') as ymlfile:
	cfg = yaml.load(ymlfile)


class Generator:
	"""
	The main generator class
	"""

	def __init__(self, config):
		sitemap_cfg = config['sitemap']
		walker_cfg = config['walker']
		self.page_list = range(sitemap_cfg['pages_num'])
		self.sitemap = ([[random.choice(self.page_list) #todo: point towards more realistic connections
			for link in range(sitemap_cfg['links_per_page'])]
			for page in self.page_list])
		self.resolved_probability = walker_cfg['resolved_probability']


	def generate_data(self, n_sessions, n_records, buffer_limit):
		self.n_sessions = n_sessions
		self.n_records = n_records
		self.buffer_limit = buffer_limit

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

		new_timestamp = pd.Timestamp.now()
		if session_state == None:
			new_pageid_origin = random.choice(self.page_list)
		else:
			new_pageid_origin = session_state

		new_pageid_target = random.choice(self.page_list)
		new_case_status = np.random.choice([True, False], p=[self.resolved_probability, 1-self.resolved_probability])

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
			df = pd.DataFrame(self.buff)
			df.columns = ['epochtime','userid','pageid_origin','pageid_target','case_status']
			df.to_csv(cfg['output_file_path'], mode='w' if self.is_first_flush else 'a',
			  header=self.is_first_flush, index=False)
			self.buff=list()
			self.is_first_flush = False

if __name__ == '__main__':
	n_sessions = int(sys.argv[1])
	n_records = int(sys.argv[2])
	buffer_limit = int(sys.argv[3])

	gen = Generator(cfg)
	gen.generate_data(n_sessions=n_sessions, n_records=n_records, buffer_limit=buffer_limit)
