#!/usr/bin/env python

"""
[Customer Support Percolation] Data Generator
This script is part of the "Customer Support Percolation" project developped during my Insight Fellowship program (NYC Jan 2018).
It can generate customer click-stream data that is used as main input for the data pipeline.
This data would be collected on the customer support website webservers.
"""

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
		sitemap_cfg = (config['sitemap'])
		self.page_list = range(sitemap_cfg['pages_num'])
		self.sitemap = ([[random.choice(self.page_list) #todo: point towards more realistic connections
			for link in range(sitemap_cfg['links_per_page'])] 
			for page in self.page_list])


	def generate_data(self, sessions, n, buffer_limit):
		buff = list()
		self.session_states = [None]*sessions
		buff_counter = 0
		is_first_flush = True

		for rec in range(n):
			session_id = random.choice(range(sessions))
			buff.append(self.generate_new_record(session_id))
			buff_counter += 1

			if buff_counter % buffer_limit == 0:
				df = pd.DataFrame(buff)
				df.columns = ['epochtime','userid','pageid_origin','pageid_target','case_status']
				df.to_csv(cfg['output_file_path'], mode='w' if is_first_flush else 'a',
				 header=is_first_flush, index=False)
				buff=list()
				is_first_flush = False

	
	def generate_new_record(self, session_id):
		new_record = list()
		session_state = self.session_states[session_id]

		new_timestamp = pd.Timestamp.now()
		if session_state == None:
			new_pageid_origin = random.choice(self.page_list)
		else:
			new_pageid_origin = session_state

		new_pageid_target = random.choice(self.page_list)
		new_case_status = np.random.choice([True, False], p=[0.1, 0.9])

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
		

if __name__ == '__main__':

	gen = Generator(cfg)
	gen.generate_data(sessions=10, n=100, buffer_limit=10)