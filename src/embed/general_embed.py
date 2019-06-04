# -*- coding: utf-8 -*-

# 1. 首先读取数据库，id, aid, keyword, sentence
# 2. 计算句子的语义向量(with bert)
# 3. 存入数据库 id, aid, keyword, sentence, kw_embed, sent_embed, create_time

import numpy as np
import queue
from threading import Thread
import time
import pickle
import os
import glob

import sys
sys.path.append("..")

from dbbase import db
import embedding
# from mlog import mlog



# test
class Config_textsum(object):
	host="10.0.0.171"
	user="root"
	passwd="123456"
	db="textsum"
	poolSize=3

class Config_titan(object):
	host="10.0.0.114"
	user="titan_readonly"
	passwd="1lfTyBSaiPyqxtGG"
	db="titan"
	poolSize=3

class GeneralEmbed(object):
	"""docstring for GeneralEmbed"""
	def __init__(self):

		self.bert = embedding.Embedding("10.0.0.171", 5555)
		self.queue_sent = queue.Queue()
		self.queue_kw = queue.Queue()
		self.queue_insert = queue.Queue()

		cg = Config_textsum()
		self.db_pool_textsum = db.MysqlConnPool(
			host=cg.host,
			user=cg.user,
			passwd=cg.passwd,
			db=cg.db,
			poolSize=cg.poolSize)
		self.dbcon_textsum = self.db_pool_textsum.Acquire()

		cg = Config_titan()
		self.db_pool_titan = db.MysqlConnPool(
			host=cg.host,
			user=cg.user,
			passwd=cg.passwd,
			db=cg.db,
			poolSize=cg.poolSize)
		self.dbcon_titan = self.db_pool_titan.Acquire()

	def __del__(self):
		if self.dbcon_textsum != None:
			self.db_pool_textsum.Release(self.dbcon_textsum)

		if self.dbcon_titan != None:
			self.db_pool_titan.Release(self.dbcon_titan)

	def load_data_from_db(self):
		sql = "select id,aid,keyword,content from titan_section;"
		self.datas = self.dbcon_titan.query(sql)

	def put_data_into_queue(self):
		for data in self.datas:
			self.queue_sent.put(data)
			self.queue_kw.put(data)

	def get_sentence_embed(self):
		print("start get_sentence_embed thread success.")
		str_list = []
		tup_list = []
		while self.queue_sent.qsize() > 0:
			data = self.queue_sent.get()
			id = data[0]
			aid = data[1]
			keyword = data[2]
			sentence = data[3]
			if sentence == "":
				continue
			# print((id, aid, keyword, sentence))
			tup_list.append((id, aid, keyword, sentence))
			str_list.append(sentence)
			if len(str_list) >= 10:
				emb_list = self.bert.get_embedding(str_list)
				# 插入数据库
				for index in range(len(str_list)):
					tup = tup_list[index]
					emb = emb_list[index]
					put_data = (tup[0],tup[1], tup[2], tup[3], emb)
					self.queue_insert.put(put_data)
				print("put data success")
				# 清空缓存
				tup_list = []
				str_list = []
				emb_list = []

		emb_list = self.bert.get_embedding(str_list)
		# 插入数据库
		for index in range(len(str_list)):
			tup = tup_list[index]
			emb = emb_list[index]
			put_data = (tup[0],tup[1], tup[2], tup[3], emb)
			self.queue_insert.put(put_data)		

	def insert_db(self):
		print("start ------------insert_db thread success.")
		while True:

			data = self.queue_insert.get()

			file_name = "embed/{}_{}.bin".format(data[1],data[0])
			with open(file_name,"wb") as f:
				pickle.dump(data, f)

			# sql = "INSERT INTO `session_embed`(`id`, `embed_sent`) VALUES (%s,%s)" % ( data[0], data[4])
			# try:
			# 	self.dbcon_textsum.execute(sql)
			# 	self.dbcon_textsum.commit()
			# except Exception as e:
			# 	print(e)
			# time.sleep(3)

	def load_data(self, path="embed"):
		if os.path.exists(path) is not True:
			return
		for f_name in glob.glob(os.path.join(path, "*.bin")):
			with open(f_name, "rb") as f:
				data = pickle.load(f)
				yield data
    
	def merge_embed(self):
		embed_dict = {}
		iter_data = self.load_data()
		for data in iter_data:
			id = data[0]
			embed_dict[id] = data[4]
			print("total: ",len(embed_dict))
		with open("embe_dict.bin","wb") as f:
			pickle.dump(embed_dict, f)
			print("svave embe_dict success.")

	def consin(self,i,v):
		consin=np.dot(i,v)/(np.linalg.norm(i)*(np.linalg.norm(v)))
		return consin


	def test(self,path="./"):
		content = "因越军躲藏在一个无名高地上向我军进行猛烈的开火，致使我军无法前进一步。喷火班接到命令后，利用茂密的植物等作掩护悄悄前进，并且顺利抵达距敌暗堡30米处时迅速占领射击位置"
		data = {}
		with open("embe_dict.bin", "rb") as f:
			data = pickle.load(f)

		unsort_list = []
		vec = self.bert.get_embedding([content])[0]
		for k,v in data.items():
			# print(k,v.shape)
			d = self.consin(vec,v)
			d_s = (k,d)
			unsort_list.append(d_s)

		sorted_list = sorted(unsort_list, key=lambda dd: dd[1],reverse=True)

		for index in range(10):
			print(sorted_list[index])







	def begin(self):
		self.load_data_from_db()
		self.put_data_into_queue()

		t1 = Thread(target=self.get_sentence_embed, args=())
		t2 = Thread(target=self.insert_db, args=())
		
		t2.daemon = True
		
		t1.daemon = True
		t1.start()
		t2.start()


		while True:
			time.sleep(30)
			print("main is alive...")

		

if __name__ == '__main__':
	ge = GeneralEmbed()
	ge.test()
	
