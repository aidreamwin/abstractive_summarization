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
from mlog.mlog import mlog
from textrank.textrank import TextRank

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

		self.textrank = TextRank()

	def load_data_from_titan_section(self,queue_sent):
		sql = "select id,aid,keyword,content from titan_section where keyword is not null order by id ASC;"
		datas = self.dbcon_titan.query(sql)
		for data in datas:
			queue_sent.put(data)

	def get_sentence_embed(self,queue_sent,queue_insert):
		mlog.info("start get_sentence_embed thread success.")
		sentence_list = []
		keyword_list = []
		tup_list = []
		while queue_sent.qsize() > 0:
			data = queue_sent.get()
			id = data[0]
			aid = data[1]
			keyword = data[2]
			sentence = data[3]
			if sentence == "":
				continue
			if keyword == "":
				continue
			# print((id, aid, keyword, sentence))
			tup_list.append((id, aid, keyword, sentence))
			sentence_list.append(sentence)
			keyword_list.append(keyword)
			if len(sentence_list) >= 20:
				keyword_list.extend(sentence_list)
				mlog.debug("begin to get embed")
				emb_list = self.bert.get_embedding(keyword_list)
				# 插入数据库
				for index in range(len(sentence_list)):
					tup = tup_list[index]
					key_emb = emb_list[index]
					sent_emb = emb_list[index+len(sentence_list)]
					put_data = (tup[0],tup[1], tup[2], tup[3], key_emb, sent_emb)
					queue_insert.put(put_data)
				mlog.debug("put data success {}_{}".format(id,aid))
				# 清空缓存
				tup_list = []
				sentence_list = []
				keyword_list = []
				emb_list = []

		keyword_list.extend(sentence_list)
		emb_list = self.bert.get_embedding(keyword_list)
		# 插入数据库
		for index in range(len(sentence_list)):
			tup = tup_list[index]
			key_emb = emb_list[index]
			sent_emb = emb_list[index+len(sentence_list)]
			put_data = (tup[0],tup[1], tup[2], tup[3], key_emb, sent_emb)
			queue_insert.put(put_data)	
		mlog.info("get sentence embed success.")

	def save_embed_from_mysql(self,queue_insert):
		mlog.info("start save_embed_from_mysql thread success.")
		while True:
			data = queue_insert.get()
			file_name = "embed/{}_{}.bin".format(data[1],data[0])
			with open(file_name,"wb") as f:
				pickle.dump(data, f)
				mlog.debug(file_name)

	#----------------------------------------------------- 
	def load_data_small_file(self, path="embed"):
		if os.path.exists(path) is not True:
			return
		for f_name in glob.glob(os.path.join(path, "*.bin")):
			with open(f_name, "rb") as f:
				data = pickle.load(f)
				yield data
    # 合并embed
	def merge_embed_from_file(self):
		embed_dict = {}
		iter_data = self.load_data_small_file()
		for data in iter_data:
			id = data[0]
			embed_dict[id] = data
			mlog.debug("total: {}".format(len(embed_dict)))
		mlog.info("begin to write to file")
		with open("embe_dict.bin","wb") as f:
			pickle.dump(embed_dict, f)
			mlog.info("svave embe_dict success, data size[{}].".format(len(embed_dict)))

	def consin(self,i,v):
		consin=np.dot(i,v)/(np.linalg.norm(i)*(np.linalg.norm(v)))
		return consin

	def search_best_ids(self, content=""):
		unsort_list = []
		vec = self.bert.get_embedding([content])[0]
		for k,v in self.data_embed.items():
			d = self.consin(vec,v[5])
			d_s = (k,d)
			unsort_list.append(d_s)

		sorted_list = sorted(unsort_list, key=lambda dd: dd[1],reverse=True)
		return sorted_list

	def search_keyword_from_embed(self,ids):
		for _id in ids:
			id = _id[0]	# 参考id
			score = _id[1] # 匹配分数
			keyword = self.data_embed[id][2] # 参考关键词
			sentence = self.data_embed[id][3] # 参考句子
			if self.data_embed[id][2] != None:
				return (id, keyword, score, sentence)

	def get_keyword_for_null(self,sql_queue):
		mlog.info("start thread get_keyword_for_null success.")

		self.data_embed = {}
		with open("embe_dict.bin", "rb") as f:
			self.data_embed = pickle.load(f)
	
		datas = self.load_data_null_keyword()
		for data in datas:
			content = data[3]
			if len(content) > 90:continue
			ids = self.search_best_ids(content)
			ref_id, ref_keyword, ref_score, ref_sentence = self.search_keyword_from_embed(ids)
			if ref_score == 1:continue

			# ref_kw = self.get_rate_keyword(ref_sentence)
			# src_kw = self.get_rate_keyword(content)

			str_key = "`keyword_general_embed` (`id`, `aid`, `ref_score`, `ref_keyword`, `ref_kw`, `src_kw`, `content`, `ref_content`, `ref_id`)"
			str_sql = "INSERT INTO {} VALUES {};"
			sql = str_sql.format(str_key, (data[0],data[1],ref_score,ref_keyword,"","",content,ref_sentence,ref_id) )
			sql_queue.put(sql)

	def insert_keyword_to_mysql(self,sql_queue):
		mlog.info("start insert_keyword_to_mysql success.")
		nSum = 0
		while True:
			sql = sql_queue.get()
			try:
				self.dbcon_textsum.execute(sql)
				if nSum >= 20:
					self.dbcon_textsum.commit()
					nSum = 0
				if sql_queue.qsize() <=0:
					self.dbcon_textsum.commit()
					time.sleep(5)
			except Exception as e:
				mlog.error(e)
			nSum += 1


	def load_data_null_keyword(self):
		sql = "select id,aid,keyword,content from titan_section where keyword is null;"
		datas = self.dbcon_titan.query(sql)
		mlog.info("total data size[{}]".format(len(datas)))
		return datas

	def get_rate_keyword(self,text):
		kws = self.textrank.get_keywords(text,3)
		result = ""
		for kw in kws:
			result += kw["word"] + " "
		return result.strip()

def save_embed_to_file():
	ge = GeneralEmbed()
	queue_sent = queue.Queue()
	queue_insert = queue.Queue()
	ge.load_data_from_titan_section(queue_sent)
	t1 = Thread(target=ge.get_sentence_embed, args=(queue_sent,queue_insert,))
	t2 = Thread(target=ge.save_embed_from_mysql, args=(queue_insert,))
	t2.daemon = True
	t1.daemon = True
	t1.start()
	t2.start()

	while True:
		if not t1.isAlive() and queue_insert.qsize() == 0:
			time.sleep(10)
			mlog.info("main thread end.")
			break
		time.sleep(30)
		print("main is alive, queue size[{}]".format(queue_insert.qsize()))

def merge_embed():
	ge = GeneralEmbed()
	ge.merge_embed_from_file()

# 获取数据库中没有关键词的句子，并标记关键词
def generai_keyword():
	ge = GeneralEmbed()
	sql_queue = queue.Queue()
	t1 = Thread(target=ge.get_keyword_for_null, args=(sql_queue,))
	t1.daemon = True
	t1.start()
	
	t2 = Thread(target=ge.insert_keyword_to_mysql, args=(sql_queue,))
	t2.daemon = True
	t2.start()
	
	while True:
		if not t1.isAlive() and sql_queue.qsize() == 0:
			time.sleep(10)
			break
		mlog.info("main thread is alive, sql_queue[{}]".format(sql_queue.qsize()))
		time.sleep(20)



if __name__ == '__main__':
	# save_embed_to_file()
	# merge_embed()

	generai_keyword()
