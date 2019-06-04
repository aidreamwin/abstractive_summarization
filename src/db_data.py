# -*- coding: utf-8 -*-
import db

# really
# class Config(object):
# 	host="10.0.0.114"
# 	user="titan_readonly"
# 	passwd="1lfTyBSaiPyqxtGG"
# 	db="titan"
# 	poolSize=3

# test
class Config(object):
	host="10.0.0.171"
	user="root"
	passwd="123456"
	db="textsum"
	poolSize=3
from mlog import mlog

class Data(object):
	"""docstring for Data"""
	def __init__(self,cg):
		self.db_pool = db.MysqlConnPool(
			host=cg.host,
			user=cg.user,
			passwd=cg.passwd,
			db=cg.db,
			poolSize=cg.poolSize)
		self.dbcon = self.db_pool.Acquire()
	def __del__(self):
		if self.dbcon != None:
			self.db_pool.Release(self.dbcon)

	def get_data_by_aid(self,aid):
		if self.dbcon == None:
			self.dbcon = self.db_pool.Acquire()
		aid = int(aid)
		sql = "select id,aid,keyword,content from titan_section where keyword is not null and aid={} order by id;".format(aid)
		data = self.dbcon.query(sql)
		return databcon.query(sql)
		return data

	# table keyword_session
	def insert_keyword_session(self,values):
		if self.dbcon == None:
			self.dbcon = self.db_pool.Acquire()
		str_key = "`keyword_general` (`id`, `aid`, `keyword`, `ai_keyword`, `sentence`)"
		str_sql = "INSERT INTO {} VALUES {};"

		for v in values:
			sql = str_sql.format(str_key,v)
			mlog.debug(sql)
			self.dbcon.execute(sql)
		self.dbcon.commit()



dbdata = Data(Config())
		