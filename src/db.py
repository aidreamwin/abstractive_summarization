# -*- coding: utf-8 -*-

import time
import pymysql
import threading

from mlog import mlog

class MyDBConnect(object):
	def __init__(self):
		super(MyDBConnect, self).__init__()
		self._conn = None
		self._c = None
		self._isConnect = False

	def commit(self):
		try:
			tmp = self._conn.commit()
		except Exception as e:
			raise e
		mlog.debug("commit sql success[{}].".format(tmp))

	def execute(self,sql):
		try:
			tmp = self._c.execute(sql)
		except Exception as e:
			mlog.error("execute error[{}]".format(e))
			return None
		mlog.debug("execute sql[{}] success.".format(sql))
		return tmp

	def rollback(self):
		self._conn.rollback()

	def close(self):
		self._conn.close()

	def status(self):
		return self._isConnect

	def reconnect(self):
		for _ in range(3):
			self.connect()
			if self._isConnect:
				mlog.debug("reconnect success.")
				break
			mlog.error("reconnect failed,sleep...")
			time.sleep(5)

	def connect(self):
		pass


class MysqlDB(MyDBConnect):
	def __init__(self, conn):
		super(MysqlDB, self).__init__()
		self._conn = conn
		self._c = conn.cursor()
		self._isConnect = True

	def query(self,sql):
		try:
			self._c.execute(sql)
			result = self._c.fetchall()
		except Exception as e:
			mlog.error("query error[{}]".format(e))
			return None
		mlog.debug("query sql[{}] success.".format(sql))
		return result
	def query_iter(self,sql):
		try:
			self._c.execute(sql)
			while True:
				result = self._c.fetchone()
				yield result
		except Exception as e:
			mlog.error("query error[{}]".format(e))
		# return None
		

# 连接池
class MysqlConnPool(object):
	def __init__(self,host,user,passwd,db,port=3306,charset='utf8',poolSize=1):
		super(MysqlConnPool, self).__init__()
		self.conns = []
		self.poolSize = poolSize
		self.mutex = threading.Lock()
		self.currentSize = 0
		self.host = host
		self.user = user
		self.passwd = passwd
		self.db = db
		self.port = port
		self.charset = charset
		self.init()

	def __del__(self):
		for conn in self.conns:
			conn.close()

	def init(self):
		for x in range(self.poolSize):
			conn = pymysql.connect(host=self.host, port=self.port, user=self.user, \
			passwd=self.passwd, db=self.db, charset=self.charset)
			dbcon = MysqlDB(conn)
			self.conns.append(dbcon)
		mlog.debug("init db pool[%d] success." % len(self.conns))
	# 获取连接
	def Acquire(self):
		self.mutex.acquire()
		try:
			if len(self.conns) > 0:
				mlog.info("从池中获取连接,当前连接池数量:%d", len(self.conns))
				dbcon = self.conns[0]
				self.conns = self.conns[1:]
				return dbcon

			mlog.info("创建新连接,当前连接池数量0")
			self.currentSize+=1
			conn = pymysql.connect(host=self.host, port=self.port, user=self.user, \
			passwd=self.passwd, db=self.db, charset=self.charset)

			dbcon = MysqlDB(conn)
			return dbcon

		except Exception as e:
			raise e
		finally:
			self.mutex.release()

	# 释放连接
	def Release(self,dbcon):
		self.mutex.acquire()
		try:
			if self.currentSize >= self.poolSize:
				self.currentSize-=1
				dbcon.close()
				mlog.info("关闭连接,当前连接池数量:%d", len(self.conns))
			else:
				self.conns.append(dbcon)
				mlog.info("释放连接,当前连接池数量:%d", len(self.conns))
		except Exception as e:
			raise e	
		finally:
			self.mutex.release()