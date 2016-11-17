import os
import threading
import fdb
import cPickle
import zipfile
from driver import Driver

class FirebirdDriver(Driver):

	def __init__(self, firebirdFile, options=None):
		self.__firebirdFile = firebirdFile
		self.__options = options or {"user": "SYSDBA", "password": "masterkey", "charset": "WIN1251"}
		page_size = 16384
		if "page_size" in self.__options:
			page_size = int(self.__options.get("page_size", page_size))
			del self.__options["page_size"]

		if os.path.exists(self.__firebirdFile):
			self.__conn = fdb.connect(database=self.__firebirdFile, **self.__options)
		else:
			self.__conn = fdb.create_database(database=self.__firebirdFile, page_size=page_size, **self.__options)

		self.__lock = threading.RLock()

	def serialize(self, fileName, raftData):
		raftData = cPickle.dumps(raftData)
		with self.__lock:
			self.__conn.close()
			try:
				with zipfile.ZipFile(fileName, 'w', zipfile.ZIP_DEFLATED) as f:
					f.writestr('raft.bin', raftData)
					f.write(self.__firebirdFile, 'sql.bin')
			except:
				self.__conn = fdb.connect(database=self.__firebirdFile, **self.__options)
				raise
			self.__conn = fdb.connect(database=self.__firebirdFile, **self.__options)

	def deserialize(self, fileName):
		with self.__lock:
			self.__conn.close()
			with zipfile.ZipFile(fileName, 'r') as archiveFile:
				raftData = cPickle.loads(archiveFile.read('raft.bin'))
				with open(self.__firebirdFile, 'wb') as sqlDstFile:
					with archiveFile.open('sql.bin', 'r') as sqlSrcFile:
						while True:
							data = sqlSrcFile.read(2 ** 21)
							if not data:
								break
							sqlDstFile.write(data)
				self.__conn = fdb.connect(database=self.__firebirdFile, **self.__options)
				return raftData

	def executeWrite(self, queries):
		with self.__lock:
			c = self.__conn.cursor()
			if isinstance(queries, list):
				for query in queries:
					if isinstance(query, list):
						c.executemany(query[0], query[1])
					else:
						c.execute(query)
			else:
				c.execute(queries)
			self.__conn.commit()

	def executeRead(self, query):
		with self.__lock:
			c = self.__conn.cursor()
			c.execute(query)
			return c.fetchall()

	def getType(self):
		return 'firebird'
