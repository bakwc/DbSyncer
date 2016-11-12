import threading
import sqlite3
import cPickle
import zipfile
from driver import Driver

class SqliteDriver(Driver):

	def __init__(self, sqliteFile):
		self.__sqliteFile = sqliteFile
		self.__conn = sqlite3.connect(self.__sqliteFile, check_same_thread=False)
		self.__lock = threading.RLock()

	def serialize(self, fileName, raftData):
		raftData = cPickle.dumps(raftData)
		with self.__lock:
			self.__conn.close()
			try:
				with zipfile.ZipFile(fileName, 'w', zipfile.ZIP_DEFLATED) as f:
					f.writestr('raft.bin', raftData)
					f.write(self.__sqliteFile, 'sql.bin')
			except:
				self.__conn = sqlite3.connect(self.__sqliteFile, check_same_thread=False)
				raise
			self.__conn = sqlite3.connect(self.__sqliteFile, check_same_thread=False)

	def deserialize(self, fileName):
		with self.__lock:
			self.__conn.close()
			with zipfile.ZipFile(fileName, 'r') as archiveFile:
				raftData = cPickle.loads(archiveFile.read('raft.bin'))
				with open(self.__sqliteFile, 'wb') as sqlDstFile:
					with archiveFile.open('sql.bin', 'r') as sqlSrcFile:
						while True:
							data = sqlSrcFile.read(2 ** 21)
							if not data:
								break
							sqlDstFile.write(data)
				self.__conn = sqlite3.connect(self.__sqliteFile, check_same_thread=False)
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
		cmd = str(query).lower().split()
		if not cmd or cmd[0] not in ('select',):
			raise Exception('readonly command not allowed')
		with self.__lock:
			c = self.__conn.cursor()
			c.execute(query)
			return c.fetchall()
