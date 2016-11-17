import gdbm
import threading
import cPickle
import zipfile
from driver import Driver


class GdbmDriver(Driver):

	def __init__(self, gdbmFile):
		self.__gdbmFile = gdbmFile
		self.__db = gdbm.open(gdbmFile, 'c')
		self.__lock = threading.RLock()

	def serialize(self, fileName, raftData):
		raftData = cPickle.dumps(raftData)
		with self.__lock:
			self.__db.close()
			try:
				with zipfile.ZipFile(fileName, 'w', zipfile.ZIP_DEFLATED) as f:
					f.writestr('raft.bin', raftData)
					f.write(self.__gdbmFile, 'gdbm.bin')
			finally:
				self.__db = gdbm.open(self.__gdbmFile, 'c')


	def deserialize(self, fileName):
		with self.__lock:
			self.__db.close()
			with zipfile.ZipFile(fileName, 'r') as archiveFile:
				raftData = cPickle.loads(archiveFile.read('raft.bin'))
				with open(self.__gdbmFile, 'wb') as sqlDstFile:
					with archiveFile.open('gdbm.bin', 'r') as gdbmSrcFile:
						while True:
							data = gdbmSrcFile.read(2 ** 21)
							if not data:
								break
							sqlDstFile.write(data)
				self.__db = gdbm.open(self.__gdbmFile, 'c')
				return raftData

	def executeWrite(self, queries):
		if len(queries) == 0:
			return
		with self.__lock:
			if isinstance(queries, list) and isinstance(queries[0], list):
				for query in queries:
					key, value = query
					self.__db[key] = value
			else:
				key, value = queries
				self.__db[key] = value


	def executeRead(self, queries):
		with self.__lock:
			if isinstance(queries, list):
				results = []
				for query in queries:
					try:
						results.append(self.__db[query])
					except KeyError:
						results.append(None)
				return results

			try:
				return self.__db[queries]
			except KeyError:
				return None

	def getType(self):
		return 'gdbm'
