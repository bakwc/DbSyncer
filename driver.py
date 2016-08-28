
class Driver(object):

	def serialize(self, fileName, raftData):
		raise NotImplementedError()

	def deserialize(self, fileName):
		raise NotImplementedError()

	def executeWrite(self, queries):
		raise NotImplementedError()

	def executeRead(self, query):
		raise NotImplementedError()

def createDriver(config):
	dbType = config.get('DBSyncer', 'dbType')
	if dbType == 'sqlite':
		sqliteFile = config.get('sqlite', 'dbFile')
		from sqlite_driver import SqliteDriver
		return SqliteDriver(sqliteFile)
	raise LookupError('wrong database type')
