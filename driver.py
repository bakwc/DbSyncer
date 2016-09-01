
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
	elif dbType == 'firebird':
		firebirdFile = config.get(dbType, 'dbFile')
		options = dict(config.items(dbType))
		if "dbfile" in options:
			del options["dbfile"]
		if "database" in options:
			if options["database"]:
				firebirdFile = options["database"]
			del options["database"]
		from firebird_driver import FirebirdDriver
		return FirebirdDriver(firebirdFile, options)
	raise LookupError('wrong database type')
