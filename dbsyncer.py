from pysyncobj import SyncObj, SyncObjConf, replicated
import msgpackrpc
import ConfigParser
import sys
import time
from driver import createDriver
from custom_actions import CustomActions


class DBSyncer(SyncObj, CustomActions):

	def __init__(self, config):
		conf = SyncObjConf(
			journalFile=config.get('PySyncObj', 'journalFile'),
			fullDumpFile=config.get('PySyncObj', 'fullDumpFile'),
			serializer=self.__serialize,
			deserializer=self.__deserialize,
		)
		if config.has_option('PySyncObj', 'logCompactionMinTime'):
			conf.logCompactionMinTime = config.getint('PySyncObj', 'logCompactionMinTime')
		if config.has_option('PySyncObj', 'logCompactionMinEntries'):
			conf.logCompactionMinEntries = config.getint('PySyncObj', 'logCompactionMinTime')

		selfNodeAddr = config.get('PySyncObj', 'selfNodeAddr')
		otherNodeAddrs = [x.strip() for x in config.get('PySyncObj', 'otherNodeAddrs').split(',')]
		self.__driver = createDriver(config)
		CustomActions.__init__(self, self.__driver, config)
		SyncObj.__init__(self, selfNodeAddr, otherNodeAddrs, conf)

	def __serialize(self, fileName, raftData):
		self.__driver.serialize(fileName, raftData)

	def __deserialize(self, fileName):
		return self.__driver.deserialize(fileName)

	@replicated
	def executeWrite(self, queries):
		self.__driver.executeWrite(queries)

	def executeRead(self, query):
		return self.__driver.executeRead(query)


if __name__ == '__main__':
	if len(sys.argv) != 2:
		print 'Usage: %s config.ini' % sys.argv[0]
		sys.exit(1)

	config = ConfigParser.RawConfigParser()
	config.read(sys.argv[1])
	syncer = DBSyncer(config)
	while not syncer._isReady():
		time.sleep(1.0)
		print 'waiting for ready'
	print 'ready'

	rpcAddress = config.get('DBSyncer', 'msgPackRpcAddr')
	rpcHost, rpcPort = rpcAddress.split(':')
	rpcPort = int(rpcPort)
	rpcServer = msgpackrpc.Server(syncer)
	rpcServer.listen(msgpackrpc.Address(rpcHost, rpcPort))
	rpcServer.start()
