from pysyncobj import SyncObj, SyncObjConf, replicated
import tornado.httpserver
import msgpackrpc
import json
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
		try:
			self.__driver.executeWrite(queries)
		except:
			pass

	def executeRead(self, query):
		return self.__driver.executeRead(query)


class HTTPJsonRpcHandler(object):

	def __init__(self, syncer):
		self.__syncer = syncer

	def onNewRequest(self, request):
		reqID = None
		try:
			req = json.loads(request.body)
			method = req[u'method']
			params = req[u'params']
			reqID = req[u'id']
			if method.startswith('_'):
				raise Exception('wrong method')
			func = getattr(self.__syncer, method, None)
			if func is None:
				raise Exception('wrong method')
			res = {
				u'result': func(*params),
				u'error': None,
				u'id': reqID,
			}
			response = json.dumps(res)
		except Exception as e:
			res = {
				u'result': None,
				u'error': str(e),
				u'id': reqID,
			}
			response = json.dumps(res)

		request.write("HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n%s" % (len(response), response))
		request.finish()


if __name__ == '__main__':
	if len(sys.argv) != 2:
		print 'Usage: %s config.ini' % sys.argv[0]
		sys.exit(1)

	config = ConfigParser.RawConfigParser()
	config.read(sys.argv[1])
	syncer = DBSyncer(config)
	httpHandler = HTTPJsonRpcHandler(syncer)

	while not syncer._isReady():
		time.sleep(1.0)
		print 'waiting for ready'
	print 'ready'


	if config.has_option('DBSyncer', 'msgPackRpcAddr'):
		rpcAddress = config.get('DBSyncer', 'msgPackRpcAddr')
		rpcHost, rpcPort = rpcAddress.split(':')
		rpcPort = int(rpcPort)
		rpcServer = msgpackrpc.Server(syncer)
		rpcServer.listen(msgpackrpc.Address(rpcHost, rpcPort))

	loop = tornado.ioloop.IOLoop.current()

	if config.has_option('DBSyncer', 'httpRpcAddr'):
		rpcAddress = config.get('DBSyncer', 'httpRpcAddr')
		rpcHost, rpcPort = rpcAddress.split(':')
		rpcPort = int(rpcPort)

		http_server = tornado.httpserver.HTTPServer(httpHandler.onNewRequest)
		http_server.listen(rpcPort)

	loop.start()
