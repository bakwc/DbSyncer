from pysyncobj import SyncObj, SyncObjConf, replicated, FAIL_REASON
import tornado.httpserver
import msgpackrpc
import json
import ConfigParser
import sys
import time
from functools import partial
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
			return (True, None)
		except Exception as e:
			return (False, str(e))

	def executeRead(self, query):
		return self.__driver.executeRead(query)

	def getDriverType(self):
		return self.__driver.getType()


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
			funcReplicated = func.func_dict.get('replicated', False)
			if funcReplicated:
				func(*params, callback=partial(self.onResponseCompleted, request, reqID))
				return
			result = func(*params)
			self.sendResponse(request, reqID, result, None)
		except Exception as e:
			self.sendResponse(request, reqID, None, str(e))

	def onResponseCompleted(self, request, reqID, method, result, failReason):
		if failReason == FAIL_REASON.SUCCESS:
			self.sendResponse(request, reqID, result, None)
			return
		self.sendResponse(request, reqID, result, failReason)

		if failReason != FAIL_REASON.SUCCESS:
			self.sendResponse(request, reqID, result, str(failReason))
			return

		if method == 'executeWrite':
			if result[0]:
				self.sendResponse(request, reqID, None, str(result[1]))
				return
			else:
				self.sendResponse(request, reqID, None, None)
				return

		self.sendResponse(request, reqID, result, None)



	def sendResponse(self, request, reqID, result, error):
		res = {
			u'result': result,
			u'error': error,
			u'id': reqID,
		}
		response = json.dumps(res)
		request.write("HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n%s" % (len(response), response))
		request.finish()


class MsgPackRpcHanlder(object):

	def __init__(self, syncer):
		self.__syncer = syncer

	def dispatch(self, method, params, responder):
		try:
			method = str(method)
			if method.startswith('_'):
				raise Exception('wrong method')
			func = getattr(self.__syncer, method, None)
			if func is None:
				raise Exception('wrong method')
			funcReplicated = func.func_dict.get('replicated', False)
			if funcReplicated:
				func(*params, callback=partial(self.onResponseCompleted, responder, method))
				return
			result = func(*params)
			responder.set_result(result)
		except Exception as e:
			responder.set_error(str(e))

	def onResponseCompleted(self, responder, method, result, failReason):
		if failReason != FAIL_REASON.SUCCESS:
			responder.set_error(str(failReason))
			return

		if method == 'executeWrite':
			if result[0]:
				responder.set_result(None)
				return
			else:
				responder.set_error(str(result[1]))
				return

		responder.set_result(result)


if __name__ == '__main__':
	if len(sys.argv) != 2:
		print 'Usage: %s config.ini' % sys.argv[0]
		sys.exit(1)

	config = ConfigParser.RawConfigParser()
	config.read(sys.argv[1])
	syncer = DBSyncer(config)
	httpHandler = HTTPJsonRpcHandler(syncer)
	msgPackHandler = MsgPackRpcHanlder(syncer)

	while not syncer._isReady():
		time.sleep(1.0)
		print 'waiting for ready'
	print 'ready'


	if config.has_option('DBSyncer', 'msgPackRpcAddr'):
		rpcAddress = config.get('DBSyncer', 'msgPackRpcAddr')
		rpcHost, rpcPort = rpcAddress.split(':')
		rpcPort = int(rpcPort)
		rpcServer = msgpackrpc.Server(None)
		rpcServer.dispatch = msgPackHandler.dispatch
		rpcServer.listen(msgpackrpc.Address(rpcHost, rpcPort))

	loop = tornado.ioloop.IOLoop.current()

	if config.has_option('DBSyncer', 'httpRpcAddr'):
		rpcAddress = config.get('DBSyncer', 'httpRpcAddr')
		rpcHost, rpcPort = rpcAddress.split(':')
		rpcPort = int(rpcPort)

		http_server = tornado.httpserver.HTTPServer(httpHandler.onNewRequest)
		http_server.listen(rpcPort)

	loop.start()
