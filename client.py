import msgpackrpc
import sys

try:
	import readline
except:
	pass

def processSqlCommand(cmd, client):
	normCmd = cmd.lower()
	splitCmd = normCmd.split()
	if not splitCmd:
		return
	if splitCmd[0] in ('select',):
		print client.call('executeRead', cmd)
	else:
		client.call('executeWrite', cmd)

def processKeyValueCommand(cmd, client):
	normCmd = cmd.lower()
	splitCmd = normCmd.split()
	if not splitCmd:
		return
	if splitCmd[0] == 'set':
		if len(splitCmd) != 3:
			raise Exception('Wrong arguments number')
		client.call('executeWrite', [splitCmd[1], splitCmd[2]])
		return
	if splitCmd[0] == 'get':
		if len(splitCmd) != 2:
			raise Exception('Wrong arguments number')
		print client.call('executeRead', splitCmd[1])
		return
	raise Exception('Wrong command')

if __name__ == '__main__':
	if len(sys.argv) != 2:
		print 'Usage: %s rpc_server_host:port'
		sys.exit(1)

	host, port = sys.argv[1].split(':')
	port = int(port)
	client = msgpackrpc.Client(msgpackrpc.Address(host, port))
	driverType = client.call('getDriverType')
	print 'driver type:', driverType

	handler = None
	if driverType in ('sqlite', 'firebird',):
		handler = processSqlCommand
	if driverType in ('gdbm',):
		handler = processKeyValueCommand

	assert handler is not None

	while True:
		cmd = raw_input('>> ')
		try:
			handler(cmd, client)
		except Exception as e:
			print e
