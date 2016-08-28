import msgpackrpc
import sys

if __name__ == '__main__':
	if len(sys.argv) != 2:
		print 'Usage: %s rpc_server_host:port'
		sys.exit(1)

	host, port = sys.argv[1].split(':')
	port = int(port)
	client = msgpackrpc.Client(msgpackrpc.Address(host, port))
	while True:
		cmd = raw_input('>> ')
		normCmd = cmd.lower()
		if normCmd.startswith('insert') or \
				normCmd.startswith('create'):
			client.call('executeWrite', cmd)
		elif normCmd.startswith('select'):
			print client.call('executeRead', cmd)
		elif cmd:
			print 'unsupported command'
