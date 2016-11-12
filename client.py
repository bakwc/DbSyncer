import msgpackrpc
import sys

try:
	import readline
except:
	pass

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
		splitCmd = normCmd.split()
		if not splitCmd:
			continue
		try:
			if splitCmd[0] in ('select',):
				print client.call('executeRead', cmd)
			else:
				client.call('executeWrite', cmd)
		except Exception as e:
			print e
