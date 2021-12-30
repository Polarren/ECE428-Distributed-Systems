import socket
import sys
import threading 
import time

def configure(config_filename):
	num_other_nodes = 0
	config_file = open(config_filename)
	lines = config_file.read().splitlines()
	identifiers_other = []
	hostnames_other=[]
	ports_other=[]
	for i in range(len(lines)):
		if i == 0:
			num_other_nodes = int(lines[0]) 
		else:
			identifier_other, hostname_other, port_other = lines[i].split()
			identifiers_other.append(identifier_other)
			hostnames_other.append(hostname_other)
			ports_other.append(int(port_other))
	return num_other_nodes,identifiers_other,hostnames_other,ports_other

def tryconnect(hostname,port):
	connected = False
	print(hostname,port)
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	# s.connect((hostname, port))
	while not connected:
		try:
			s.connect((hostname, port))
		except:
			connected = False
		else:
			connected = True


s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
identifier = sys.argv[1]
port = int(sys.argv[2])
config_filename = str(sys.argv[3])
num_other_nodes,identifiers_other,hostnames_other,ports_other = configure(config_filename)
#print(hostnames_other,ports_other)
for i in range(num_other_nodes):
	t = threading.Thread(target=tryconnect, args =(hostnames_other[i], ports_other[i]) )
	t.start()

#time.sleep(1)
#s.send(nodeName.encode())