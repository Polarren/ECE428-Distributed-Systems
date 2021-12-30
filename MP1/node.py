
import socket
import sys
import time

RUNTIME=100 #Run for 100 seconds
t0 = time.time()
if len(sys.argv) != 4:
	print("Inlvalid input. Please try again.")
	print("input length: %d",len(sys.argv))

else:
	nodeName=str(sys.argv[1])
	loggerAddress=str(sys.argv[2])
	port = int(sys.argv[3])
	t0 = time.time()


	# creat a socket:
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	# establish the connection
	s.connect((loggerAddress, port))
	s.send(nodeName.encode())
	data = s.recv(1024).decode('utf-8')
	# time.sleep(1)
	while True:
		stdin = input()
		print(stdin)
		event_time = stdin.split()[0]
		event = stdin.split()[1]
		message = "%s %s %s" %(event_time, nodeName, event)
		
		s.send(message.encode())
		data = s.recv(1024).decode('utf-8')
		if time.time()-t0>RUNTIME:
			break
	s.close()
