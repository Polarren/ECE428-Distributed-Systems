import socket
import sys
import threading 
import time

def tcplink(sock, addr):
	nodeName = sock.recv(1024).decode('utf-8')
	print('%s - %s connected' %(str(time.time()), nodeName ))
	sock.send(b'Node name received')
	Num_bytes = 0
	last_bytes = 0
	last_time = time.time()
	while True:
		data = sock.recv(1024)
		if not data :
			break
		message = data.decode('utf-8')
		print(message)
		sock.send(b'Message received')
        # bandwidth tracking
		Num_bytes+=len(message)
		t0 = time.time()-last_time
		if t0 >=1:
			amount_bytes = Num_bytes-last_bytes
			bandwidth = amount_bytes/t0
			bandwidth_file.write('%f %s\n'%(time.time()-t_start,bandwidth))
			last_bytes = Num_bytes
			last_time = last_time+t0

        # delay tracking
		event_time = message.split()[0]
		delay = time.time()-float(event_time)
		delay_file.write('%f %s\n'%(time.time()-t_start,delay))


	sock.close()
	print('%s - %s disconnected' %(str(time.time()), nodeName ))
    


if len(sys.argv)==1:
	print("Noting happens")

else:
	delay_file = open("delay.txt","w")
	bandwidth_file = open("bandwidth.txt","w")
	t_start = time.time()
	port = int(sys.argv[1])
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	hostname = socket.gethostname()

	ip = socket.gethostbyname(hostname)
	print(ip)
	s.bind((ip, port))
	s.listen(8)
	print('Waiting for connection...')
	while True:
	    # accept a new connection
		sock, addr = s.accept()

	    # creat a new thread
		t = threading.Thread(target=tcplink, args=(sock, addr))
		t.start()

    