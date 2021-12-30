import socket
import sys
import threading 
import time

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
hostname = socket.gethostname()
ip = socket.gethostbyname(hostname)
s.bind(('127.0.0.1', 1233))
s.listen(8)
print('Waiting for connection...')
while True:
    # accept a new connection
	sock, addr = s.accept()
    # creat a new thread
	t = threading.Thread(target=tcplink, args=(sock, addr))
	t.start()