import socket
import sys
import threading 
import time


s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('sp21-cs425-g39-02.cs.illinois.edu' ,1234))
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('sp21-cs425-g39-03.cs.illinois.edu' ,1234))
time.sleep(10)