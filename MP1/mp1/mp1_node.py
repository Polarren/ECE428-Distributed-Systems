import socket
import sys
import threading 
import time
from signal import signal, SIGPIPE, SIG_DFL
#import numpy as np
#import queue

#signal(SIGPIPE,SIG_DFL) 
mutex = threading.Lock()
RUNTIME=10 #Run for 30 seconds

############################Definitions########################### 
def server_connect(sock, addr):
	clientName = sock.recv(1024).decode('utf-8')
	print('%s - %s connected' %(str(time.time()), clientName ))
	
	global priority_queue
	global senders
	global num_connected
	global max_agreed
	global max_propsed
	global nodeName
	global replied_msg

	num_connected+=1

	while True:
		if time.time()-t0>RUNTIME:
			break
		#try:
		data = sock.recv(1024)
		if not data:
			break
		message = data.decode('utf-8')
		
		received_priority = int(message.split()[0])
		proposing_nodeName = message.split()[1]
		message_type = message.split()[2]
		action_nodeName = message.split()[3]
		action_time = message.split()[4]
		action = message.split(' ',5)[5]
		print('The received message is : '+str(received_priority)+' '+message_type+' '+action_nodeName +' '+action_time+' '+action )
		mutex.acquire()
		#print(message)
		if message_type =='INITIAL':
			proposed_priority = max(max_propsed,max_agreed)+1
			senders[action_nodeName].send(str(proposed_priority)+' '+nodeName+' '+'REPLY '+action_nodeName+' '+action_time+' '+ action)
			priority_queue.append([proposed_priority,nodeName,'REPLY',action_nodeName+' '+action_time+' '+ action])
			priority_queue.sort()
			print('The first at priority_queue at '+nodeName+' is:',priority_queue[0])
		elif message_type == 'REPLY':
			if action_nodeName+' '+action_time+' '+ action not in replied_msg:
				replied_msg[action_nodeName+' '+action_time+' '+ action] = 1
			else:
				replied_msg[action_nodeName+' '+action_time+' '+ action] += 1
			for i in range(len(priority_queue)):
				if action_nodeName+' '+action_time+' '+ action == priority_queue[i][3]:
					position = i
					if received_priority > priority_queue[i][0]:
						priority_queue[i][0] = received_priority
						priority_queue[i][1] = proposing_nodeName
						priority_queue[i][2] = 'REPLY'
					if received_priority == priority_queue[i][0]:
						if proposing_nodeName > priority_queue[i][1]:
							priority_queue[i][1] = proposing_nodeName
							priority_queue[i][2] = 'REPLY'
					break
			if replied_msg[action_nodeName+' '+action_time+' '+ action] == num_connected:
				priority_queue[position][2] = 'FINAL'
				multicast(str(priority_queue[position][0])+' '+priority_queue[position][1]+' '+'FINAL '+action_nodeName+' '+action_time+' '+ action)
				max_agreed = max(max_agreed,priority_queue[position][0])

			priority_queue.sort()
			print('The first at priority_queue at '+nodeName+' is:',priority_queue[0])
			while True:
				if time.time()-t0>RUNTIME:
					break
				if len(priority_queue)>0:
					if priority_queue[0][2] == 'FINAL':
						process_action(priority_queue[0][3])
						del(priority_queue[0])
					else:
						break
				else:
					break
		elif message_type == 'FINAL':
			for i in range(len(priority_queue)):
				if action_nodeName+' '+action_time+' '+ action == priority_queue[i][3]:
					position = i
					priority_queue[i][0] = received_priority
					priority_queue[i][1] = proposing_nodeName
					priority_queue[i][2] = 'FINAL'
					break
			priority_queue.sort()
			print('The first at priority_queue at '+nodeName+' is:',priority_queue[0])
			max_agreed = max(max_agreed,received_priority)
			while True:
				if time.time()-t0>RUNTIME:
					break
				if len(priority_queue)>0:
					#print('The first at priority_queue at '+nodeName+' is:',priority_queue[0])
					if priority_queue[0][2] == 'FINAL':
						# print('processing action:',priority_queue[0][3])
						#if islegal(priority_queue[0][3]):
						process_action(priority_queue[0][3])
						del(priority_queue[0])
					else:
						break
				else:
					break
		mutex.release()
		# time.sleep(0.5)
		# except:
		# 	break
	sock.close()
	print('%s - %s disconnected' %(str(time.time()), clientName ))

def client_connect(nodename,hostname,port):
	t0 = time.time()
	connected = False
	#print(hostname,port)
	global senders
	global nodeName
	global num_connected_to
	senders[nodename] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	# s.connect((hostname, port))
	while not connected:
		if time.time()-t0>RUNTIME:
			break
		try:
			senders[nodename].connect((hostname, port))
		except:
			connected = False
		else:
			connected = True
	senders[nodename].send(nodeName.encode())
	num_connected_to+=1

	while True:

		if time.time()-t0>RUNTIME:
			break
	senders[nodename].close()
    
def configure(config_filename):
	num_other_nodes = 0
	config_file = open(config_filename)
	lines = config_file.read().splitlines()
	nodeNames_other = []
	hostnames_other=[]
	ports_other=[]
	for i in range(len(lines)):
		if i == 0:
			num_other_nodes = int(lines[0]) 
		else:
			nodeName_other, hostname_other, port_other = lines[i].split()
			nodeNames_other.append(nodeName_other)
			hostnames_other.append(hostname_other)
			ports_other.append(int(port_other))
	return num_other_nodes,nodeNames_other,hostnames_other,ports_other

def islegal(action):
	global balance
	print(balance)
	action_type = action.split()[0]
	if action_type=='DEPOSIT':
		return True
	if action_type == 'TRANSFER':
		payer = action.split()[1]
		payee = action.split()[3]
		amount = action.split()[4]
		if payer not in balance:
			print('Action '+action+' is not legal!')
			return False
		else:
			if balance[payer]<amount:
				print('Action '+action+' is not legal!')
				return False
			else:
				print('Action '+action+' is not legal!')
				return True

def is_action(action):
	action_type = action.split()[0]
	if action_type=='DEPOSIT':
		return True
	if action_type == 'TRANSFER':
		return True
	else: 
		return False

def process_action(action):
	global balance
	#print(balance)
	mutex = threading.Lock()
	print('processing action:',action)
	action_type = action.split()[0]
	if action_type=='DEPOSIT':
		payee = action.split()[1]
		amount = action.split()[2]
		mutex.acqure()
		if payee not in balance:
			balance[payee] = amount
		else:
			balance[payee]+=amount
		mutex.release()
		print_balance(balance)
		return True
	if action_type == 'TRANSFER':
		payer = action.split()[1]
		payee = action.split()[3]
		amount = action.split()[4]
		mutex.acquire()
		if payer not in balance:
			mutex.release()
			return False
		else:
			if balance[payer]<amount:
				mutex.release()
				return False
			else:
				balance[payee]+=amount
				balance[payer]-=amount
				print_balance(balance)
				mutex.release()
				return True
		

def print_balance(balance):
	print("Printing balance")
	report = 'BALANCES'
	for account in balance:
		report+=' '+account+':'+str(balance[account])
	print(report)

def multicast(message):
	global senders
	for nodename in senders:
		try:
			senders[nodename].send(message.encode())
		except:
			del senders[nodename]
# 	return None



##########################Program Starts########################
if len(sys.argv)<=3:
	print("Noting happens")

else:
	t0 = time.time()
	balance = {}
	replied_msg = {}
	priority_queue = []
	max_agreed = 0
	max_propsed = 0
############################Connection###########################
	nodeName = sys.argv[1]
	port = int(sys.argv[2])
	config_filename = str(sys.argv[3])
	num_other_nodes,nodeNames_other,hostnames_other,ports_other = configure(config_filename)
	num_connected = 0
	num_connected_to = 0
	senders = {}
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	hostname = socket.gethostname()

	ip = socket.gethostbyname(hostname)
	print(ip)
	s.bind((ip, port))
	s.listen(8)
	print('Waiting for connection...')
	for i in range(num_other_nodes):
		t = threading.Thread(target=client_connect, args =(nodeNames_other[i],hostnames_other[i], ports_other[i]) )
		t.start()

		#s.connect(hostnames_other[i], ports_other[i])
	for i in range(num_other_nodes):
	    # accept a new connection
		sock, addr = s.accept()

	    # creat a new thread
		t = threading.Thread(target=server_connect, args=(sock, addr))
		t.start()


#############################Process##############################
	while True:
		if num_connected>=num_other_nodes and num_connected_to>=num_other_nodes:
			time.sleep(0.5)
			break

	while True:	
		if not sys.stdin.isatty():
			action = sys.stdin.readline().strip('\n')
			#print(action+'\n')
			if is_action(action):
				proposed_priority = max(max_propsed,max_agreed)+1
				max_propsed = proposed_priority
				priority_queue.append([proposed_priority,str(nodeName),'INITIAL',nodeName+' '+str(time.time())+' '+ action])
				priority_queue.sort()
				# print(priority_queue)
				multicast(str(proposed_priority)+' '+nodeName+' '+'INITIAL '+nodeName+' '+str(time.time())+' '+ action)
		if time.time()-t0>RUNTIME:
			break
	s.close()




