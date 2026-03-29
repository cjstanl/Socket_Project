import socket
import sys
import random
import json
#utility imports
from utils import send_message, read_message, Peer

#MANAGER IP ADDRESS
#for local testing:
MANAGER_IP = '127.0.0.1'
#for lab testing (PC-A defualt IP setup)
#MANAGER_IP = '10.0.1.11'


#GLOBAL VARIABLES
peer_list = {} #Peer dictionary
m_ports = set() #track used m_ports
p_ports = set() #track used p_ports
DHT_EXISTS = False #boolean to track if DHT exists
DHT_SETUP_IN_PROGRESS = False #boolean to track if DHT is being setup
DHT_REBUILD_IN_PROGRESS = False #tracks if leave/join rebuild is in progress
DHT_TEARDOWN_IN_PROGRESS = False #tracks if teardown is in progress
DHT_REBUILD_PEER = None # tracks which peer initiated the leave/join command
peer_address = "" #address of peer sending current message
body = None #current message body to be parsed
manager_socket = None #socket to send/recieve messages for the manager to peer communication


#COMMAND FUNCTIONS
#REGISTER COMMAND:
#	- Executes manager operations for register in accordance with document specifications for 1.1.1
#	- Message Formats
#		- FAILURE: header = FAILURE, body = NULL
#		- SUCCESS: header = SUCCESS, body = NULL
def register_peer():
    #Tokenize json body for easy parsing
	tokens = body.split()
	
	#INPUT VALIDATION
	if len(tokens) != 4:
		send_message(manager_socket, peer_address, "FAILURE")
		return
	
	#EXTRACT PARAMETERS
	peer_name = tokens[0]
	ip_address = tokens[1]
	m_port = int(tokens[2])
	p_port = int(tokens[3])
	
	#VALIDATE PARAMETERS
	#Name parameter must be alphabetic, less than 15 characters, and unique
	if (not peer_name.isalpha()) or len(peer_name) > 15:
		send_message(manager_socket, peer_address, "FAILURE")
		return
	elif peer_name in peer_list:
		send_message(manager_socket, peer_address, "FAILURE")
		return
	#Port numbers need to be unique
	if (m_port in m_ports) or (p_port in p_ports):
		send_message(manager_socket, peer_address, "FAILURE")
		return

	#REGISTER PEER
	#create new peer object
	peer_list[peer_name] = Peer(peer_name, ip_address, m_port, p_port, "Free")
	#add parameters to list for future parameter validation
	m_ports.add(m_port)
	p_ports.add(p_port)
	send_message(manager_socket, peer_address, "SUCCESS")

#SETUP-DHT COMMAND
#	- Executes manager operations for setup DHT in accordance with document specifications for 1.1.2
#	- Message Formats
#		- FAILURE: header = FAILURE, body = NULL
#		- SUCCESS: header = SUCCESS, body = <dht_size> <peers_list>
def setup_dht():
	#Tokenize json body for easy parsing
	tokens = body.split()
	
	#INPUT VALIDATION
	if len(tokens) != 3:
		send_message(manager_socket, peer_address, "FAILURE")
		return
	
	#EXTRACT PARAMETERS
	peer_name = tokens[0]
	n = int(tokens[1])
	year = int(tokens[2])
	
	#VALIDATE PARAMETERS
	#peer must be registered
	if not peer_name in peer_list:
		send_message(manager_socket, peer_address, "FAILURE")
		return
	#n must be >= 3 and there needs to be at least n users registered
	if n < 3 or n > len(peer_list):
		send_message(manager_socket, peer_address, "FAILURE")
		return
	#DHT CHECK: there can be only one DHT at a time
	if DHT_EXISTS:
		send_message(manager_socket, peer_address, "FAILURE")
		return

	#SETUP-DHT PROTOCOL
	#update peer status to leader
	peer_list[peer_name].state = "Leader"
	peer_returns = [] #list of selected peers
	#add Leader as first peer in return messsage
	peer_returns.append({"name": peer_name, "ip": peer_list[peer_name].ip, "p_port": peer_list[peer_name].p_port})
	#select n-1 random n-1 free users to place in DHT
	count = 0 #tracks number of users selected
	while count < (n-1):
		random_name, random_peer = random.choice(list(peer_list.items()))
		#ensure leader is not chosen
		if random_name == peer_name:
			continue
		#ensure peer is free
		if random_peer.state != "Free":
			continue
		#Update peer state
		peer_list[random_name].state = "InDHT"
		#add peer to json list for return message
		peer_returns.append({"name": random_name, "ip": random_peer.ip, "p_port": random_peer.p_port})
		#increment count
		count = count + 1
	
	#Build nested json return statement
	body_return = {"dht_size": n, "peers": peer_returns}
	#Set DHT setup flag
	global DHT_SETUP_IN_PROGRESS
	DHT_SETUP_IN_PROGRESS = True
	#Send success message to leader
	send_message(manager_socket, peer_address, "SUCCESS", body_return)

#DHT-COMPLETE COMMAND
#	- Executes manager operations for DHT complete in accordance with document specifications for 1.1.3
#	- Message Formats
#		- FAILURE: header = FAILURE, body = NULL
#		- SUCCESS: header = SUCCESS, body = NULL
def dht_complete():
	#Tokenize json body for easy parsing
	tokens = body.split()
	
	#INPUT VALIDATION
	if len(tokens) != 1:
		send_message(manager_socket, peer_address, "FAILURE")
		return
	
	#EXTRACT PARAMETERS
	peer_name = tokens[0]
	
	#VALIDATE PARAMETERS
	#peer must be registered
	if not peer_name in peer_list:
		send_message(manager_socket, peer_address, "FAILURE")
		return
	#peer must be the leader
	if peer_list[peer_name].state != "Leader":
		send_message(manager_socket, peer_address, "FAILURE")
		return
	
	#UPDATE GLOBAL VARIABLES
	global DHT_SETUP_IN_PROGRESS
	global DHT_EXISTS
	DHT_SETUP_IN_PROGRESS = False
	DHT_EXISTS = True
	#Send success message to leader
	send_message(manager_socket, peer_address, "SUCCESS")
	
#QUERY-DHT COMMAND
#	- Executes manager operations for query DHT in accordance with document specifications for 1.1.4
#	- Message Formats
#		- FAILURE: header = FAILURE, body = NULL
#		- SUCCESS: header = SUCCESS, body = <query peer name> <query peer ip> <query peer p port>
def query_dht():
	#VALIDATE DHT CONDITION
	if not DHT_EXISTS:
		send_message(manager_socket, peer_address, "FAILURE")
		return
	#Check if peer is registered
	peer_name = body.strip()
	if peer_name not in peer_list:
		send_message(manager_socket, peer_address, "FAILURE")
		return
	#check if peer is free
	if peer_list[peer_name].state != "Free":
		send_message(manager_socket, peer_address, "FAILURE")
		return
	#choose random peer
	PEER_SELECTED = False
	while not PEER_SELECTED:
		random_name, random_peer = random.choice(list(peer_list.items()))
		if random_peer.state != "InDHT" and random_peer.state != "Leader":
			continue
		PEER_SELECTED = True

	#Build return message
	query_body = {"name": random_name, "ip": random_peer.ip, "p_port": random_peer.p_port}
	#send response message
	send_message(manager_socket, peer_address, "SUCCESS", query_body)
	
#LEAVE-DHT COMMAND
#	- Executes manager operations for leave DHT in accordance with document specifications for 1.1.5
#	- Message Formats
#		- FAILURE: header = FAILURE, body = NULL
#		- SUCCESS: header = SUCCESS, body = NULL
def leave_dht():
	#import global variables
	global DHT_EXISTS
	global DHT_REBUILD_IN_PROGRESS
	global DHT_REBUILD_PEER
	#EXTRACT PARAMETERS
	peer_name = body.strip()
	#VALIDATE DHT STATUS
	if not DHT_EXISTS:
		send_message(manager_socket, peer_address, "FAILURE")
		return
	#Check if peer is registered
	if peer_name not in peer_list:
		send_message(manager_socket, peer_address, "FAILURE")
		return
	#Check if peer is a DHT maintainer
	if peer_list[peer_name].state == "Free":
		send_message(manager_socket, peer_address, "FAILURE")
		return
	
	#UPDATE DHT STATUS
	DHT_EXISTS = False
	DHT_REBUILD_IN_PROGRESS = True
	#record which peer initiated leave command
	DHT_REBUILD_PEER = peer_name
	#send success message
	send_message(manager_socket, peer_address, "SUCCESS")

#JOIN-DHT COMMAND
#	- Executes manager operations for join DHT in accordance with document specifications for 1.1.6
#	- Message Formats
#		- FAILURE: header = FAILURE, body = NULL
#		- SUCCESS: header = SUCCESS, body = <leader name> <leader ip> <leader P port>
def join_dht():
	#import global variables
	global DHT_EXISTS
	global DHT_REBUILD_IN_PROGRESS
	global DHT_REBUILD_PEER
	#EXTRACT PARAMETER
	peer_name = body.strip()

	#VALIDATE DHT STATUS
	if not DHT_EXISTS:
		send_message(manager_socket, peer_address, "FAILURE")
		return
	#check if peer is registered
	if peer_name not in peer_list:
		send_message(manager_socket, peer_address, "FAILURE")
		return
	#check if peer is free
	if peer_list[peer_name].state != "Free":
		send_message(manager_socket, peer_address, "FAILURE")
		return
	
	#Get  leader peer
	leader = None
	for peer in peer_list.values():
		if peer.state == "Leader":
			leader = peer
			break

	#UPDATE DHT STATUS
	DHT_EXISTS = False
	DHT_REBUILD_IN_PROGRESS = True
	#record which peer initiated join command
	DHT_REBUILD_PEER = peer_name
	#send success message
	#build join message
	join_messsage_body = {"name": leader.name, "ip": leader.ip, "p_port": leader.p_port}
	send_message(manager_socket, peer_address, "SUCCESS", join_messsage_body)

#DHT-REBUILT COMMAND
#	- Executes manager operations for DHT rebuilt in accordance with document specifications for 1.1.7
#	- Message Formats
#		- FAILURE: header = FAILURE, body = NULL
#		- SUCCESS: header = SUCCESS, body = NULL
def dht_rebuilt():
	#import global variables
	global DHT_EXISTS
	global DHT_REBUILD_IN_PROGRESS
	global DHT_REBUILD_PEER
	#tokenize body for easier parsing
	tokens = body.split()

	#INPUT VALIDATION
	if len(tokens) != 2:
		send_message(manager_socket, peer_address, "FAILURE")
		return
	
	#EXTRACT PARAMETERS
	peer_name = tokens[0]
	new_leader = tokens[1]

	#VALIDATE PARAMETERS
	#check if peer initiated leave/join command
	if peer_name != DHT_REBUILD_PEER:
		send_message(manager_socket, peer_address, "FAILURE")
		return
	#check if peer is registered
	if peer_name not in peer_list:
		send_message(manager_socket, peer_address, "FAILURE")
		return
	#check if new leader is registered
	if new_leader not in peer_list:
		send_message(manager_socket, peer_address, "FAILURE")
		return
	
	#UPDATE PEER STATES
	#set peer state according to current status
	if peer_list[peer_name].state == "Free":
		#JOIN COMMAND ISSUED
		peer_list[peer_name].state = "InDHT"
	else:
		#LEAVE COMMAND ISSUED
		peer_list[peer_name].state = "Free"
	#Remove old leader if not the same
	for peer in peer_list.values():
		if peer.state == "Leader" and peer.name != new_leader:
			peer.state = "InDHT"
	#set new leaders state
	peer_list[new_leader].state = "Leader"
	
	#UPDATE DHT STATUS
	DHT_EXISTS = True
	DHT_REBUILD_IN_PROGRESS = False
	DHT_REBUILD_PEER = None
	#send response
	send_message(manager_socket, peer_address, "SUCCESS")

#DEREGISTER COMMAND
#	- Executes manager operations for the deregister command
#	- Message Formats
#		- FAILURE: header = FAILURE, body = NULL
#		- SUCCESS: header = SUCCESS, body = NULL
def deregister():
	#EXTRACT PARAMETER
	peer_name = body.strip()

	#VALIDATE PARAMETER
	#check if peer is registered
	if peer_name not in peer_list:
		send_message(manager_socket, peer_address, "FAILURE")
		return
	#check if peer is Free
	if peer_list[peer_name].state != "Free":
		send_message(manager_socket, peer_address, "FAILURE")
		return
	
	#REMOVE PEER FROM REGISTERED LISTS
	#remove ports from in use ports
	m_ports.discard(peer_list[peer_name].m_port)
	p_ports.discard(peer_list[peer_name].p_port)
	#delete peer from peer_list dictionary
	del peer_list[peer_name]
	#send response
	send_message(manager_socket, peer_address, "SUCCESS")

#TEARDOWN-DHT COMMAND
#	- Executes manager operations for teardown DHT in accordance with document specifications for 1.1.9
#	- Message Formats
#		- FAILURE: header = FAILURE, body = NULL
#		- SUCCESS: header = SUCCESS, body = NULL
def teardown_dht():
	#IMPORT GLOBAL VARIABLES
	global DHT_EXISTS
	global DHT_TEARDOWN_IN_PROGRESS

	#EXTRACT PARAMETER
	peer_name = body.strip()

	#VALIDATE PARAMETER and CONDITIONS
	#check if DHT exists
	if not DHT_EXISTS:
		send_message(manager_socket, peer_address, "FAILURE")
		return
	#check if peer is registered
	if peer_name not in peer_list:
		send_message(manager_socket, peer_address, "FAILURE")
		return
	#check if peer is the leader
	if peer_list[peer_name].state != "Leader":
		send_message(manager_socket, peer_address, "FAILURE")
		return
	
	#UPDATE DHT STATUS
	DHT_EXISTS = False
	DHT_TEARDOWN_IN_PROGRESS = True 
	#send response
	send_message(manager_socket, peer_address, "SUCCESS")

#TEARDOWN-COMPLETE COMMAND
#	- Executes manager operations for teardown complete in accordance with document specifications for 1.1.10
#	- Message Formats
#		- FAILURE: header = FAILURE, body = NULL
#		- SUCCESS: header = SUCCESS, body = NULL
def teardown_complete():
	#EXTRACT PARAMETER
	peer_name = body.strip()

	#VALIDATE PARAMETER
	#check if peer is registered
	if peer_name not in peer_list:
		send_message(manager_socket, peer_address, "FAILURE")
		return
	#check if peer is the leader
	if peer_list[peer_name].state != "Leader":
		send_message(manager_socket, peer_address, "FAILURE")
		return
	
	#RESET PEER STATES
	for peer in peer_list.values():
		if peer.state != "Free":
			peer.state = "Free"
	
	#UPDATE DHT STATUS
	global DHT_EXISTS
	global DHT_TEARDOWN_IN_PROGRESS
	DHT_EXISTS = False
	DHT_TEARDOWN_IN_PROGRESS = False
	#send response
	send_message(manager_socket, peer_address, "SUCCESS")

#MANGER
#   Main Manager Function that implements the always on manager server 
def Manager():
    #IMPORT GLOBAL VARIABLES
	global manager_socket
    #COMMAND LINE INPUT VALIDATION: Manager command line port
	if len(sys.argv) != 2:
		print("USAGE ERROR: python3 manager.py <listening port #>")
        #Exit with an error code
		sys.exit(1)
    
    #Collect Port number from command line
	MANAGER_PORT = int(sys.argv[1])

    #Create UDP Socket for manager
	manager_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	manager_socket.bind((MANAGER_IP, MANAGER_PORT))

    #INFINITE MANAGER LOOP
	while True:
		#IMPORT GLOBAL VARIABLES
		global peer_address
		global body

        #READ MESSAGES
		payload, peer_address = manager_socket.recvfrom(4096)
        #Read json message
		header, body = read_message(payload)
		#Extract command from message
		command = header

		#BLOCKING COMMAND CHECK
        #Check if DHT is being setup, rebuilt, or torn down (block all except complete command)
		if DHT_SETUP_IN_PROGRESS and command != "dht-complete":
			send_message(manager_socket, peer_address, "FAILURE")
			continue
		#check if DHT is being rebuilt (block all except rebuilt command)
		if DHT_REBUILD_IN_PROGRESS and command != "dht-rebuilt":
			send_message(manager_socket, peer_address, "FAILURE")
			continue
		#check if DHT is being torn down (block all excpet complete command)
		if DHT_TEARDOWN_IN_PROGRESS and command != "teardown-complete":
			send_message(manager_socket, peer_address, "FAILURE")
			continue

        #COMMAND DECISION TREE
		if command == "register":
			#REGISTER COMMAND
			register_peer()   

		elif command == "setup-dht":
            #SETUP-DHT COMMAND
			setup_dht()

		elif command == "dht-complete":
            #DHT-COMPLETE COMMAND
			dht_complete()

		elif command == "query-dht":
            #QUERY-DHT COMMAND
			query_dht()

		elif command == "leave-dht":
            #LEAVE-DHT COMMAND
			leave_dht()
            
		elif command == "join-dht":
            #JOIN-DHT COMMAND
			join_dht()
            
		elif command == "dht-rebuilt":
            #DHT-REBUILT
			dht_rebuilt()

		elif command == "deregister":
            #DEREGISTER COMMAND
			deregister()
            
		elif command == "teardown-dht":
            #TEARDOWN-DHT
			teardown_dht()
            
		elif command == "teardown-complete":
            #TEARDOWN-COMPLETE
			teardown_complete()
            
		else:
            #UNKNOWN COMMAND
			send_message(manager_socket, peer_address, "FAILURE")
			continue

#Call Manager function on startup
if __name__ == "__main__":
    Manager()
