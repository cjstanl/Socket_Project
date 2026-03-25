import socket
import sys
import threading
import csv
#utility imports
from utils import send_message, read_message, count_storm_events_csv, compute_hashes, find_next_prime, is_prime

#CUSTOM DATA STRUCTURES
#   Record stores the 14 fields required as an object for easy parsing and storage as well as the pos and id
class Record:
    def __init__(self, pos, owner_id, event_id, state, year, month_name, event_type, cz_type, cz_name, injuries_direct, injuries_indirect, deaths_direct, deaths_indirect, damage_property, damage_crops, tor_f_scale):
        self.pos = pos
        self.id = owner_id
        self.event_id = event_id
        self.state = state
        self.year = year
        self.month_name = month_name
        self.event_type = event_type
        self.cz_type = cz_type
        self.cz_name = cz_name
        self.injuries_direct = injuries_direct
        self.injuries_indirect = injuries_indirect
        self.deaths_direct = deaths_direct
        self.deaths_indirect = deaths_indirect
        self.damage_property = damage_property
        self.damage_crops = damage_crops
        self.tor_f_scale = tor_f_scale

#Thread to listen for peer to peer messages
#   - Function takes the peer socket and listens infintely for peer messages
def peer2peer_Listener(peer2peer_socket):
    #Peer to Peer Variables
    identifier = 0
    ring_size = 0
    neighbor_ip = ""
    neighbor_pPort = 0

    peerRecordList = {} #dictionary to store records keyed by eventID

    #Infinite loop listening for messages
    while True:
        #READ MESSAGE
        payload, peer_address = peer2peer_socket.recvfrom(4096) #get message from socket
        #read json message from peer
        header, body = read_message(payload)
        #Check for empty message
        if not header:
            continue

        #EXTRACT COMMAND
        peer_command = header

        #PEER-PEER COMMAND DECISION TREE
        if peer_command == "set-id":
            #SET-ID COMMAND

            #EXTRACT DHT DATA
            identifier = body["peer_id"]
            ring_size = body["ring_size"]
            peer_list = body["peer_list"]
            
            #Determine right neighbor id and index in tuples
            neighbor_id = (identifier + 1) % ring_size
            neighbor_ip = peer_list[neighbor_id]["ip"]
            neighbor_pPort = peer_list[neighbor_id]["p_port"]

        elif peer_command == "store":
            #STORE COMMAND

            #Wait until peer is setup to prevent race condition
            while not neighbor_ip:
                continue

            #EXTRACT DATA from body
            curr_id = body["destination_id"]
            curr_pos = body["pos"]
            record = body["record"]

            #Check ID does not match to propogate around the ring
            if curr_id != identifier:
                send_message(peer2peer_socket, (neighbor_ip, neighbor_pPort), "store", body)
                continue

            #If ID matches store in recordList
            peerRecordList[record["EVENT_ID"]] = Record(curr_pos, curr_id, record["EVENT_ID"], record["STATE"], record["YEAR"], record["MONTH"], record["EVENT_TYPE"], record["CZ_TYPE"], record["CZ_NAME"], record["INJURIES_DIRECT"], record["INJURIES_INDIRECT"], record["DEATHS_DIRECT"], record["DEATHS_INDIRECT"], record["DAMAGE_PROPERTY"], record["DAMAGE_CROPS"], record["TOR_F_SCALE"])


#PEER CLI COMMANDS
#SETUP-PEER
#
def setup_peer(tokens, peer2Peer_socket, peer2Manager_socket):
	#INPUT VALIDATION
	if len(tokens) != 5:
		print("USAGE ERROR: peer-setup <peer name> <peer ip> <manager port> <peer port>")
		return
	
	#EXTRACT PARAMETERS
	peer_name = tokens[1]
	peer_ip = tokens[2]
	m_port = int(tokens[3])
	p_port = int(tokens[4])
	
	#set up UDP sockets
	peer2Peer_socket.bind((peer_ip, p_port))
	peer2Manager_socket.bind((peer_ip, m_port))

	#start peer listening thread for peer to peer commands
	peer_thread = threading.Thread(target=peer2peer_Listener, args=(peer2Peer_socket,))
	peer_thread.start()

	#Output status
	print("Peer is setup")
	return peer_name, peer_ip, m_port, p_port

#REGISTER PEER
#
def register_peer(peer_name, peer_ip, m_port, p_port, peer2Manager_socket, MANAGER_IP, MANAGER_PORT):
	#Build message
	register_message = peer_name + " " + peer_ip + " " + str(m_port) + " " + str(p_port)
	#Send message to manager
	send_message(peer2Manager_socket, (MANAGER_IP, MANAGER_PORT), "register", register_message)
	#Get response
	managerResponse_register, manager_address = peer2Manager_socket.recvfrom(4096)
	#read response (body never used)
	header, body = read_message(managerResponse_register)
	#Check for failure
	if header == "FAILURE":
		print("REGISTER FAILED")
	else:
		print("REGISTERED")

#SETUP_DHT
#
def setup_dht(tokens, peer_name, peer2Peer_socket, peer2Manager_socket, MANAGER_IP, MANAGER_PORT):
    #INPUT VALIDATION
    #Input: command n YYYY
    if len(tokens) != 3:
        print("USAGE ERROR: setup-dht n YYYY")
        return
    #EXTRACT PARAMETERS
    n = int(tokens[1])
    year = int(tokens[2])
    #PARAMETER VALIDATION
    #n must be greater than or equal to 3 and year must be a valid 4 digit integer
    if n < 3 or year < 1000 or year > 9999:
        print("USAGE ERROR (Parameters): setup-dht n YYYY (n must be greater than or equal to 3)")
        return
    #build message
    setupMessage = peer_name + " " + str(n) + " " + str(year)
    #send message to manager
    send_message(peer2Manager_socket, (MANAGER_IP, MANAGER_PORT), "setup-dht", setupMessage)
    #get response
    managerResponse, manager_address = peer2Manager_socket.recvfrom(4096)
    #read response
    header, body = read_message(managerResponse)
    #check for failure
    if header == "FAILURE":
        print("SETUP-DHT FAILED")
        return
    #extract peer list and dht size from response
    peers = body["peers"]
    DHT_RING_SIZE = body["dht_size"]
    #Set leader information
    leader_identifier = 0 #leader is always node 0
    nodeStorageAmounts = {} #dictionary to store how many records are at each node
    nodeStorageAmounts[0] = 0 #initialize node count for leader to 0
    leader_neighbor_name = peers[1]["name"]
    leader_neighbor_IP = peers[1]["ip"]
    leader_neighbor_port = peers[1]["p_port"]
    #loop over remainder of peers starting at index 1 to exclude leader
    for i in range(1, len(peers)):
        peer = peers[i] #get current peer
        nodeStorageAmounts[i] = 0 #initialize node count to 0
        #build message for peer to peer id and neighbor assignments
        peer_assignment_body = {"peer_id": i, "ring_size": DHT_RING_SIZE, "peer_list": peers}
        #send peer to peer message
        send_message(peer2Peer_socket, (peer["ip"], peer["p_port"]), "set-id", peer_assignment_body)
    
    #PARSING CSV FILE
    #Build filename
    selected_file = "Data/details_" + str(year) + ".csv"
    #get number of storm events
    num_of_events = count_storm_events_csv(selected_file)
    #compute hash table size
    hash_size = find_next_prime(num_of_events)
    leader_recordList = {}

    #Loop over all storm events
    with open(selected_file, newline='') as stormcsv:
        reader = csv.DictReader(stormcsv)
        for row in reader:
            #extract paramters
            curr_event_id = row['EVENT_ID']
            curr_state = row['STATE']
            curr_year = row['YEAR']
            curr_month_name = row['MONTH_NAME']
            curr_event_type = row['EVENT_TYPE']
            curr_cz_type = row['CZ_TYPE']
            curr_cz_name = row['CZ_NAME']
            curr_injuries_direct = row['INJURIES_DIRECT']
            curr_injuries_indirect = row['INJURIES_INDIRECT']
            curr_deaths_direct = row['DEATHS_DIRECT']
            curr_deaths_indirect = row['DEATHS_INDIRECT']
            curr_damage_property = row['DAMAGE_PROPERTY']
            curr_damage_crops = row['DAMAGE_CROPS']
            curr_tor_f_scale = row['TOR_F_SCALE']

            #compute pos and id for event
            curr_pos, curr_id = compute_hashes(hash_size, DHT_RING_SIZE, int(curr_event_id))

            #If event needs to be stored at leader do this first
            if curr_id == 0:
                #add record to leaders list
                leader_recordList[curr_event_id] = Record(curr_pos, curr_id, curr_event_id, curr_state, curr_year, curr_month_name, curr_event_type, curr_cz_type, curr_cz_name, curr_injuries_direct, curr_injuries_indirect, curr_deaths_direct, curr_deaths_indirect, curr_damage_property, curr_damage_crops, curr_tor_f_scale)
                nodeStorageAmounts[0] = nodeStorageAmounts[0] + 1
                continue #skip to next entry
            
            #build message for store command
            store_command_body = {"pos": curr_pos, "destination_id": curr_id, "record": {"EVENT_ID": curr_event_id,"STATE": curr_state,"YEAR": curr_year,"MONTH": curr_month_name,"EVENT_TYPE": curr_event_type,"CZ_TYPE": curr_cz_type,"CZ_NAME": curr_cz_name,"INJURIES_DIRECT": curr_injuries_direct,"INJURIES_INDIRECT": curr_injuries_indirect,"DEATHS_DIRECT": curr_deaths_direct,"DEATHS_INDIRECT": curr_deaths_indirect,"DAMAGE_PROPERTY": curr_damage_property,"DAMAGE_CROPS": curr_damage_crops,"TOR_F_SCALE": curr_tor_f_scale}}
            #send store command around DHT Ring
            send_message(peer2Peer_socket, (leader_neighbor_IP, leader_neighbor_port), "store", store_command_body)
            #increment node storage amount for identifier
            nodeStorageAmounts[curr_id] = nodeStorageAmounts[curr_id] + 1

    #PRINT DHT STATUS
    print("Records Distributed:")
    for key, value in nodeStorageAmounts.items():
        print(f"Node: {key}, Records Stored: {value}")
    
    #Send DHT-Complete Message to Manager
    send_message(peer2Manager_socket, (MANAGER_IP, MANAGER_PORT), "dht-complete", peer_name)
    #get response
    dht_complete_response, manager_address = peer2Manager_socket.recvfrom(4096)
    #read response
    dht_complete_header, dht_complete_body = read_message(dht_complete_response)
    #check for failure
    if dht_complete_header == "FAILURE":
        print("DHT-COMPLETE FAILED")
        return
    print("DHT SETUP COMPLETE")
    #Return leader variables to main loop
    return leader_neighbor_IP, leader_neighbor_port, DHT_RING_SIZE, nodeStorageAmounts, leader_recordList

#PEER
#   Main Peer Function with stdin command interface
def Peer():
    
    #Variables for PEER
    PEER_SETUP = False #boolean to track if the peer has been setup
    #general peer variables
    peer_name = "" 
    peer_ip = ""
    m_port = 0
    p_port = 0
    
    #LEADER ONLY VARIABLES
    leader_identifier = 0
    leader_neighbor_name = ""
    leader_neighbor_IP =""
    leader_neighbor_port = 0
    DHT_RING_SIZE = 0
    leader_recordList = {} #dictionary to store leaders hash table keyed by eventID
    nodeStorageAmounts = {} # dictionary to store how many records are at each node

    #Command line Input Validation
    if len(sys.argv) != 3:
        print("USAGE ERROR: peer.py <MANAGER_IP> <MANAGER_PORT>")
        sys.exit(1)

    #EXTRACT COMMAND LINE ARGUMENTS
    MANAGER_IP = sys.argv[1]
    MANAGER_PORT = int(sys.argv[2])

    #UDP sockets for peer-peer messages and peer-manager messages
    peer2Peer_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    peer2Manager_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    #STDIN INTERFACE
    for line in sys.stdin:
        #Tokenize the command with strip and split
        tokens = line.strip().split()
        if not tokens:
            continue #Skips empty lines with no commands

        #Extract Command
        command = tokens[0]

        #null variable protection
        if (not PEER_SETUP) and command != "peer-setup":
            print("ERROR: Peer not setup yet")
            continue
        
        #COMMAND DECISION TREE
        if command == "register":
            #REGISTER COMMAND
            register_peer(peer_name, peer_ip, m_port, p_port, peer2Manager_socket, MANAGER_IP, MANAGER_PORT)

        elif command == "setup-dht":
            #SETUP-DHT COMMAND
            setup_dht_result = setup_dht(tokens, peer_name, peer2Peer_socket, peer2Manager_socket, MANAGER_IP, MANAGER_PORT)
            #check if setup was successful and extract values
            if setup_dht_result:
                leader_neighbor_IP, leader_neighbor_port, DHT_RING_SIZE, nodeStorageAmounts, leader_recordList = setup_dht_result

        elif command == "query-dht":
            #QUERY-DHT COMMAND
            print("COMMAND NOT SUPPORTED")
        elif command == "leave-dht":
            #LEAVE-DHT COMMAND
            print("COMMAND NOT SUPPORTED")
        elif command == "join-dht":
            #JOIN-DHT COMMAND
            print("COMMAND NOT SUPPORTED")
        elif command == "dht-rebuilt":
            #DHT-REBUILT
            print("COMMAND NOT SUPPORTED")
        elif command == "deregister":
            #DEREGISTER COMMAND
            print("COMMAND NOT SUPPORTED")
        elif command == "teardown-dht":
            #TEARDOWN-DHT
            print("COMMAND NOT SUPPORTED")
        elif command == "teardown-complete":
            #TEARDOWN-COMPLETE
            print("COMMAND NOT SUPPORTED")
        elif command == "peer-setup":
            #SETUP PEER COMMAND
            #Duplicate Setup protection
            if PEER_SETUP:
                print("ERROR Peer already setup")
                continue 
            #call setup peer function
            peer_setup_result = setup_peer(tokens, peer2Peer_socket, peer2Manager_socket)
            #check if setup was successful and extract values
            if peer_setup_result:
                peer_name, peer_ip, m_port, p_port = peer_setup_result
                #UPDATE SETUP FLAG
                PEER_SETUP = True

        else:
            #UNKNOWN COMMAND
            print("COMMAND NOT SUPPORTED")
       

#Call peer function
if __name__ == "__main__":
    Peer()
    
