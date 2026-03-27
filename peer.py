import socket
import sys
import threading
import csv
import random
import time
#utility imports
from utils import send_message, read_message, count_storm_events_csv, compute_hashes, find_next_prime, is_prime, print_record

#GLOBAL VARIABLES
DHT_RING_SIZE = 0 # tracks size of DHT ring
hash_size = 0 #hash table size for computing pos and id
peers = [] #list of peers in the DHT
DHT_READY = False # prevents race condition in storing records
neighbor_ip = "" #tracks address of neighbor in DHT ring
neighbor_p_port = 0 # tracks neighbors peer 2 peer port in DHT ring
neighbor_id = 0 # tracks identifier for neightbor in DHT ring

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
def peer2peer_Listener(peer2peer_socket, peer2Manager_socket, MANAGER_IP, MANAGER_PORT):
    #import global variables for updating
    global hash_size
    global DHT_RING_SIZE
    global peers
    global DHT_READY
    global neighbor_id
    global neighbor_ip
    global neighbor_p_port

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
            hash_size = body["hash_size"]
            DHT_RING_SIZE = ring_size
            peers = peer_list
            
            #Determine right neighbor id and index in tuples
            neighbor_id = (identifier + 1) % ring_size
            neighbor_ip = peer_list[neighbor_id]["ip"]
            neighbor_p_port = peer_list[neighbor_id]["p_port"]
            DHT_READY = True

        elif peer_command == "store":
            #STORE COMMAND

            #Wait until peer is setup to prevent race condition
            while not DHT_READY:
                continue

            #EXTRACT DATA from body
            curr_id = body["destination_id"]
            curr_pos = body["pos"]
            record = body["record"]

            #Check ID does not match to propogate around the ring
            if curr_id != identifier:
                send_message(peer2peer_socket, (neighbor_ip, neighbor_p_port), "store", body)
                continue

            #If ID matches store in recordList
            peerRecordList[record["EVENT_ID"]] = Record(curr_pos, curr_id, record["EVENT_ID"], record["STATE"], record["YEAR"], record["MONTH"], record["EVENT_TYPE"], record["CZ_TYPE"], record["CZ_NAME"], record["INJURIES_DIRECT"], record["INJURIES_INDIRECT"], record["DEATHS_DIRECT"], record["DEATHS_INDIRECT"], record["DAMAGE_PROPERTY"], record["DAMAGE_CROPS"], record["TOR_F_SCALE"])
        
        elif peer_command == "find-event":
            #FIND EVENT COMMAND
            
            #EXTRACT PARAMETERS
            event_id = body["event_id"]
            S_ip = body["S_ip"]
            S_p_port = body["S_port"]
            id_seq = body["id_seq"]
            unvisited = body["unvisited_nodes"]
            is_first_node = body["first_node"]

            #check if node is the first visited to initialize unvisited list
            if is_first_node:
                unvisited = list(range(DHT_RING_SIZE))
                #set flag to off
                body["first_node"] = False

            #COMPUTE POS AND ID FOR EVENT
            curr_pos, curr_id = compute_hashes(hash_size, DHT_RING_SIZE, int(event_id))

            #update id_seq and unvisited_nodes
            id_seq.append(identifier)
            if identifier in unvisited:
                unvisited.remove(identifier)
            
            #check if id matches current peer
            if curr_id == identifier:
                #check local hash table
                if str(event_id) in peerRecordList:
                    #record found return record information
                    #get record
                    found_record = peerRecordList[str(event_id)]
                    #build message with record information
                    found_record_body = {"id_seq": id_seq, "command_type": "find-event", "record": {"EVENT_ID": found_record.event_id, "STATE": found_record.state, "YEAR": found_record.year, "MONTH": found_record.month_name, "EVENT_TYPE": found_record.event_type, "CZ_TYPE": found_record.cz_type, "CZ_NAME": found_record.cz_name, "INJURIES_DIRECT": found_record.injuries_direct, "INJURIES_INDIRECT": found_record.injuries_indirect, "DEATHS_DIRECT": found_record.deaths_direct, "DEATHS_INDIRECT": found_record.deaths_indirect, "DAMAGE_PROPERTY": found_record.damage_property, "DAMAGE_CROPS": found_record.damage_crops, "TOR_F_SCALE": found_record.tor_f_scale}}
                    #send message back to S peer
                    send_message(peer2peer_socket, (S_ip, S_p_port), "SUCCESS", found_record_body)
                else:
                    #record not found
                    send_message(peer2peer_socket, (S_ip, S_p_port), "FAILURE", {"command_type": "find-event", "event_id": event_id})
            else:
                #ID doen't match, propogate around the ring
                #check if all nodes have been visited to prevent infinite loop
                if len(unvisited)==0:
                    send_message(peer2peer_socket, (S_ip, S_p_port), "FAILURE", {"command_type": "find-event", "event_id": event_id})
                else:
                    #send to random next node
                    #get random unvisited node id
                    next_node_id = random.choice(unvisited)
                    #get next peer info from peer list
                    next_node = peers[next_node_id]
                    #update id_seq and unvisited_nodes for next search
                    body["id_seq"] = id_seq
                    body["unvisited_nodes"] = unvisited
                    send_message(peer2peer_socket, (next_node["ip"], next_node["p_port"]), "find-event", body) 

        elif peer_command == "teardown":
            #TEARDOWN COMMAND
            #get initiating peer (Leader)
            initiating_peer = body["initiating_peer"]
            #get teardown command type (for leave-dht check)
            teardown_type = body["teardown_type"]
            #delete local hash table
            peerRecordList.clear()
            #check if Leader
            if peers[identifier]["name"] == initiating_peer:
                #check command type
                if teardown_type == "teardown-standard":
                    #send teardown complete message to manager
                    send_message(peer2Manager_socket, (MANAGER_IP, MANAGER_PORT), "teardown-complete", initiating_peer)
                    print("TEARDOWN COMPLETE")
                elif teardown_type == "leave-dht":
                    #leave teardown is finished, start reset-id loop
                    #remove initiating peer from peer list and extract initiating peer information
                    peer_u = None
                    new_peers = []
                    for peer in peers:
                        if peer["name"] != initiating_peer:
                            new_peers.append(peer)
                        else:
                            peer_u = peer
                    #get new ring size
                    new_ring_size = len(new_peers)
                    #send reset-id command to neighbor
                    send_message(peer2peer_socket, (neighbor_ip, neighbor_p_port), "reset-id", {"peer_list": new_peers, "ring_size": new_ring_size, "current_id": 0, "initiating_peer": initiating_peer, "initiating_peer_ip": peer_u["ip"], "initiating_peer_port": peer_u["p_port"]})
            else:
                #propogate teardown around the ring
                send_message(peer2peer_socket, (neighbor_ip, neighbor_p_port), "teardown", body)

            #RESET GLOBAL VARIABLES
            DHT_READY = False
            DHT_RING_SIZE = 0
            hash_size = 0
            peers = []
            neighbor_ip = ""
            neighbor_p_port = 0
            neighbor_id = 0

        elif peer_command == "reset-id":
            #RESET-ID COMMAND
            #extract command parameters
            new_peers = body["peer_list"]
            new_ring_size = body["ring_size"]
            curr_id = body["current_id"]
            initiating_peer = body["initiating_peer"]
            initiating_peer_ip = body["initiaing_peer_ip"]
            initiating_peer_port = body["initiating_peer_port"]
            #check if loop has returned to peer u
            if peer[identifier]["name"] == initiating_peer:
                #reset-id complete
                #initiate rebuild-dht command
                new_leader = new_peers[0]
                send_message(peer2peer_socket, (new_leader["ip"], new_leader["p_port"]), "rebuild-dht", {})
            else:
                #update identifiers and DHT information
                identifier = curr_id
                neighbor_id = (identifier + 1) % new_ring_size
                neighbor_ip = new_peers[neighbor_id]["ip"]
                neighbor_p_port = new_peers[neighbor_id]["p_port"]
                DHT_RING_SIZE = new_ring_size
                peers = new_peers
                #propogate reset-id command around the ring
                #increment current id
                body["current_id"] = curr_id + 1
                send_message(peer2peer_socket, (neighbor_ip, neighbor_p_port), "reset-id", body)

        elif peer_command == "SUCCESS":
            #Get Command Type
            command_type = body["command_type"]
            if command_type == "find-event":
                #find event success, get record and id_seq
                record = body["record"]
                id_seq = body["id_seq"]
                print_record(record)
                print(f"ID_SEQ: {id_seq}")
        elif peer_command == "FAILURE":
            #get command type
            command_type = body["command_type"]
            if command_type == "find-event":
                #find event failure get event id
                event_id = body["event_id"]
                print(f"Storm event {event_id} not found in the DHT.")




#PEER CLI COMMANDS
#SETUP-PEER INTERNAL COMMAND
#
def setup_peer(tokens, peer2Peer_socket, peer2Manager_socket, MANAGER_IP, MANAGER_PORT):
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
	peer_thread = threading.Thread(target=peer2peer_Listener, args=(peer2Peer_socket, peer2Manager_socket, MANAGER_IP, MANAGER_PORT))
	peer_thread.start()

	#Output status
	print("Peer is setup")
	return peer_name, peer_ip, m_port, p_port

#REGISTER PEER COMMAND
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

#SETUP-DHT COMMAND
#
def setup_dht(tokens, peer_name, peer2Peer_socket, peer2Manager_socket, MANAGER_IP, MANAGER_PORT):
    #bring in global variables for ease of access later
    global DHT_RING_SIZE
    global hash_size
    global peers
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
    #Initialize list to track storage amounts 
    nodeStorageAmounts = {} #dictionary to store how many records are at each node

    #PARSING CSV FILE
    #Build filename
    selected_file = "Data/details_" + str(year) + ".csv"
    #get number of storm events
    num_of_events = count_storm_events_csv(selected_file)
    #compute hash table size
    hash_size = find_next_prime(num_of_events)

    #loop over peers to send set-id commands
    for i in range(0, len(peers)):
        peer = peers[i] #get current peer
        nodeStorageAmounts[i] = 0 #initialize node count to 0
        #build message for peer to peer id and neighbor assignments
        peer_assignment_body = {"peer_id": i, "ring_size": DHT_RING_SIZE, "peer_list": peers, "hash_size": hash_size}
        #send peer to peer message
        send_message(peer2Peer_socket, (peer["ip"], peer["p_port"]), "set-id", peer_assignment_body)
    
    #wait until all set-ids are set before parsing csv to prevent race condition and lost records
    while not DHT_READY:
        continue

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
            
            #build message for store command
            store_command_body = {"pos": curr_pos, "destination_id": curr_id, "record": {"EVENT_ID": curr_event_id,"STATE": curr_state,"YEAR": curr_year,"MONTH": curr_month_name,"EVENT_TYPE": curr_event_type,"CZ_TYPE": curr_cz_type,"CZ_NAME": curr_cz_name,"INJURIES_DIRECT": curr_injuries_direct,"INJURIES_INDIRECT": curr_injuries_indirect,"DEATHS_DIRECT": curr_deaths_direct,"DEATHS_INDIRECT": curr_deaths_indirect,"DAMAGE_PROPERTY": curr_damage_property,"DAMAGE_CROPS": curr_damage_crops,"TOR_F_SCALE": curr_tor_f_scale}}
            #send store command around DHT Ring
            send_message(peer2Peer_socket, (peers[0]["ip"], peers[0]["p_port"]), "store", store_command_body)
            #increment node storage amount for identifier
            nodeStorageAmounts[curr_id] = nodeStorageAmounts[curr_id] + 1
            time.sleep(0.0001)

    #PRINT DHT STATUS
    print("Records Distributed:")
    for key, value in nodeStorageAmounts.items():
        print(f"Node: {peers[key]['name']}, ID: {key}, Records Stored: {value}")
    
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
    return nodeStorageAmounts

#QUERY-DHT COMMAND
#
def query_dht(tokens, peer_name, peer_ip, p_port, peer2Peer_socket, peer2Manager_socket, MANAGER_IP, MANAGER_PORT):
    #INPUT VALIDATION
    if len(tokens) != 2:
        print("USAGE ERROR: query_dht <event_id>")
        return
    #EXTRACT PARAMATER
    query_event_id = int(tokens[1])
    #Send query message to manager
    send_message(peer2Manager_socket, (MANAGER_IP, MANAGER_PORT), "query-dht", peer_name)
    #Get response
    managerResponse_query, manager_address = peer2Manager_socket.recvfrom(4096)
    #Read response
    header, body = read_message(managerResponse_query)
    #Check for failure
    if header == "FAILURE":
        print("QUERY FAILED")
        return
    #Extract response paramaters
    query_peer_name = body["name"]
    query_peer_ip = body["ip"]
    query_peer_p_port = body["p_port"]
    #Build find-event message
    find_event_body = {"event_id": query_event_id, "S_name": peer_name, "S_ip": peer_ip, "S_port": p_port, "id_seq": [], "unvisited_nodes": [], "first_node": True}
    #send find-event message
    send_message(peer2Peer_socket, (query_peer_ip, query_peer_p_port), "find-event", find_event_body)
    #Print status for starting search
    print(f"Searching for event {query_event_id} through peer {query_peer_name}.")

#LEAVE-DHT COMMAND
#
def leave_dht(peer_name, peer2Peer_socket, peer2Manager_socket, MANAGER_IP, MANAGER_PORT):
    #send leave dht message to manager
    send_message(peer2Manager_socket, (MANAGER_IP, MANAGER_PORT), "leave-dht", peer_name)
    #get response
    leave_response, manager_address = peer2Manager_socket.recvfrom(4096)
    #read response
    header, body = read_message(leave_response)
    #check for failure
    if header == "FAILURE":
        print("LEAVE-DHT FAILED")
        return
    #Initiate teardown 
    send_message(peer2Peer_socket, (neighbor_ip, neighbor_p_port), "teardown", {"initiating_peer": peer_name, "teardown_type": "leave-dht"})

    
#JOIN_DHT COMMAND
#

#TEARDOWN-DHT COMMAND
#
def teardown_dht(peer_name, peer2Peer_socket, peer2Manager_socket, MANAGER_IP, MANAGER_PORT):
    #Send teardwon message to manager
    send_message(peer2Manager_socket, (MANAGER_IP, MANAGER_PORT), "teardown-dht", peer_name)
    #get manager response
    teardown_response, manager_address = peer2Manager_socket.recvfrom(4096)
    #read response
    header, body = read_message(teardown_response)
    #check for failure
    if header == "FAILURE":
        print("TEARDOWN-DHT FAILED")
        return
    #send teardown message around ring
    send_message(peer2Peer_socket, (neighbor_ip, neighbor_p_port), "teardown", {"initiating_peer": peer_name, "teardown_type": "teardown-standard"})
    print("TEARDOWN INITIATIED")
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
    #Leader variable
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
                nodeStorageAmounts = setup_dht_result

        elif command == "query-dht":
            #QUERY-DHT COMMAND
            query_dht(tokens, peer_name, peer_ip, p_port, peer2Peer_socket, peer2Manager_socket, MANAGER_IP, MANAGER_PORT)

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
            teardown_dht(peer_name, peer2Peer_socket, peer2Manager_socket, MANAGER_IP, MANAGER_PORT)

        elif command == "peer-setup":
            #SETUP PEER COMMAND
            #Duplicate Setup protection
            if PEER_SETUP:
                print("ERROR Peer already setup")
                continue 
            #call setup peer function
            peer_setup_result = setup_peer(tokens, peer2Peer_socket, peer2Manager_socket, MANAGER_IP, MANAGER_PORT)
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
    
