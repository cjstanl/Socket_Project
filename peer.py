import socket
import sys
import threading
import csv
import random
import time
#utility imports
from utils import send_message, read_message, count_storm_events_csv, compute_hashes, find_next_prime, print_record, Peer

#GLOBAL VARIABLES
#   COMMUNICAION VARIABLES
MANAGER_PORT = 0
MANAGER_IP = ""
peer2Manager_socket = None
peer2Peer_socket = None
#   PEER VARIABLES
this_peer = None #Global peer object for ease of access
PEER_SETUP = False #FLAG: tracks if peer is setup
identifier = 0 #DHT id number
#   DHT VARIABLES
DHT_READY = False # prevents race condition in storing records
TEARDOWN_COMPLETE = False #tracks if the Teardown of the DHT is completed
DHT_RING_SIZE = 0 # tracks size of DHT ring
hash_size = 0 #hash table size for computing pos and id
peers_list = [] #list of peers in the DHT
neighbor_ip = "" #tracks address of neighbor in DHT ring
neighbor_p_port = 0 # tracks neighbors peer 2 peer port in DHT ring
neighbor_id = 0 # tracks identifier for neightbor in DHT ring
nodeStorageAmounts = {} #dictionary to store how many records are at each node
csv_file = "" #tracks name of csv file to be read for rebuilding the dht
LEAVE_COMPLETE = False # tracks if leave operation is complete
JOIN_COMPLETE = False # tracks if join operation is complete
JOIN_PENDING_PEER_IP = "" #holds joining peer IP address for join operation
JOIN_PENDING_PEER_PORT = 0 #holds joining peer p_port for join operation
JOIN_PENDING = False #tracks if a join operation is in progress

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

#PEER TO PEER THREAD LISTENER
#   - Function takes the peer socket and listens infintely for peer messages and handles all peer to peer commands
#   - Listens for messages and then reads them into a header and body for parsing
#   - Thread is started as daemon for graceful exit on deregister
#   - COMMANDS:
#       - set-id: sets the global DHT info for the peer
#       - store: handles storing of record at this peer or propogates around the ring
#       - find-event: searches local DHT table for record event id
#       - teardown: deletes local hash table and propogates to next node in DHT
#       - teardown-acknowledge: ack command for teardown used in join-dht
#       - reset-id: renumbering of identifiers for dht-rebuild
#       - join: sets joining peer information and initiates rebuild process
#       - SUCCESS: on receipt of success take the appropriate actions depending on rebuild or find event process
#       - FAILURE: used for all nodes searched in find-event loop
#       - exit: exits listening thread for graceful process termination on deregister
def peer2peer_Listener():
    #IMPORT GLOBAL VARIABLES
    #   - FLAGS
    global TEARDOWN_COMPLETE
    global JOIN_COMPLETE
    global LEAVE_COMPLETE
    global DHT_READY
    global JOIN_PENDING
    #   - DHT INFO
    global hash_size
    global DHT_RING_SIZE
    global peers_list   
    global neighbor_id
    global neighbor_ip
    global neighbor_p_port
    global identifier
    global csv_file
    #   - JOININING PEER ADDRESS
    global JOIN_PENDING_PEER_IP
    global JOIN_PENDING_PEER_PORT

    peerRecordList = {} #dictionary to store records keyed by eventID

    #PEER TO PEER LISTENING LOOP
    while True:
        #READ CURRENT MESSAGE
        payload, peer_address = peer2Peer_socket.recvfrom(4096) #get message from socket
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
            DHT_RING_SIZE = body["ring_size"]
            peers_list = body["peer_list"]
            hash_size = body["hash_size"]
            csv_file = body["csv_file"]
            
            #DETERMINE RIGHT NEIGHBOR ID, IP, AND P PORT
            neighbor_id = (identifier + 1) % DHT_RING_SIZE
            neighbor_ip = peers_list[neighbor_id]["ip"]
            neighbor_p_port = peers_list[neighbor_id]["p_port"]
            DHT_READY = True

        elif peer_command == "store":
            #STORE COMMAND

            #CHECK IF DHT IS READY FOR STORAGE
            while not DHT_READY:
                continue

            #EXTRACT DATA FROM MESSAGE
            curr_id = body["destination_id"]
            curr_pos = body["pos"]
            record = body["record"]

            #CHECK IF CURRENT NODE STORES RECORD OR PROPOGATE AROUND THE RING
            if curr_id != identifier:
                send_message(peer2Peer_socket, (neighbor_ip, neighbor_p_port), "store", body)
                continue

            #STORE RECORD AT THIS NODE
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

            #CHECK IF UNVISITED IS INITIALIZED
            if is_first_node:
                unvisited = list(range(DHT_RING_SIZE))
                #set first node flag to false
                body["first_node"] = False

            #COMPUTE POS AND ID FOR EVENT
            curr_pos, curr_id = compute_hashes(hash_size, DHT_RING_SIZE, int(event_id))

            #UPDATE ID_SEQ AND UNVISITED
            id_seq.append(identifier)
            if identifier in unvisited:
                unvisited.remove(identifier)
            
            #CHECK IF ID MATCHES
            if curr_id == identifier:
                #check local hash table
                if str(event_id) in peerRecordList:
                    #record found return record information
                    #get record
                    found_record = peerRecordList[str(event_id)]
                    #build message with record information
                    found_record_body = {"id_seq": id_seq, "command_type": "find-event", "record": {"EVENT_ID": found_record.event_id, "STATE": found_record.state, "YEAR": found_record.year, "MONTH": found_record.month_name, "EVENT_TYPE": found_record.event_type, "CZ_TYPE": found_record.cz_type, "CZ_NAME": found_record.cz_name, "INJURIES_DIRECT": found_record.injuries_direct, "INJURIES_INDIRECT": found_record.injuries_indirect, "DEATHS_DIRECT": found_record.deaths_direct, "DEATHS_INDIRECT": found_record.deaths_indirect, "DAMAGE_PROPERTY": found_record.damage_property, "DAMAGE_CROPS": found_record.damage_crops, "TOR_F_SCALE": found_record.tor_f_scale}}
                    #send message back to S peer
                    send_message(peer2Peer_socket, (S_ip, S_p_port), "SUCCESS", found_record_body)
                else:
                    #record not found
                    send_message(peer2Peer_socket, (S_ip, S_p_port), "FAILURE", {"command_type": "find-event", "event_id": event_id})
            else:
                #ID DOESN'T MATCH: HOT POTATO PROTOCOL
                #check if all nodes have been visited to prevent infinite loop
                if len(unvisited)==0:
                    send_message(peer2Peer_socket, (S_ip, S_p_port), "FAILURE", {"command_type": "find-event", "event_id": event_id})
                else:
                    #send to random next node
                    #get random unvisited node id
                    next_node_id = random.choice(unvisited)
                    #get next peer info from peer list
                    next_node = peers_list[next_node_id]
                    #update id_seq and unvisited_nodes for next search
                    body["id_seq"] = id_seq
                    body["unvisited_nodes"] = unvisited
                    send_message(peer2Peer_socket, (next_node["ip"], next_node["p_port"]), "find-event", body) 

        elif peer_command == "teardown":
            #TEARDOWN COMMAND
            
            #EXTRACT PARAMETER
            initiating_peer = body["initiating_peer"]

            #CHECK IF TEARDOWN COMPLETE
            if this_peer.name == initiating_peer:
                peerRecordList.clear()
                TEARDOWN_COMPLETE = True
                #notify joining peer if pending
                if JOIN_PENDING:
                    send_message(peer2Peer_socket, (JOIN_PENDING_PEER_IP, JOIN_PENDING_PEER_PORT), "teardown-acknowledge", {"peers_list": peers_list, "hash_size": hash_size, "csv_file": csv_file, "current_DHT_SIZE": DHT_RING_SIZE})
                    #reset join pending flag
                    JOIN_PENDING = False
            else:
                #propogate around the ring
                peerRecordList.clear()
                send_message(peer2Peer_socket, (neighbor_ip, neighbor_p_port), "teardown", body)
            
        elif peer_command == "teardown-acknowledge":
            #TEARDOWN-ACKNOWLEDGE COMMAND

            #SET DHT INFO
            peers_list = body["peers_list"]
            hash_size = body["hash_size"]
            csv_file = body["csv_file"]
            DHT_RING_SIZE = body["current_DHT_SIZE"]
            DHT_READY = True
            #set teardown complete flag and reset joining IP
            TEARDOWN_COMPLETE = True
            JOIN_PENDING_PEER_IP = ""

        elif peer_command == "reset-id":
            #RESET-ID COMMAND

            #EXTRACT PARAMETERS
            initiating_peer = body["initiating_peer"]
            curr_id = body["current_id"]
            new_peers_list = body["new_peers_list"]
            new_ring_size = body["new_ring_size"]
            
            #Check if reset-id has propogated back to initializing node
            if curr_id == new_ring_size:
                #send rebuild command to neighbor
                send_message(peer2Peer_socket, (new_peers_list[0]["ip"], new_peers_list[0]["p_port"]), "rebuild-dht", {"initiating_peer": initiating_peer, "initiating_peer_ip": body["initiating_peer_ip"], "initiating_peer_port": body["initiating_peer_port"]})
            else:
                #update peer information
                identifier = curr_id
                peers_list = new_peers_list
                DHT_RING_SIZE = new_ring_size
                neighbor_id = (identifier + 1) % DHT_RING_SIZE
                neighbor_ip = peers_list[neighbor_id]["ip"]
                neighbor_p_port = peers_list[neighbor_id]["p_port"]
                #increment current id
                body["current_id"] = curr_id + 1
                #propogate reset to next node
                send_message(peer2Peer_socket, (neighbor_ip, neighbor_p_port), "reset-id", body)

        elif peer_command == "rebuild-dht":
            #REBUILD-DHT COMMAND
            
            #EXTRACT PARAMETERS
            initiating_peer = body["initiating_peer"]
            initiating_peer_ip = body["initiating_peer_ip"]
            initiating_peer_port = body["initiating_peer_port"]

            #Redistribute records
            DHT_READY = True
            store_DHT()
            print_record_distribution()

            #SEND SUCCESS MESSAGE BACK TO INITIATING PEER
            send_message(peer2Peer_socket, (initiating_peer_ip, initiating_peer_port), "SUCCESS", {"command_type": "rebuild-dht", "new_leader": this_peer.name})
     
        elif peer_command == "join":
            #JOIN COMMAND

            #store joining peers address
            JOIN_PENDING_PEER_IP = body["joining_ip"]
            JOIN_PENDING_PEER_PORT = body["joining_p_port"]
            #set JOIN pending flag
            JOIN_PENDING = True
            #INITIATIE TEARDOWN
            send_message(peer2Peer_socket, (neighbor_ip, neighbor_p_port), "teardown", {"initiating_peer": this_peer.name})

        elif peer_command == "SUCCESS":
            #Get Command Type
            command_type = body["command_type"]

            #SUCCESS DECISION TREE
            if command_type == "find-event":
                #find event success, get record and id_seq
                record = body["record"]
                id_seq = body["id_seq"]
                print_record(record)
                print(f"ID_SEQ: {id_seq}")
            elif command_type == "rebuild-dht":
                #rebuild dht success
                #extract new leader and initiating peer from body
                initiating_peer = this_peer.name
                new_leader = body["new_leader"]
                #send complete message to manager
                send_message(peer2Manager_socket, (MANAGER_IP, MANAGER_PORT), "dht-rebuilt", initiating_peer + " " + new_leader)
                #get response
                rebuild_response, manager_address = peer2Manager_socket.recvfrom(4096)
                #read resposne
                header, body = read_message(rebuild_response)
                #check for success
                if header == "SUCCESS":
                    print(f"DHT has been successfully rebuilt - new leader: {new_leader}")
                else:
                    print("DHT rebuild has failed.")
                LEAVE_COMPLETE = True
                JOIN_COMPLETE = True

        elif peer_command == "FAILURE":
            #get command type
            command_type = body["command_type"]
            if command_type == "find-event":
                #find event failure get event id
                event_id = body["event_id"]
                print(f"Storm event {event_id} not found in the DHT.")

        elif peer_command == "exit":
            #EXIT COMMAND for graceful deregister and process stop
            break


#PEER UTILITY COMMANDS
#STORE_DHT
#   - This is an auxillary function to simplify the parsing of csv files and sending of store commands used in setup-dht and rebuild-dht
#   - Message Format: Header = store, Body = <pos> <destination_id> <record>
def store_DHT():
    #IMPORT GLOBAL VARIABLES
    global nodeStorageAmounts
    #initialize node storage
    nodeStorageAmounts = {}
    for index in range(DHT_RING_SIZE):
        nodeStorageAmounts[index] = 0
    #PARSE CSV FILE
    with open(csv_file, newline='') as stormcsv:
        #open file with csv reader
        reader = csv.DictReader(stormcsv)
        #Loop over all storm events
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
            send_message(peer2Peer_socket, (peers_list[0]["ip"], peers_list[0]["p_port"]), "store", store_command_body)
            #increment node storage amount for identifier
            nodeStorageAmounts[curr_id] = nodeStorageAmounts[curr_id] + 1
            time.sleep(0.0001)

#PRINT RECORDS DISTRIBUTION
#   - This is an auxillary function to print the current distribution of records
def print_record_distribution():
    #PRINT DHT STATUS
    print("Records Distributed:")
    for key, value in nodeStorageAmounts.items():
        print(f"Node: {peers_list[key]['name']}, ID: {key}, Records Stored: {value}")

#PEER CLI COMMANDS
#SETUP-PEER INTERNAL COMMAND
#   - This is an auxillary function to simplify the setting of peer variables used in later functions
#   - Parameter:
#       - tokens: tokenized command line inputs 
def setup_peer(tokens):
    #INPUT VALIDATION
    if len(tokens) != 5:
        print("USAGE ERROR: peer-setup <peer name> <peer ip> <manager port> <peer port>")
        return False
    
    #IMPORT GLOBAL VARIABLE
    global this_peer
    global PEER_SETUP
    global peer2Peer_socket
    global peer2Manager_socket
	
	#EXTRACT PARAMETERS
    peer_name = tokens[1]
    peer_ip = tokens[2]
    m_port = int(tokens[3])
    p_port = int(tokens[4])
	
    #Create Peer Object
    this_peer = Peer(peer_name, peer_ip, m_port, p_port, "Free")
    PEER_SETUP = True

	#set up UDP sockets
    peer2Peer_socket.bind((peer_ip, p_port))
    peer2Manager_socket.bind((peer_ip, m_port))

	#start peer listening thread for peer to peer commands
    peer_thread = threading.Thread(target=peer2peer_Listener)
    peer_thread.start()

    #Setup successful
    return True
    
#REGISTER PEER COMMAND
#   - This function registers the peer with the manager in accordance with the document specifications section 1.1
#   - Message Format: Header = register, Body = <peer-name> <IPv4-address> <m-port> <p-port>
#   - Response Format: Header = SUCCESS/FAILURE, Body = NULL
def register_peer():
	#Build message
    register_message = this_peer.name + " " + this_peer.ip + " " + str(this_peer.m_port) + " " + str(this_peer.p_port)
	#Send message to manager
    send_message(peer2Manager_socket, (MANAGER_IP, MANAGER_PORT), "register", register_message)
	#Get response from manager
    response_register, manager_address = peer2Manager_socket.recvfrom(4096)
	#read response 
    header, body = read_message(response_register)
	#Check for failure and output status
    if header == "FAILURE":
        print(f"Peer {this_peer.name} failed to register.")
    else:
        print(f"Peer {this_peer.name} is registered with the manager.")

#SETUP-DHT COMMAND
#   -This function sets up the distributed hash table in accordance with the document specifications 1.1 and 1.2.1
#   - PARAMETER:
#       - tokens: tokenized command line input containing the size of the DHT and the year to access records
#   - Message Formats:
#       - peer to manager setup-DHT: Header = setup-dht, Body = <n> <YYYY>
#       - peer to peer set-id:  Header = set-id, Body = <peer_id> <ring_size> <peer_list> <hash_size>
#       - peer to manager dht-complete: Header = dht-complete, Body = <leader name>
#   - Response Formats:
#       - manager to peer setup-dht: Header = SUCCESS/FAILURE, Body = NULL
#       - manager to peer dht-complete: Header = SUCCESS?FAILURE, Body = NULL
def setup_dht(tokens):
    #INPUT VALIDATION
    if len(tokens) != 3:
        print("USAGE ERROR: setup-dht n YYYY")
        return
    
    #IMPORT GLOBAL VARIABLES
    global DHT_RING_SIZE
    global hash_size
    global peers_list
    global nodeStorageAmounts
    global csv_file
    
    #EXTRACT PARAMETERS FROM CLI
    n = int(tokens[1])
    year = int(tokens[2])
    #CLI PARAMETER VALIDATION
    #n must be greater than or equal to 3 and year must be a valid 4 digit year
    if n < 3 or year < 1900 or year > 2027:
        print("USAGE ERROR (Parameters): setup-dht n YYYY (n must be greater than or equal to 3)")
        return
    
    #INTIAL SETUP-DHT PEER TO MANAGE MESSAGE
    #build message for manager
    setupMessage = this_peer.name + " " + str(n) + " " + str(year)
    #send message to manager
    send_message(peer2Manager_socket, (MANAGER_IP, MANAGER_PORT), "setup-dht", setupMessage)
    #get response
    managerResponse, manager_address = peer2Manager_socket.recvfrom(4096)
    #read response
    header, body = read_message(managerResponse)
    #check for failure
    if header == "FAILURE":
        print("DHT setup has failed.")
        return
    #extract peer list and dht size from response
    peers_list = body["peers"]
    DHT_RING_SIZE = body["dht_size"]
    
    #DHT INITIALIZATION VALUES
    #Build filename
    csv_file = "Data/details_" + str(year) + ".csv"
    #get number of storm events
    num_of_events = count_storm_events_csv(csv_file)
    #compute hash table size
    hash_size = find_next_prime(num_of_events)
    
    #SET ID MESSAGES PEER TO PEER
    #loop over peers to send set-id commands
    for i in range(0, len(peers_list)):
        peer = peers_list[i] #get current peer
        nodeStorageAmounts[i] = 0 #initialize node count to 0
        #build message for peer to peer id and neighbor assignments
        peer_assignment_body = {"peer_id": i, "ring_size": DHT_RING_SIZE, "peer_list": peers_list, "hash_size": hash_size, "csv_file": csv_file}
        #send peer to peer message
        send_message(peer2Peer_socket, (peer["ip"], peer["p_port"]), "set-id", peer_assignment_body)
    

    #WAIT FOR DHT TO BE READY: prevents dropped records in store command
    while not DHT_READY:
        continue

    #PARSE CSV FILE
    store_DHT()

    #PRINT DHT STATUS
    print_record_distribution()
    
    #DHT-COMPLETE MESSAGE TO MANAGER
    send_message(peer2Manager_socket, (MANAGER_IP, MANAGER_PORT), "dht-complete", this_peer.name)
    #get response
    dht_complete_response, manager_address = peer2Manager_socket.recvfrom(4096)
    #read response
    dht_complete_header, dht_complete_body = read_message(dht_complete_response)
    #check for failure
    if dht_complete_header == "FAILURE":
        print("DHT setup failed to complete.")
        return
    print("DHT is setup.")
    return 

#QUERY-DHT COMMAND
#   -This function handles the querying of the DHT with hot potato protocol in accordance with the document specifications 1.1 and 1.2.2
#   - Parameter:
#       - tokens: tokenized command line input containing the event-id being searched for
#   - Message Formats:
#       - peer to manager query dht: Header = query-dht, Body = <peer name>
#       - peer to peer find event: Header = find-event, Body = <event id> <S_name> <S_ip> <S_port> <id_seq> <unvisited_nodes> <first_Node>
#   - Response Formats: 
#       - manager to peer query dht: Header = SUCCESS/FAILURE, Body = <query peer name> <query peer ip> <query peer p port>
def query_dht(tokens):
    #INPUT VALIDATION
    if len(tokens) != 2:
        print("USAGE ERROR: query_dht <event_id>")
        return

    #EXTRACT PARAMATER
    query_event_id = int(tokens[1])

    #PEER TO MANAGER QUERY MESSAGE
    send_message(peer2Manager_socket, (MANAGER_IP, MANAGER_PORT), "query-dht", this_peer.name)
    #Get response
    managerResponse_query, manager_address = peer2Manager_socket.recvfrom(4096)
    #Read response
    header, body = read_message(managerResponse_query)
    #Check for failure
    if header == "FAILURE":
        print("QUERY FAILED")
        return
    
    #EXTRACT PARAMETERS
    query_peer_name = body["name"]
    query_peer_ip = body["ip"]
    query_peer_p_port = body["p_port"]

    #PEER TO PEER FIND EVENT MESSAGE
    #Build find-event message
    find_event_body = {"event_id": query_event_id, "S_name": this_peer.name, "S_ip": this_peer.ip, "S_port": this_peer.p_port, "id_seq": [], "unvisited_nodes": [], "first_node": True}
    #send find-event message
    send_message(peer2Peer_socket, (query_peer_ip, query_peer_p_port), "find-event", find_event_body)
    #Print status for starting search
    print(f"Searching for event: {query_event_id}, through peer: {query_peer_name}.")

#LEAVE-DHT COMMAND
#   - This function handles the leave operation for a node inside the DHT in accordance with the document specifications 1.1 and 1.2.3
#   - Message Formats:
#       - peer to manager leave-dht: Header = leave-dht, Body = <peer_name>
#       - peer to peer reset-id: Header = reset-id, Body = <initiating_peer_ip> <initiating_peer_port> <current_id> <new_ring_size> <new_peer_list> <hash_size>
#   - Response Formats:
#       - manager to peer leave-dht: Header = SUCCESS/FAILURE, Body = NULL
def leave_dht():
    #IMPORT GLOBAL VARIABLES
    global DHT_RING_SIZE
    global TEARDOWN_COMPLETE

    #RESET TEARDOWN FLAG
    TEARDOWN_COMPLETE = False

    #SEND LEAVE-DHT MESSAGE TO MANAGER
    send_message(peer2Manager_socket, (MANAGER_IP, MANAGER_PORT), "leave-dht", this_peer.name)
    #get response
    leave_response, manager_address = peer2Manager_socket.recvfrom(4096)
    #read response
    header, body = read_message(leave_response)
    #check for failure
    if header == "FAILURE":
        print("Leave DHT operation failed")
        return

    #INITIATE TEARDOWN
    teardown_dht()
    #wait until teardown is complete
    while not TEARDOWN_COMPLETE:
        continue
    #reset teardown complete flag
    TEARDOWN_COMPLETE = False

    #INITIATE ID RESET
    #get new leader index
    new_leader_index = 0
    for index in range(len(peers_list)):
        if peers_list[index]["name"] == this_peer.name:
            new_leader_index = index
            break
    #create new peer list
    new_peers_list = []
    for index in range(1, DHT_RING_SIZE):
        new_peers_list.append(peers_list[(new_leader_index + index) % DHT_RING_SIZE])
    #set new ring size
    DHT_RING_SIZE = DHT_RING_SIZE - 1

    #SEND RESET-ID MESSAGE TO NEIGHBOR
    send_message(peer2Peer_socket, (neighbor_ip, neighbor_p_port), "reset-id", {"initiating_peer": this_peer.name, "initiating_peer_ip": this_peer.ip, "initiating_peer_port": this_peer.p_port, "current_id": 0, "new_ring_size": DHT_RING_SIZE, "new_peers_list": new_peers_list, "hash_size": hash_size})


    
#JOIN_DHT COMMAND
#   - This function handles the join operation for a node outside the DHT in accordance with the document specifications 1.1 and 1.2.4
#   - Message Formats:
#       - peer to manager join-dht: Header = join-dht, Body = <peer_name>
#       - peer to leader join: Header = join, Body = <joining_peer_name> <joining_peer_ip> <joining_peer_p_port>
#       - peer to peer reset-id: Header = reset-id, Body = <initiating_peer_name> <initiating_peer_ip> <initiating_peer_port> <current_id> <new_ring_size> <new_peer_list> <hash_size>
#   - Response Formats:
#       - manager to peer join-dht: Header = SUCCESS/FAILURE, Body = <leader_name> <leader_ip> <leader_p_port>
def join_dht():
    #IMPORT GLOBAL VARIABLES
    global TEARDOWN_COMPLETE
    global JOIN_COMPLETE
    global DHT_RING_SIZE
    global neighbor_id
    global neighbor_ip
    global neighbor_p_port
    global identifier
    global peers_list


    #SEND JOIN-DHT MESSAGE TO MANAGER
    send_message(peer2Manager_socket, (MANAGER_IP, MANAGER_PORT), "join-dht", this_peer.name)
    #get response
    leave_response, manager_address = peer2Manager_socket.recvfrom(4096)
    #read response
    header, body = read_message(leave_response)
    #check for failure
    if header == "FAILURE":
        print("Join DHT operation failed")
        return
    
    #EXTRACT LEADER PEER FOR REBUILD
    leader_name = body["name"]
    leader_ip = body["ip"]
    leader_p_port = body["p_port"]

    #SEND JOIN MESSAGE TO LEADER
    send_message(peer2Peer_socket, (leader_ip, leader_p_port), "join", {"joining_name": this_peer.name, "joining_ip": this_peer.ip, "joining_p_port": this_peer.p_port})

    #WAIT FOR TEARDOWN TO COMPLETE
    while not TEARDOWN_COMPLETE:
        continue
    #reset join flag
    TEARDOWN_COMPLETE = False

    #BUILD NEW PEER LIST
    new_peers_list = list(peers_list)
    new_peers_list.append({"name": this_peer.name, "ip": this_peer.ip, "p_port": this_peer.p_port})
    DHT_RING_SIZE = DHT_RING_SIZE + 1

    #INITIATE RESET-ID
    send_message(peer2Peer_socket, (new_peers_list[0]["ip"], new_peers_list[0]["p_port"]), "reset-id", {"initiating_peer": this_peer.name, "initiating_peer_ip": this_peer.ip, "initiating_peer_port": this_peer.p_port, "current_id": 0, "new_ring_size": DHT_RING_SIZE, "new_peers_list": new_peers_list, "hash_size": hash_size})
    
    #WAIT FOR REBUILD TO COMPLETE
    while not JOIN_COMPLETE:
        continue
    #reset join flag
    JOIN_COMPLETE = False

    #SET DHT INFO FOR JOINING PEER
    identifier = DHT_RING_SIZE - 1 #joining node will always be last
    neighbor_id = 0
    neighbor_ip = new_peers_list[0]["ip"]
    neighbor_p_port = new_peers_list[0]["p_port"]
    peers_list = new_peers_list

    #JOIN COMPLETED
    print(f"Peer {this_peer.name} has joined the DHT.")

#TEARDOWN-DHT COMMAND
#   - This function deletes the DHT in a simple way so that it can be called from rebuild dht
#   - Message Format: peer to peer teardown message: Header = teardown, Body = <initiating_peer>
def teardown_dht():

    #Send message to right neighbor
    send_message(peer2Peer_socket, (neighbor_ip, neighbor_p_port), "teardown", {"initiating_peer": this_peer.name})
    print("Teardown initiatied")

#PEER
#   - Main Peer Function takes commands from STDIN and interacts with system through UDP sockets to manager and other peer protocols
#   - INPUT: peer.py <MANAGER_IP> <MANAGER_PORT>
#   - EXIT: call deregister command to stop  listening thread and exit gracefully
def Peer_Main():
    #IMPORT GLOBAL VARIABLES
    #   -FLAGS
    global PEER_SETUP
    global TEARDOWN_COMPLETE
    #   -COMMUNICATION
    global MANAGER_IP
    global MANAGER_PORT
    global peer2Peer_socket
    global peer2Manager_socket
    #   -PEER AND DHT VARIABLES
    global this_peer
    global nodeStorageAmounts
    

    #PROCESS STARTUP CLI INPUT VALIDATION
    if len(sys.argv) != 3:
        print("USAGE ERROR: peer.py <MANAGER_IP> <MANAGER_PORT>")
        sys.exit(1)

    #EXTRACT COMMAND LINE ARGUMENTS
    MANAGER_IP = sys.argv[1]
    MANAGER_PORT = int(sys.argv[2])

    #CREATE UDP SOCKETS
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
            register_peer()

        elif command == "setup-dht":
            #SETUP-DHT COMMAND
            setup_dht(tokens)

        elif command == "query-dht":
            #QUERY-DHT COMMAND
            query_dht(tokens)

        elif command == "leave-dht":
            #LEAVE-DHT COMMAND
            leave_dht()

        elif command == "join-dht":
            #JOIN-DHT COMMAND
            join_dht()
            
        elif command == "deregister":
            #DEREGISTER COMMAND

            #SEND DEREGISTER TO MANAGER
            send_message(peer2Manager_socket, (MANAGER_IP, MANAGER_PORT), "deregister", this_peer.name)
            #get response
            deregister_response, manager_address = peer2Manager_socket.recvfrom(4096)
            #read response
            header, body = read_message(deregister_response)
            #check for failure
            if header == "FAILURE":
                print("deregister failed")
            else:
                #Successful deregister, exit program
                #send thread exit command
                send_message(peer2Peer_socket, (this_peer.ip, this_peer.p_port), "exit")
                print(f"Peer {this_peer.name} is deregistered")
                sys.exit(0)

        elif command == "teardown-dht":
            #TEARDOWN-DHT COMMAND

            #SEND TEARDOWN TO MANAGER
            send_message(peer2Manager_socket, (MANAGER_IP, MANAGER_PORT), "teardown-dht", this_peer.name)
            #get manager response
            teardown_response, manager_address = peer2Manager_socket.recvfrom(4096)
            #read response
            header, body = read_message(teardown_response)
            #check for failure
            if header == "FAILURE":
                print("DHT teardown has failed")
                continue

            #CALL TEARDOWN FUNCTION
            teardown_dht()
            #WAIT FOR TEARDOWN TO FINISH
            while not TEARDOWN_COMPLETE:
                continue

            #SEND TEARDOWN COMPLETE TO MANAGER
            send_message(peer2Manager_socket, manager_address, "teardown-complete", this_peer.name)
            #get manager response
            teardown_response_complete, manager_address = peer2Manager_socket.recvfrom(4096)
            #read response
            header, body = read_message(teardown_response_complete)
            #check for failure
            if header == "FAILURE":
                print("Teardown has failed.")
            else:
                print("Teardown is complete")
            #reset teardown flag for future use
            TEARDOWN_COMPLETE = False

        elif command == "peer-setup":
            #SETUP PEER COMMAND

            #Duplicate Setup protection
            if PEER_SETUP:
                print("ERROR Peer already setup")
                continue 

            #CALL SETUP FUNCTION
            peer_setup_result = setup_peer(tokens)
            #check if setup was successful
            if peer_setup_result:
                print(f"Peer {this_peer.name} is setup and ready to register.")
            else:
                print("Peer setup has failed")

        else:
            #UNKNOWN COMMAND
            print("COMMAND NOT SUPPORTED")
       

#CALL PEER_MAIN FUNCTION
if __name__ == "__main__":
    Peer_Main()
    
