import socket
import sys
import threading
import csv

#UTILITY FUNCTIONS
def count_storm_events_csv(filename):
    """
    Reads a CSV file containing storm data and counts the total number of event records.
    
    Parameters:
        filename (str): The path to the CSV file to be read.
        
    Returns:
        int: The total number of events (rows) in the CSV, excluding the header.
    """
    event_count = 0
    with open(filename, 'r', newline='') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader) # Skip the header row
        for row in csv_reader:
            event_count += 1
    return event_count

def compute_hashes(hash_table_size, ring_size, event_id):
    """
    Computes the local table index and the peer ownership for a given event ID.
    
    Parameters:
        hash_table_size (int): The size of the hash table (should be the first prime number larger than 2 * total_events).
        ring_size (int): The total number of clients/nodes in the DHT ring.
        event_id (int): The unique identifier for the parsed storm event.
        
    Returns:
        tuple: A tuple containing:
            - pos (int): The local table index where the event is stored.
            - client_id (int): The ID of the client that owns this event.
    """
    pos = event_id % hash_table_size # local table index
    client_id = pos % ring_size      # peer ownership
    return (pos, client_id)

def find_next_prime(total_storm_events):
    """
    Finds the first prime number that is strictly greater than 2 * total_storm_events.
    This value is used to determine the appropriate size for the hash table.
    
    Parameters:
        total_storm_events (int): The total number of events parsed from the CSV.
        
    Returns:
        int: The next prime number greater than 2 * total_storm_events.
    """
    current_num = total_storm_events * 2 + 1

    while True:
        if (is_prime(current_num)):
            return current_num
        else:
            current_num = current_num + 1

def is_prime(num):
    """
    Determines whether a given number is a prime number.
    
    Parameters:
        num (int): The number to check for primality.
        
    Returns:
        bool: True if the number is prime, False otherwise.
    """
    # base cases, should never happen
    if num <= 1:
        return False
    elif num == 2:
        return True
    elif num == 3:
        return True

    sqr_root_value = int(num ** 0.5)
    for i in range (3, sqr_root_value + 1, 2):
        if (num % i == 0):
            return False

    return True

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
        #normalize payload (peer message) 
        message = payload.decode().strip()
        #tokenize message for parsing using split
        peer_tokens = message.split()
        #Check for empty message
        if not peer_tokens:
            continue

        #EXTRACT COMMAND
        peer_command = peer_tokens[0]

        #PEER-PEER COMMAND DECISION TREE
        if peer_command == "set-id":
            #SET-ID COMMAND

            #EXTRACT DHT DATA
            identifier = int(peer_tokens[1])
            ring_size = int(peer_tokens[2])
            tuples = peer_tokens[3:]
            
            #Determine right neighbor id and index in tuples
            neighbor_id = (identifier + 1) % ring_size
            neighbor_index = neighbor_id*3

            #Extract right neighbor data
            neighbor_name = tuples[neighbor_index]
            neighbor_ip = tuples[neighbor_index+1]
            neighbor_pPort = int(tuples[neighbor_index+2])

        if peer_command == "store":
            #STORE COMMAND

            #Wait until peer is setup to prevent race condition
            while not neighbor_ip:
                continue

            #EXTRACT ID DATA
            curr_id = int(peer_tokens[2])

            #Check ID does not match to propogate around the ring
            if curr_id != identifier:
                peer2peer_socket.sendto(message.encode(), (neighbor_ip, neighbor_pPort))
                continue

            #If ID matches store in recordList
            #normalize storm record data
            records_fields = peer_tokens[3].split(",")
            #extract values
            curr_pos = int(peer_tokens[1])
            curr_event_id = records_fields[0]
            curr_state = records_fields[1]
            curr_year = records_fields[2]
            curr_month_name = records_fields[3]
            curr_event_type = records_fields[4]
            curr_cz_type = records_fields[5]
            curr_cz_name = records_fields[6]
            curr_injuries_direct = records_fields[7]
            curr_injuries_indirect = records_fields[8]
            curr_deaths_direct = records_fields[9]
            curr_deaths_indirect = records_fields[10]
            curr_damage_property = records_fields[11]
            curr_damage_crops = records_fields[12]
            curr_tor_f_scale = records_fields[13]

            #add record to list
            peerRecordList[curr_event_id] = Record(curr_pos, curr_id, curr_event_id, curr_state, curr_year, curr_month_name, curr_event_type, curr_cz_type, curr_cz_name, curr_injuries_direct, curr_injuries_indirect, curr_deaths_direct, curr_deaths_indirect, curr_damage_property, curr_damage_crops, curr_tor_f_scale)


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

            #build message
            registerMessage = "register " + peer_name + " " + peer_ip + " " + str(m_port) + " " + str(p_port)
            #send message to manager
            peer2Manager_socket.sendto(registerMessage.encode(), (MANAGER_IP, MANAGER_PORT))
            #get response
            managerResponse, manager_address = peer2Manager_socket.recvfrom(1024)
            #normalize response with decode and strip
            response = managerResponse.decode().strip()
            
            #OUTCOME
            if response == "SUCCESS":
                print("Peer is registered")
            else:
                print("Peer registration failed")

        elif command == "setup-dht":
            #SETUP-DHT COMMAND

            #INPUT VALIDATION
            #Input: command n YYYY
            if len(tokens) != 3:
                print("USAGE ERROR: setup-dht n YYYY")
                continue
            
            #EXTRACT PARAMETERS
            n = int(tokens[1])
            year = int(tokens[2])

            #PARAMETER VALIDATION
            #n must be greater than or equal to 3 and year must be a valid 4 digit integer
            if n < 3 or year < 1000 or year > 9999:
                print("USAGE ERROR (Parameters): setup-dht n YYYY (n must be greater than or equal to 3)")
                continue

            #build message
            setupMessage = "setup-dht " + peer_name + " " + str(n) + " " + str(year)
            #send message to manager
            peer2Manager_socket.sendto(setupMessage.encode(), (MANAGER_IP, MANAGER_PORT))
            #get response
            managerResponse, manager_address = peer2Manager_socket.recvfrom(1024)
            #normalize response with decode and strip
            response = managerResponse.decode().strip()
            #Tokenize message for parsing using split
            response_tokens = response.split()

            #Check for failure
            if response_tokens[0] == "FAILURE":
                print("SETUP-DHT FAILED")
                continue

            #Remove SUCCESS from tuples
            response_tokens = response_tokens[1:]
            #rebuld tuples string for set-id message
            peer_tuples = " ".join(response_tokens)

            #SET LEADER VARIABLES
            leader_identifier = 0
            DHT_RING_SIZE = n
            nodeStorageAmounts[0] = 0

            #loop to parse response
            index = 0 #tracks which tuple is being parsed (start after leader)
            while index < len(response_tokens):
                #extract tuple information
                current_name = response_tokens[index]
                current_ip = response_tokens[index+1]
                current_pPort = int(response_tokens[index+2])

                #set leader variables first
                if index == 0:
                    leader_neighbor_name = current_name
                    leader_neighbor_IP = current_ip
                    leader_neighbor_port = current_pPort
                    index = index + 3
                    continue

                #set identifier
                identifier = index // 3

                #initialize node storage amounts
                nodeStorageAmounts[identifier] = 0

                #build message
                set_id_message = "set-id " + str(identifier) + " " + str(n) + " " + peer_tuples
                #send peer message
                peer2Peer_socket.sendto(set_id_message.encode(), (current_ip, current_pPort))

                #increment by 3 for next tuple
                index = index + 3

            #PARSING CSV FILE
            #Build filename
            selected_file = "Data/details_" + str(year) + ".csv"
            #get number of storm events
            num_of_events = count_storm_events_csv(selected_file)
            #compute hash table size
            hash_size = find_next_prime(num_of_events)
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
                    stormRecordData = ",".join([curr_event_id, curr_state, curr_year, curr_month_name, curr_event_type, curr_cz_type, curr_cz_name, curr_injuries_direct, curr_injuries_indirect, curr_deaths_direct, curr_deaths_indirect, curr_damage_property, curr_damage_crops, curr_tor_f_scale])
                    storeCommand = "store " + str(curr_pos) + " " + str(curr_id) + " " + stormRecordData

                    #send store command around DHT Ring
                    peer2Peer_socket.sendto(storeCommand.encode(), (leader_neighbor_IP, leader_neighbor_port))

                    #increment node storage amount for identifier
                    nodeStorageAmounts[curr_id] = nodeStorageAmounts[curr_id] + 1

            #AFTER Print DHT status
            print("Records Distributed:")
            for key, value in nodeStorageAmounts.items():
                print(f"Node: {key}, Records Stored: {value}")

            #Send DHT-Complete Message to Manager
            #build message
            completeMessage = "dht-complete " + peer_name 
            #send message to manager
            peer2Manager_socket.sendto(completeMessage.encode(), (MANAGER_IP, MANAGER_PORT))
            #get response
            managerResponse_complete, manager_address = peer2Manager_socket.recvfrom(1024)
            #normalize response with decode and strip
            complete_response = managerResponse_complete.decode().strip()
            #Tokenize message for parsing using split
            complete_response_tokens = complete_response.split()

            #Check for failure
            if complete_response_tokens[0] == "FAILURE":
                print("SETUP-DHT FAILED")
            else:
                print("DHT-COMPLETED")

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

            #INPUT VALIDATION
            if len(tokens) != 5:
                print("USAGE ERROR: peer-setup ⟨peer-name⟩ ⟨IPv4-address⟩ ⟨m-port⟩ ⟨p-port⟩")
                continue

            #Duplicate Setup protection
            if PEER_SETUP:
                print("ERROR Peer already setup")
                continue
            
            #EXTRACT PEER VARIABLES
            peer_name = tokens[1]
            peer_ip = tokens[2]
            m_port = int(tokens[3])
            p_port = int(tokens[4])

            #Set up UDP Sockets
            peer2Manager_socket.bind((peer_ip, m_port))
            peer2Peer_socket.bind((peer_ip, p_port))

            #Start peer listening thread for peer to peer commands
            peer_thread = threading.Thread(target=peer2peer_Listener, args=(peer2Peer_socket,))
            peer_thread.start()

            #UPDATE SETUP FLAG
            PEER_SETUP = True

            print("Peer is setup")

        else:
            #UNKNOWN COMMAND
            print("COMMAND NOT SUPPORTED")
       

#Call peer function
if __name__ == "__main__":
    Peer()
    
