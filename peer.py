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

#Thread to listen for peer to peer messages
#   - Function takes the peer socket and listens infintely for peer messages
def peer2peer_Listener(peer2peer_socket):
    #Peer to Peer Variables
    identifier = 0
    ring_size = 0

    #Infinite loop listening for messages
    while True:
        #READ MESSAGE
        payload, peer_address = peer2peer_socket.recvfrom(1024) #get message from socket
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
            neighbor_pPort = tuples[neighbor_index+2]

            print(neighbor_name)
            print(neighbor_ip)
            print(neighbor_pPort)




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
    
    #Command line Input Validation
    if len(sys.argv) != 3:
        print("USAGE ERROR: peer.py <MANAGER_IP> <MANAGER_PORT>")
        sys.exit(1)

    #EXTRACT COMMAND LINE ARGUMENTS
    MANAGER_IP = sys.argv[1]
    MANAGER_PORT = sys.argv[2]

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

            #loop to parse response
            index = 3 #tracks which tuple is being parsed (start after leader)
            while index < len(response_tokens):
                #extract tuple information
                current_name = response_tokens[index]
                current_ip = response_tokens[index+1]
                current_pPort = int(response_tokens[index+2])

                #set identifier
                identifier = index // 3

                #build message
                set_id_message = "set-id " + str(identifier) + " " + str(n) + " " + peer_tuples
                #send peer message
                peer2Peer_socket.sendto(set_id_message.encode(), (current_ip, current_pPort))

                #increment by 3 for next tuple
                index = index + 3

            #PARSING CSV FILE
            #Build filename
            selected_file = "details_" + str(year) + ".csv"
            #get number of storm events
            num_of_events = count_storm_events_csv(selected_file)
            #Loop over all storm events
            


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
    
