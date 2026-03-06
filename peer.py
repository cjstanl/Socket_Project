import socket
import sys
import threading
import csv

#MANAGER IP ADDRESS
#for local testing:
MANAGER_IP = '127.0.0.1'
#for lab testing (PC-A defualt IP setup)
#MANAGER_IP = '10.0.1.11'

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
    MANAGER_PORT = 0 # set during peer setup based on manager port during manager start
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



        elif command == "dht-complete":
            #DHT-COMPLETE COMMAND
            print("COMMAND NOT SUPPORTED")
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
            if len(tokens) != 6:
                print("USAGE ERROR: peer-setup ⟨peer-name⟩ ⟨IPv4-address⟩ ⟨m-port⟩ ⟨p-port⟩ ⟨MANAGER-PORT⟩")
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
            MANAGER_PORT = int(tokens[5])

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
    
