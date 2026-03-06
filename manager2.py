import socket
import sys
import random

#MANAGER IP ADDRESS
#for local testing:
MANAGER_IP = '127.0.0.1'
#for lab testing (PC-A defualt IP setup)
#MANAGER_IP = '10.0.1.11'

#MANGER
#   Main Manager Function that implements the always on manager 
def Manager():
    
    #INPUT VALIDATION: Manager command line port
    if len(sys.argv) != 2:
        print("USAGE ERROR: python3 manager.py <listening port #>")
        #Exit with an error code
        sys.exit(1)
    
    #Collect Port number from command line
    MANAGER_PORT = int(sys.argv[1])

    #Create UDP Socket for manager
    manager_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    manager_socket.bind()

    #Infinite Loop for reading messages
    while True:
        #READ MESSAGES
        payload, peer_address = manager_socket.recvfrom(1024)
        #normalize payload (the peer message)
        message = payload.decode()
        #split message into tokens for parsing
        tokens = message.strip().split()
        #Extract command from message
        command = tokens[0]

        #COMMAND DECISION TREE
        if command == "register":
            #REGISTER COMMAND

            #INPUT VALIDATION
            if len(tokens) != 5:
                manager_socket.sendto(b'FAILURE', peer_address)
                continue

            #EXTRACT PARAMETERS
            peer_name = tokens[1]
            ip_address = tokens[2]
            m_port = tokens[3]
            p_port = tokens[4]

        elif command == "setup-dht":
            #SETUP-DHT COMMAND

            #INPUT VALIDATION
            if len(tokens) != 4:
                manager_socket.sendto(b'FAILURE', peer_address)
                continue

            #EXTRACT PARAMETERS
            peer_name = tokens[1]
            n = tokens[2]
            year = tokens[3]

        elif command == "dht-complete":
            #DHT-COMPLETE COMMAND

            #INPUT VALIDATION
            if len(tokens) != 2:
                manager_socket.sendto(b'FAILURE', peer_address)
                continue

            #EXTRACT PARAMETERS
            peer_name = tokens[1]

        elif command == "query-dht":
            #QUERY-DHT COMMAND

            #INPUT VALIDATION
            if len(tokens) != 2:
                manager_socket.sendto(b'FAILURE', peer_address)
                continue

            #EXTRACT PARAMETERS
            peer_name = tokens[1]

        elif command == "leave-dht":
            #LEAVE-DHT COMMAND

            #INPUT VALIDATION
            if len(tokens) != 2:
                manager_socket.sendto(b'FAILURE', peer_address)
                continue

            #EXTRACT PARAMETERS
            peer_name = tokens[1]
            
        elif command == "join-dht":
            #JOIN-DHT COMMAND

            #INPUT VALIDATION
            if len(tokens) != 2:
                manager_socket.sendto(b'FAILURE', peer_address)
                continue

            #EXTRACT PARAMETERS
            peer_name = tokens[1]
            
        elif command == "dht-rebuilt":
            #DHT-REBUILT

            #INPUT VALIDATION
            if len(tokens) != 3:
                manager_socket.sendto(b'FAILURE', peer_address)
                continue

            #EXTRACT PARAMETERS
            peer_name = tokens[1]
            new_leader = tokens[2]

        elif command == "deregister":
            #DEREGISTER COMMAND

            #INPUT VALIDATION
            if len(tokens) != 2:
                manager_socket.sendto(b'FAILURE', peer_address)
                continue

            #EXTRACT PARAMETERS
            peer_name = tokens[1]
            
        elif command == "teardown-dht":
            #TEARDOWN-DHT

            #INPUT VALIDATION
            if len(tokens) != 2:
                manager_socket.sendto(b'FAILURE', peer_address)
                continue

            #EXTRACT PARAMETERS
            peer_name = tokens[1]
            
        elif command == "teardown-complete":
            #TEARDOWN-COMPLETE

            #INPUT VALIDATION
            if len(tokens) != 2:
                manager_socket.sendto(b'FAILURE', peer_address)
                continue

            #EXTRACT PARAMETERS
            peer_name = tokens[1]
            
        else:
            #UNKNOWN COMMAND
            manager_socket.sendto(b'FAILURE', peer_address)
            continue

#Call Manager function on startup
if __name__ == "__main__":
    Manager()
