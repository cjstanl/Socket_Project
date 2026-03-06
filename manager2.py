import socket
import sys
import random

#MANAGER IP ADDRESS
#for local testing:
MANAGER_IP = '127.0.0.1'
#for lab testing (PC-A defualt IP setup)
#MANAGER_IP = '10.0.1.11'

#CUSTOM DATA STRUCTURES
#Peer
#   Structure to hold peers data for ease of access, a peer has the following attributes:
#       - name: <15 character alphabetic string
#       - ip: IPv4 Address associated with the peer
#       - m_port: port for manager to peer communication
#       - p_port: port for peer to peer communication
#       - state: state of the peer (Free, Leader, InDHT)
class Peer:
    #Constructor
    def __init__(self, name, ip, m_port, p_port, state):
        self.name = name
        self.ip = ip
        self.m_port = m_port
        self.p_port = p_port
        self.state = state


#Utility Functions


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
    manager_socket.bind((MANAGER_IP, MANAGER_PORT))

    #INITIAL VARIABLES
    peer_list = {} #Peer dictionary
    m_ports = set() #track used m_ports
    p_ports = set() #track used p_ports
    DHT_EXISTS = False #boolean to track if DHT exists
    DHT_SETUP_IN_PROGRESS = False #boolean to track if DHT is being setup

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
            m_port = int(tokens[3])
            p_port = int(tokens[4])

            #VALIDATE PARAMETERS
            #Name parameter must be alphabetic, less than 15 characters, and unique
            if (not peer_name.isalpha()) or len(peer_name) > 15:
                manager_socket.sendto(b'FAILURE', peer_address)
                continue
            elif peer_name in peer_list:
                manager_socket.sendto(b'FAILURE', peer_address)
                continue
            #Port numbers need to be unique
            if (m_port in m_ports) or (p_port in p_ports):
                manager_socket.sendto(b'FAILURE', peer_address)
                continue

            #REGISTER PEER
            #create new peer object
            peer_list[peer_name] = Peer(peer_name, ip_address, m_port, p_port, "Free")
            #add parameters to list for future parameter validation
            m_ports.add(m_port)
            p_ports.add(p_port)
            manager_socket.sendto(b'SUCCESS', peer_address)

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
