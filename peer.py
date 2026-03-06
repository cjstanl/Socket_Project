import socket
import sys

#MANAGER IP ADDRESS
#for local testing:
MANAGER_IP = '127.0.0.1'
#for lab testing (PC-A defualt IP setup)
#MANAGER_IP = '10.0.1.11'

#PEER
#   Main Peer Function
def Peer():
    
    #Variables for PEER
    PEER_SETUP = False
    peer_name = ""
    peer_ip = ""
    m_port = 0
    p_port = 0
    MANAGER_PORT = 0
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

            

            print("COMMAND NOT SUPPORTED")
        elif command == "setup-dht":
            #SETUP-DHT COMMAND
            print("COMMAND NOT SUPPORTED")
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

            #UPDATE SETUP FLAG
            PEER_SETUP = True

        else:
            #UNKNOWN COMMAND
            print("COMMAND NOT SUPPORTED")
       

#Call peer function
if __name__ == "__main__":
    Peer()
    
