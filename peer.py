import socket
import sys

#PEER
#   Main Peer Function that...


#Stdin interface for Peer
if __name__ == "__main__":
    
    #STDIN INTERFACE
    for line in sys.stdin:
        #Tokenize the command with strip and split
        tokens = line.strip().split()
        if not tokens:
            continue #Skips empty lines with no commands
        
        #Extract Command
        command = tokens[0]
        
        #COMMAND DECISION TREE
        if command == "register":
            #REGISTER COMMAND
        elif command == "setup-dht":
            #SETUP-DHT COMMAND
        elif command == "dht-complete":
            #DHT-COMPLETE COMMAND
        elif command == "query-dht":
            #QUERY-DHT COMMAND
        elif command == "leave-dht":
            #LEAVE-DHT COMMAND
        elif command == "join-dht":
            #JOIN-DHT COMMAND
        elif command == "dht-rebuilt":
            #DHT-REBUILT
        elif command == "deregister":
            #DEREGISTER COMMAND
        elif command == "teardown-dht":
            #TEARDOWN-DHT
        elif command == "teardown-complete":
            #TEARDOWN-COMPLETE
        else:
            #UNKNOWN COMMAND
       
