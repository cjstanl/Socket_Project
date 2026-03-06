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


#Call Manager function on startup
if __name__ == "__main__":
    Manager()
