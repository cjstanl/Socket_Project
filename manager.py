import socket
import sys

#INITIAL MANAGER SOCKET VARIABLES
HOST = '127.0.0.1'
SERVER_PORT = 6501

#while True:
 #   data, client_address = server_socket.recvfrom(1024)
  #  print(f"Received from {client_address}: {data.decode()}")
   # server_socket.sendto(b'Hello from UDP server!', client_address)

#DATA STRUCTURES


#Main Manager Program
def Manager():
    #Input validation for port number
    if len(sys.argv) != 2:
        print("USAGE ERROR: python3 manager.py <listening port #>")
        #Exit with error code
        sys.exit(1)
    
    #Get server port from command line
    SERVER_PORT = int(sys.argv[1])

    #set up UDP socket and print status
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_socket.bind((HOST, SERVER_PORT))
    print(f"UDP server is listening on {HOST}:{SERVER_PORT}")

    #Infinite Listening Loop
    while True:
        #READ MESSAGE
        #Collect socket message data
        data, peer_addrress = server_socket.recvfrom(1024)
        #normalize message from peer
        message = data.decode().strip
        #split message into sections to parse
        message_sections = message.split(' ')
        #extract command from message
        command = message_sections[0]

        #COMMAND DECISION TREE
        if command == "register":
            #Validate input length
            if len(message_sections) != 5:
                print("USAGE ERROR: register ⟨peer-name⟩ ⟨IPv4-address⟩ ⟨m-port⟩ ⟨p-port⟩")
                break
            #Extract register arguments
            peer_name = message_sections[1]
            ipv4_address = message_sections[2]
            m_port = int(message_sections[3])
            p_port = int(message_sections[4])

            print("[DEBUG] Command: register, command not yet supported")

        elif command == "setup-dht":
            #validate input length
            if len(message_sections) != 4:
                print("USAGE ERROR: setup-dht ⟨peer-name⟩ ⟨n⟩ ⟨YYYY⟩, where n ≥ 3")
                break
            #Extract setup-dht arguments
            peer_name = message_sections[1]
            n = int(message_sections[2])
            year = int(message_sections[3])

            print("[DEBUG] Command: setup-dht, command not yet supported")

        elif command == "dht-complete":
            #Validate input length
            if len(message_sections) != 2:
                print("USAGE ERROR: dht-complete ⟨peer-name⟩")
                break
            #Extract dht-complete arguments
            peer_name = message_sections[1]

            print("[DEBUG] Command: dht-complete, command not yet supported")

        elif command == "query-dht":
            #Validate input length
            if len(message_sections) != 2:
                print("USAGE ERROR: query-dht ⟨peer-name⟩")
                break
            #Extract query-dht arguments
            peer_name = message_sections[1]

            print("[DEBUG] Command: query-dht, command not yet supported")

        elif command == "leave-dht":
            #Validate input length
            if len(message_sections) != 2:
                print("USAGE ERROR: leave-dht ⟨peer-name⟩")
                break
            #Extract leave-dht arguments
            peer_name = message_sections[1]

            print("[DEBUG] Command: leave-dht, command not yet supported")

        elif command == "join-dht":
            #Validate input length
            if len(message_sections) != 2:
                print("USAGE ERROR: join-dht ⟨peer-name⟩")
                break
            #Extract join-dht arguments
            peer_name = message_sections[1]

            print("[DEBUG] Command: join-dht, command not yet supported")

        elif command == "dht-rebuilt":
            #Validate input length
            if len(message_sections) != 3:
                print("USAGE ERROR: dht-rebuilt ⟨peer-name⟩ ⟨new-leader⟩")
                break
            #extract dht-rebuilt arguments
            peer_name = message_sections[1]
            new_leader = message_sections[2]

            print("[DEBUG] Command: dht-rebuilt, command not yet supported")

        elif command == "deregister":
            #Validate input length
            if len(message_sections) != 2:
                print("USAGE ERROR: deregister ⟨peer-name⟩")
                break
            #Extract deregister arguments
            peer_name = message_sections[1]

            print("[DEBUG] Command: deregister, command not yet supported")

        elif command == "teardown-dht":
            #Validate input length
            if len(message_sections) != 2:
                print("USAGE ERROR: teardown-dht ⟨peer-name⟩")
                break
            #Extract teardown-dht arguments
            peer_name = message_sections[1]

            print("[DEBUG] Command: teardown-dht, command not yet supported")

        elif command == "teardown-complete":
            #Validate input length
            if len(message_sections) != 2:
                print("USAGE ERROR:  teardown-complete ⟨peer-name⟩")
                break
            #Extract teardown-complete arguments
            peer_name = message_sections[1]

            print("[DEBUG] Command: teardown-complete, command not yet supported")

        else:
            print("[ERROR] Unknown Command")