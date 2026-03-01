import socket
import sys

#INITIAL MANAGER SOCKET VARIABLES
HOST = '127.0.0.1'

#DATA STRUCTURES
class Peer:
    def __init__(self, name, ip, m_port, p_port, state):
        self.name = name
        self.ip = ip
        self.m_port = m_port
        self.p_port = p_port
        self.state = state

class PeerList:
    def __init__(self):
        self.peers = {}
        self.taken_m_ports = set()
        self.taken_p_ports = set()

#REGISTER
def register(peer_list: PeerList, peer_name: str, ipv4_address: str, m_port: int, p_port: int) -> bool:
    #Validate registration
    if peer_name in peer_list.peers or len(peer_name) > 15 or (not peer_name.isalpha()):
        return False
    if m_port in peer_list.taken_m_ports:
        return False
    if p_port in peer_list.taken_p_ports:
        return False
    
    #if valid arguments create peer and add to list
    peer = Peer(peer_name, ipv4_address, m_port, p_port, "Free")
    peer_list.peers[peer_name] = peer
    peer_list.taken_m_ports.add(m_port)
    peer_list.taken_p_ports.add(p_port)
    return True

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

    #initialize peer list
    peer_list = PeerList()

    #Infinite Listening Loop
    while True:
        #READ MESSAGE
        #Collect socket message data
        data, peer_address = server_socket.recvfrom(1024)
        #normalize message from peer
        message = data.decode().strip()
        #split message into sections to parse
        message_sections = message.split()
        #extract command from message
        command = message_sections[0]

        #COMMAND DECISION TREE
        if command == "register":
            #Validate input length
            if len(message_sections) != 5:
                server_socket.sendto(b'FAILURE', peer_address)
                continue
            #Extract register arguments
            peer_name = message_sections[1]
            ipv4_address = message_sections[2]
            m_port = int(message_sections[3])
            p_port = int(message_sections[4])
            
            #call register functions
            if register(peer_list=peer_list, peer_name=peer_name, ipv4_address=ipv4_address, m_port=m_port, p_port=p_port):
                server_socket.sendto(b'SUCCESS', peer_address)
                continue
            else:
                server_socket.sendto(b'FAILURE', peer_address)
                continue

        elif command == "setup-dht":
            #validate input length
            if len(message_sections) != 4:
                server_socket.sendto(b'FAILURE', peer_address)
                continue
            #Extract setup-dht arguments
            peer_name = message_sections[1]
            n = int(message_sections[2])
            year = int(message_sections[3])

            print("[DEBUG] Command: setup-dht, command not yet supported")

        elif command == "dht-complete":
            #Validate input length
            if len(message_sections) != 2:
                server_socket.sendto(b'FAILURE', peer_address)
                continue
            #Extract dht-complete arguments
            peer_name = message_sections[1]

            print("[DEBUG] Command: dht-complete, command not yet supported")

        elif command == "query-dht":
            #Validate input length
            if len(message_sections) != 2:
                server_socket.sendto(b'FAILURE', peer_address)
                continue
            #Extract query-dht arguments
            peer_name = message_sections[1]

            print("[DEBUG] Command: query-dht, command not yet supported")

        elif command == "leave-dht":
            #Validate input length
            if len(message_sections) != 2:
                server_socket.sendto(b'FAILURE', peer_address)
                continue
            #Extract leave-dht arguments
            peer_name = message_sections[1]

            print("[DEBUG] Command: leave-dht, command not yet supported")

        elif command == "join-dht":
            #Validate input length
            if len(message_sections) != 2:
                server_socket.sendto(b'FAILURE', peer_address)
                continue
            #Extract join-dht arguments
            peer_name = message_sections[1]

            print("[DEBUG] Command: join-dht, command not yet supported")

        elif command == "dht-rebuilt":
            #Validate input length
            if len(message_sections) != 3:
                server_socket.sendto(b'FAILURE', peer_address)
                continue
            #extract dht-rebuilt arguments
            peer_name = message_sections[1]
            new_leader = message_sections[2]

            print("[DEBUG] Command: dht-rebuilt, command not yet supported")

        elif command == "deregister":
            #Validate input length
            if len(message_sections) != 2:
                server_socket.sendto(b'FAILURE', peer_address)
                continue
            #Extract deregister arguments
            peer_name = message_sections[1]

            print("[DEBUG] Command: deregister, command not yet supported")

        elif command == "teardown-dht":
            #Validate input length
            if len(message_sections) != 2:
                server_socket.sendto(b'FAILURE', peer_address)
                continue
            #Extract teardown-dht arguments
            peer_name = message_sections[1]

            print("[DEBUG] Command: teardown-dht, command not yet supported")

        elif command == "teardown-complete":
            #Validate input length
            if len(message_sections) != 2:
                server_socket.sendto(b'FAILURE', peer_address)
                continue
            #Extract teardown-complete arguments
            peer_name = message_sections[1]

            print("[DEBUG] Command: teardown-complete, command not yet supported")

        else:
            print("[ERROR] Unknown Command")

#Call Manager function on startup
if __name__ == "__main__":
    Manager()
