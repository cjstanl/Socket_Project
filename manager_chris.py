import socket
import sys
import random
import argparse
import json

from enum import Enum
from dataclasses import dataclass


# **********************************************
# *				   GLOBAL VARS			   	   *
# **********************************************
# Create global list for storing peer info for easy access across program
peer_info_list = []

# global sets (for fast lookup) for already used peer names, p_ports, and m_ports
taken_names = set()
taken_p_ports = set()
taken_m_ports = set()

# global var for tracking dht state, default to false
dht_is_setup = False

# global dht members
# Format: (peer_name, peer_ip, peer_p_port)
dht_members = []


def main():
	# Define manager information
	ip_address = "127.0.0.1" # use local host for testing
	port_num = manager_cli() # obtain port num from cli
	print(f"[INFO] Manager running on \n--> ip_address = {ip_address}\n--> port_num = {port_num}")

	# Create manager UDP socket
	manager_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	manager_socket.bind((ip_address, port_num))

	# Enter loop for listening for peers
	while True:
		manager_loop(manager_socket)




# **********************************************
# *				MANAGER LOOP FUNCS			   *
# **********************************************

'''
Manager Main Loop
PURPOSE: Receive a message, dispatch to handler, send response
PARAMS: (socket) UDP socket for manager
RETURNS: None
'''
def manager_loop(manager_socket):
	try:
		# Receive a payload from Peer
		payload, peer_address = manager_socket.recvfrom(1024)
		message = payload.decode()

		# read the json message
		command, body = read_json_message(message)

		# Ensure command is not empty
		if not command:
			print("[ERROR] Command empty")
			send_json_message(manager_socket, peer_address, construct_json_message("FAILURE", None))
			return

		# Reconstruct previous expected string tokens structure (TODO Fix if time)
		tokens = [command]
		if body:
			tokens.extend(body.strip().split())

	except Exception as e:
		print(f"[ERROR] Error occurred when receiving/decoding data from peer: {e}")


	# TODO CHANGE TO ELIF STATEMENTS FOR DHT COMPLETE BLOCKING
	# Create function mapping for dynamic function calls
	# Tutorial on function mapping - https://medium.com/@prasanthrao/dynamically-call-functions-based-on-user-choice-in-python-30c2addbd012
	command_functions_map = {
		"register": register_peer,
		"setup_dht": setup_dht,
		"dht_complete": dht_complete,
		"query_dht": query_dht
	}
	# Parse Message
	command_index = 0
	command = tokens[command_index]

	# Map command to function
	command_function = command_functions_map.get(command, invalid_command) # invalid command is called if no match exists for command
	command_function(tokens, manager_socket, peer_address) # call matching function

# Query DHT Method Description
# -----------------------------------------------------
# Received message format:
# Header: query_dht
# Body: <peer_name>
# -----------------------------------------------------
# Return message format:
# Header: SUCCESS or FAILURE
# Body: (<peer_name>, <peer_ip>, <peer_p_port>) (randomly selected peer to search first)
def query_dht(tokens, manager_socket, peer_address):
	global dht_members

	# Check for invalid query_dht command possibilities
	# DHT not setup
	print(f"[INFO] Received query_dht message: {tokens}")
	if not dht_is_setup:
		print("[ERROR] DHT not setup")
		send_json_message(manager_socket, peer_address, construct_json_message("FAILURE", None))
		return

	# Invalid command length
	if len(tokens) != 2:
		print("[ERROR] Invalid query_dht command received")
		send_json_message(manager_socket, peer_address, construct_json_message("FAILURE", None))
		return
	
	# Peer not registered
	peer_name = tokens[1]
	if peer_name not in taken_names:
		print("[ERROR] Peer name not found")
		send_json_message(manager_socket, peer_address, construct_json_message("FAILURE", None))
		return
	
	# Peer is in DHT table
	for peer in peer_info_list:
		if peer.name == peer_name:
			# Check peer state
			if not peer.state == PeerInfo.State.FREE:
				print("[ERROR] Peer in DHT table")
				send_json_message(manager_socket, peer_address, construct_json_message("FAILURE", None))
				return
			else:
				break

	# Valid query_dht command, so send success with name of peer to search first
	print(f"[INFO] Valid query_dht command, sending success with name of peer to search first")
	
	# Get random peer from dht_members
	random_index = random.randint(0, len(dht_members) - 1)
	peer_name_index = 0
	peer_ip_index = 1
	peer_p_port_index = 2

	peer_to_search_tuple = (dht_members[random_index][peer_name_index], dht_members[random_index][peer_ip_index], dht_members[random_index][peer_p_port_index])
	body = {
		"start_peer": peer_to_search_tuple,
		"dht_members": dht_members
	}
	send_json_message(manager_socket, peer_address, construct_json_message("SUCCESS", body))
	return


# DHT Complete Method Description
# -----------------------------------------------------
# Received message format:
# Header: dht_complete
# Body: <peer_name>
# -----------------------------------------------------
# Return message format:
# Header: SUCCESS or FAILURE
# Body: None
def dht_complete(tokens, manager_socket, peer_address):
	global dht_members
	global dht_is_setup

	print(f"[INFO] Received dht_complete message: {tokens}")
	
	# get leader information for verification
	leader_index = 0
	name_index = 0

	leader_name = dht_members[leader_index][name_index]

	if leader_name != tokens[1]:
		print("[ERROR] Leader name does not match")
		send_json_message(manager_socket, peer_address, construct_json_message("FAILURE", None))
		return
	else:
		print("[INFO] Leader name matches")
		send_json_message(manager_socket, peer_address, construct_json_message("SUCCESS", None))
		dht_is_setup = True
		return
	

# Register Peer Method Description
# -----------------------------------------------------
# Received message format:
# Header: register
# Body: <peer_name> <peer_ip> <peer_m_port> <peer_p_port>
# -----------------------------------------------------
# Return message format:
# Header: SUCCESS or FAILURE
# Body: None
def register_peer(tokens, manager_socket, peer_address):
	# Initialize indexes for message content
	peer_name_index = 1
	peer_ip_index = 2
	peer_m_port_index = 3
	peer_p_port_index = 4

	# Check that message is long enough
	if len(tokens) != 5:
		print("[ERROR] Register message length is incorrect")
		send_json_message(manager_socket, peer_address, construct_json_message("FAILURE", None))
		return

	# Parse message
	# Example of defining a peer object: (State of FREE is assigned if none given)
	# peer_example = PeerInfo("A", "123.456.789", 6514, 6515, PeerInfo.State.LEADER)
	peer_name = tokens[peer_name_index]
	peer_ip = tokens[peer_ip_index]
	peer_m_port = int(tokens[peer_m_port_index])
	peer_p_port = int(tokens[peer_p_port_index])


	# Validate params
	is_valid = validate_register_params(peer_name, peer_m_port, peer_p_port)

	if not is_valid:
		print("[ERROR] Invalid register information")
		send_json_message(manager_socket, peer_address, construct_json_message("FAILURE", None))
		return

	# Create peer info object
	# Default state is free
	peer = PeerInfo(peer_name, peer_ip, peer_m_port, peer_p_port, PeerInfo.State.FREE)

	# Update lists/sets with register information
	peer_info_list.append(peer)
	taken_names.add(peer_name)
	taken_m_ports.add(peer_m_port)
	taken_p_ports.add(peer_p_port)

	print(f"[INFO] Registered peer:\n{peer}")
	# Send JSON message back to client
	send_json_message(manager_socket, peer_address, construct_json_message("SUCCESS", None))

# Check that information for peer is available
def validate_register_params(peer_name, m_port, p_port):
	if (not peer_name.isalpha() or len(peer_name) > 15):
		return False
	elif peer_name in taken_names:
		return False
	elif (m_port in taken_m_ports) or (p_port in taken_p_ports):
		return False
	else:
		return True


# Setup DHT Method Description
# -----------------------------------------------------
# Received message format:
# Header: setup_dht
# Body: <peer_name> <dht_size> <year>
# -----------------------------------------------------
# Return message format:
# Header: SUCCESS or FAILURE
# Body: None
def setup_dht(tokens, manager_socket, peer_address):
	global dht_members

	if len(tokens) != 4:
		print("[ERROR] Invalid setup_dht command received")
		send_json_message(manager_socket, peer_address, construct_json_message("FAILURE", None))
		return

	peer_name_index = 1
	size_index = 2
	year_index = 3

	peer_name = tokens[peer_name_index]
	dht_size = int(tokens[size_index])
	year = int(tokens[year_index])

	is_valid = validate_setup_dht_params(peer_name, dht_size)
	if not is_valid:
		print("[ERROR] Invalid dht parameters received")
		send_json_message(manager_socket, peer_address, construct_json_message("FAILURE", None))
		return

	dht_members = setup_dht_handler(peer_name, dht_size)

	leader_index = 0
	print(f"[INFO] Leader information: {dht_members[leader_index]}")
	print(f"[INFO] All DHT members:\n{dht_members}")

	# Construct a json message for easier managing of list
	header = "SUCCESS"
	body = dht_members
	
	json_message = construct_json_message(header, body)
	send_json_message(manager_socket, peer_address, json_message)

def send_json_message(manager_socket, address, json_message):
	payload = json.dumps(json_message).encode()
	manager_socket.sendto(payload, address)

def construct_json_message(p_header, p_body):
	# Message format:
	# header = success or failure message
	# body = list of members if successful, None if not successful
	json_message = {
		"header": p_header,
		"body": p_body
	}

	return json_message


def read_json_message(message):
	json_message = json.loads(message)
	header, body = json_message["header"], json_message["body"]
	return header, body

# Purpose: Handles adding members to the dht table
# Params:
# -> peer_name (str): peer name for leader
# -> dht_size (int): number of peers participating in dht
# Returns: (list of tuple) list contains (peer name, peer ip, peer2peer port)
# Not most efficient but good enough for now
def setup_dht_handler(peer_name, dht_size):
	# Create list of peers for DHT table
	dht_members = []

	# Find peer name in list to make leader
	# Leader needs to be first index in list
	for peer in peer_info_list:
		if peer_name != peer.name:
			continue
		else:
			peer.state = PeerInfo.State.LEADER
			dht_members.append((peer.name, peer.ipv4, peer.p_port))
			break

	# Fill dht table with other peers
	# Break loop when table size is met
	for peer in peer_info_list:
		# Avoid adding leader peer to table twice
		if peer_name == peer.name:
			continue
		else:
			peer.state = PeerInfo.State.IN_DHT
			dht_members.append((peer.name, peer.ipv4, peer.p_port))
			# Avoid adding more members than table size
			if len(dht_members) == dht_size:
				break

	return dht_members

def validate_setup_dht_params(peer_name, dht_size):
	# Use if statement for each case for readability
	if peer_name not in taken_names:
		return False

	elif dht_size < 3:
		return False

	elif len(peer_info_list) < dht_size:
		return False

	elif dht_is_setup:
		return False

	else:
		return True

def invalid_command(tokens, manager_socket, peer_address):
	print("[ERROR] Invalid command received, no mapping available")
	send_json_message(manager_socket, peer_address, construct_json_message("FAILURE", None))
	return



# **********************************************
# *				Peer Info Class				   *
# **********************************************

""" Class for tracking peer information and state """
''' Tutorial for dataclasses - https://realpython.com/python-data-classes/ '''
@dataclass
class PeerInfo:
	''' Class States '''
	class State(Enum):	
		LEADER = "leader"
		FREE = "free"
		IN_DHT = "in_dht"

	name : str
	ipv4 : str
	m_port : str
	p_port : str
	state : State = State.FREE





# **********************************************
# *				UTILITY FUNCS				   *
# **********************************************

# Manager Command Line Interface
# PURPOSE: Command line interface for handling manager
# PARAMS: None
# RETURNS: (int) port number for manager process from stdin
def manager_cli():
	# instantiate parser
	parser = argparse.ArgumentParser(
		description = "Manager program for managing peer dht",
		formatter_class=argparse.RawDescriptionHelpFormatter,
		epilog='''
		Example for launching manager.py:
			python manager.py -p PORT_NUMBER
		'''
		)
	# only one argument for manager port number
	parser.add_argument("-p", "--port", required=True, help="Port number for manager process")

	# retrieve arguments
	args = parser.parse_args()

	# return port number
	return int(args.port)


if __name__ == "__main__":
	main()