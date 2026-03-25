import socket
import argparse
import json
import threading
import csv
import random

# Global for easier access
manager_port = 6500
manager_ip = '127.0.0.1'
manager_address = (manager_ip, manager_port)

peer_ip = '127.0.0.1'

peer_to_peer_socket = None
peer_to_manager_socket = None

# Global for peer ID
self_peer_id = None
# Global for peer name
self_peer_name = None
# global for ring size
ring_size = None
# Global for peer list
peer_list = []
# Global for overall hash table size for pos calculation in query
hash_size = None

# Global for neighbor index
neighbor_index = None

# list for storing local dht data
local_data = {}

# **********************************************
# *				MAIN FUNC			           *
# **********************************************

def main():
	global peer_to_peer_socket, peer_to_manager_socket, self_peer_name
	ip_address = "127.0.0.1" # use local host for testing
	args = peer_cli() # obtain port num from cli

	ports_list = args.port
	peer_name = args.name
	self_peer_name = peer_name

	m_port_index = 0
	p_port_index = 1

	m_port = int(ports_list[m_port_index])
	p_port = int(ports_list[p_port_index])

	print(f"[INFO] Peer {peer_name} running on ip_address = {ip_address} port_nums = {ports_list}")

	# Create peer to peer socket
	peer_to_peer_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	peer_to_peer_socket.bind((peer_ip, p_port))

	# Create peer to manager socket
	peer_to_manager_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	peer_to_manager_socket.bind((peer_ip, m_port))

	# Create and start threads for peer to peer and standard input
	# Tutorial for threading with Python: https://realpython.com/intro-to-python-threading/
	print("[INFO] Launching standard input listener thread")
	std_input_thread = threading.Thread(target=std_input_listener, args=(peer_name, m_port, p_port))
	std_input_thread.start()

	print("[INFO] Launching peer to peer listener thread")
	peer_to_peer_thread = threading.Thread(target=peer_to_peer_listener)
	peer_to_peer_thread.start()

# **********************************************
# *			   THREADING FUNCS			       *
# **********************************************

def std_input_listener(peer_name, m_port, p_port):
	while True:
		user_input = input()
		peer_stdin_handler(peer_name, m_port, p_port, user_input)

def peer_to_peer_listener():
	while True:
		payload, address = peer_to_peer_socket.recvfrom(4096)
		message = read_json_message(payload)
		peer_to_peer_handler(message, address)

# **********************************************
# *			   SENDING FUNCS			       *
# **********************************************
 
def send_to_manager(address, message):
	payload = json.dumps(message).encode()
	peer_to_manager_socket.sendto(payload, address)


def send_to_peer(address, message):
	payload = json.dumps(message).encode()
	peer_to_peer_socket.sendto(payload, address)

def send_to_neighbor(message):
	if neighbor_index is None:
		print("[ERROR] Neighbor index is not set")
		return
	
	peer = peer_list[neighbor_index]
	peer_ip = peer[1]
	peer_p_port = peer[2]
	address = (peer_ip, peer_p_port)
	
	payload = json.dumps(message).encode()
	peer_to_peer_socket.sendto(payload, address)

# **********************************************
# *			JSON MESSAGE FUNCS		           *
# **********************************************

def construct_json_message(p_header, p_body):
	# Message format:
	# header = success or failure message
	# body = list of members if successful, None if not successful
	json_message = {
		"header": p_header,
		"body": p_body
	}
	return json_message

def read_json_message(byte_message):
	# Message format:
	# header = command, success, or failure message
	# body = content of message. Can be None
	json_message = json.loads(byte_message.decode())
	return json_message

# **********************************************
# *			PEER TO PEER FUNCS			       *
# **********************************************
def peer_to_peer_handler(message, address):
	command = message["header"]
	body = message["body"]
	if command == "set_peer_id":
		print(f"[INFO] Received set_peer_id message")
		set_peer_id(body)
		set_neighbor_index()

	elif command == "store":
		store_data(body)
	
	elif command == "find_event":
		event_id = body['event_id']
		S_name = body['S_name']
		S_ip = body['S_ip']
		S_port = body['S_port']
		id_seq = body['id_seq']
		unvisited_peers = body['unvisited_peers']

		find_event(event_id, S_name, S_ip, S_port, id_seq, unvisited_peers)
	elif command == "set_hash_size":
		hash_size = body['hash_size']
		set_hash_size(hash_size)
	
	elif command == "SUCCESS":
		print(f"\\n[QUERY-RESULT] Received SUCCESS message from {address}")
		if not body is None:
			print(f"[QUERY-RESULT] Target Record Info: {body['record']}")
			print(f"[QUERY-RESULT] Id path sequence (id_seq): {body['id_seq']}\\n")
	
	elif command == "FAILURE":
		print(f"\\n[QUERY-RESULT] Received FAILURE message from {address}")
		if not body is None:
			print(f"[QUERY-RESULT] Body: {body}\\n")
			

def store_data(body):
	event_id = body["record"]["EVENT_ID"]
	# Check that self id matches destination id
	if body["dest_peer_id"] == self_peer_id:
		print(f"[STORE] Peer {self_peer_id} keeping EVENT_ID {event_id} at pos {body['dest_pos']}")
		record = make_record(event_id, body["dest_pos"], body["record"])
		local_data[body["dest_pos"]] = record
	else:
		print(f"[FORWARD] Peer {self_peer_id} passing EVENT_ID {event_id} to neighbor (Dest: Peer {body['dest_peer_id']})")
		message = construct_json_message("store", body)
		send_to_neighbor(message)

def make_record(event_id, pos,record_dict):
		return Record(
			pos, 
			self_peer_id, 
			event_id, 
			record_dict["STATE"], 
			record_dict["YEAR"], 
			record_dict["MONTH_NAME"], 
			record_dict["EVENT_TYPE"],
			record_dict["CZ_TYPE"],
			record_dict["CZ_NAME"],
			record_dict["INJURIES_DIRECT"],
			record_dict["INJURIES_INDIRECT"],
			record_dict["DEATHS_DIRECT"],
			record_dict["DEATHS_INDIRECT"],
			record_dict["DAMAGE_PROPERTY"],
			record_dict["DAMAGE_CROPS"],
			record_dict["TOR_F_SCALE"]
		)

def set_peer_id(body):
	global self_peer_id
	global ring_size
	global peer_list
	global hash_size

	print()
	self_peer_id = body["peer_id"]
	ring_size = body["ring_size"]
	peer_list = body["peer_list"]

	print(f"[INFO] Peer ID set to {self_peer_id} | Ring size: {ring_size}\n[INFO] Peer List: {peer_list}")
	print()

def set_neighbor_index():
	global neighbor_index
	global self_peer_id
	global ring_size
	global peer_list

	neighbor_index = (self_peer_id + 1) % ring_size
	print(f"[INFO] Neighbor index set to {neighbor_index} (Peer {peer_list[neighbor_index][1]}:{peer_list[neighbor_index][2]})")

def find_event(event_id, S_name, S_ip, S_port, id_seq, unvisited_peers):
	global self_peer_name

	# Add self to id_seq
	id_seq.append(self_peer_id)

	# Remove self from unvisited_peers list
	if self_peer_name in unvisited_peers:
		unvisited_peers.remove(self_peer_name)

	# Check if storing event locally by event id
	# Calculate pos from event id and hash size
	event_id = int(event_id)
	pos = event_id % hash_size

	print(f"\\n[QUERY] Peer {self_peer_id} checking local_data for EVENT_ID {event_id} at calculated pos {pos}")

	# Check if position is locally stored, if so then return messag to s_peer
	if pos in local_data:
		print(f"[QUERY] Peer {self_peer_id} has EVENT_ID {event_id} locally!")
		header = "SUCCESS"
		record_as_dict = construct_dict_from_record(local_data[pos])
		body = {
			"id_seq": id_seq,
			"record": record_as_dict
		}
		message = construct_json_message(header, body)
		address = (S_ip, S_port)
		print(f"[QUERY] Sending SUCCESS back to source {S_name} at {address}")
		send_to_peer(address, message)
		return
	
	# Calculate next peer to message using Hot Potato
	# Check if unvisited peer list is empty
	if len(unvisited_peers) == 0:
		print(f"[ERROR] No unvisited peers left to search for EVENT_ID {event_id}")
		header = "FAILURE"
		body = None
		message = construct_json_message(header, body)
		address = (S_ip, S_port)
		print(f"[QUERY] Sending FAILURE back to source {S_name} at {address}")
		send_to_peer(address, message)
		return
	# Randomly select peer from unvisited peer list
	peer_to_search_index = random.randint(0, len(unvisited_peers) - 1)
	peer_to_search_name = unvisited_peers[peer_to_search_index]

	ip_index = 1
	port_index = 2

	peer_to_search_ip = None
	peer_to_search_p_port = None
	for peer in peer_list:
		if peer[0] == peer_to_search_name:
			peer_to_search_ip = peer[ip_index]
			peer_to_search_p_port = peer[port_index]
			break
	
	# Construct message
	header = "find_event"
	body = {
		"event_id": event_id,
		"S_name": S_name,
		"S_ip": S_ip,
		"S_port": S_port,
		"id_seq": id_seq,
		"unvisited_peers": unvisited_peers
	}
	message = construct_json_message(header, body)
	address = (peer_to_search_ip, peer_to_search_p_port)
	print(f"[QUERY] Event {event_id} not found locally. Forwarding find_event message to peer {peer_to_search_name} at {address}")
	send_to_peer(address, message)
	return
	

# **********************************************
# *				LEADER FUNCS				   *
# **********************************************

def print_leader_info():
	global self_peer_id
	global peer_list
	global ring_size
	global neighbor_index

	print()
	print(f"[INFO] LEADER self_peer_id: {self_peer_id}")
	print(f"[INFO] ring_size: {ring_size}")
	print(f"[INFO] neighbor_index: {neighbor_index}")
	print()

def send_set_peer_id(peer_id, ring_size, peer_list):
	body = {
		"peer_id": peer_id,
		"ring_size": ring_size,
		"peer_list": peer_list
	}
	header = "set_peer_id"
	json_message = construct_json_message(header, body)

	peer_ip_index = 1
	peer_p_port_index = 2
	
	peer_ipv4 = peer_list[peer_id][peer_ip_index]
	peer_p_port = peer_list[peer_id][peer_p_port_index]
	address = (peer_ipv4, peer_p_port)

	print(f"[INFO] Sending set_peer_id message to peer {peer_id}")
	send_to_peer(address, json_message)


def send_hash_size(hash_size):
	header = "set_hash_size"
	body = {
		"hash_size": hash_size
	}
	json_message = construct_json_message(header, body)

	for peer in peer_list:
		peer_ip_index = 1
		peer_p_port_index = 2
		
		peer_ipv4 = peer[peer_ip_index]
		peer_p_port = peer[peer_p_port_index]
		address = (peer_ipv4, peer_p_port)

		print(f"[INFO] Sending hash_size message to peer {peer_ip}")
		send_to_peer(address, json_message)

def set_hash_size(p_hash_size):
	global hash_size

	hash_size = p_hash_size
	print(f"[INFO] Received hash size message, hash size set to {hash_size}")

def send_store_data(year):
	global hash_size
	global ring_size
	global peer_list
	global local_data

	header = "store"
	
	# CSV file parsing
	# Build file name
	selected_file = "Data/details_" + str(year) + ".csv"
	
	num_events = count_storm_events_csv(selected_file)
	hash_size = find_next_prime(num_events)

	# Send hash size to all peers
	send_hash_size(hash_size)

	# Open and read CSV file
	with open(selected_file, newline='') as csvfile:
		dict_reader = csv.DictReader(csvfile)

		for row in dict_reader:	
			# Calculate destination for each record
			current_event_id = int(row['EVENT_ID'])
			dest_pos, dest_peer_id = compute_hashes(hash_size, ring_size, current_event_id)
			record = make_record(current_event_id, dest_pos, row)
			
			# print 1 local record for example
			if dest_pos == 1:
				print()
				print(f"[EXAMPLE] Peer {self_peer_id} keeping EVENT_ID {current_event_id} at pos {dest_pos}")
				print(f"Record: {record}")
				print()			

			# Check if self is destination
			if dest_peer_id == self_peer_id:
				# store in local data structure
				print(f"[STORE] Peer {self_peer_id} keeping EVENT_ID {current_event_id} at pos {dest_pos}")
				local_data[dest_pos] = record

			else:
				# construct body of store message
				body = {
					"dest_pos": dest_pos,
					"dest_peer_id": dest_peer_id,
					"record": dict(row)
				}

				print(f"[FORWARD] Peer {self_peer_id} passing EVENT_ID {current_event_id} to neighbor (Dest: Peer {dest_peer_id})")
				# send to next peer in ring
				json_message = construct_json_message(header, body)
				send_to_neighbor(json_message)


def dht_complete(peer_name):
	print(f"[INFO] Sending dht_complete message to manager")
	send_to_manager(manager_address, construct_json_message("dht_complete", peer_name))

	response, address = peer_to_manager_socket.recvfrom(4096)
	json_response = read_json_message(response)
	print(f"[INFO] Manager response: {json_response['header']}")

# **********************************************
# *			PEER TO MANAGER FUNCS			   *
# **********************************************

def start_query_dht(peer_name):
	print(f"[INFO] Sending query_dht message to manager")
	send_to_manager(manager_address, construct_json_message("query_dht", peer_name))

	response, address = peer_to_manager_socket.recvfrom(4096)
	json_response = read_json_message(response)
	print(f"[INFO] Manager response: {json_response['header']}")

	body = json_response['body']
	peer_to_search_tuple = body['start_peer']
	dht_members = body['dht_members']

	print(f"[INFO] Peer to search first: {peer_to_search_tuple}")
	print(f"[INFO] DHT members received: {dht_members}")

	return peer_to_search_tuple, dht_members
	
def peer_stdin_handler(peer_name, m_port, p_port, user_input):
	user_tokens = user_input.strip().split()

	command_index = 0
	command = user_tokens[command_index]

	if command == "register":
		register_with_manager(peer_name, m_port, p_port)

	elif command == "setup_dht":
		if len(user_tokens) != 3:
			print("[ERROR] Proper usage: setup_dht DHT_SIZE YEAR")
			return

		size_index = 1
		year_index = 2

		dht_size = user_tokens[size_index]
		year = user_tokens[year_index]

		setup_dht(peer_name, dht_size, year)
	
	elif command == "query_dht":
		if len(user_tokens) != 1:
			print("[ERROR] Proper usage: query_dht")
			return

		peer_to_search_tuple, dht_members = start_query_dht(peer_name)
		event_id = get_query_event_id()

		peer_name_index = 0
		peer_ip_index = 1
		peer_p_port_index = 2

		peer_to_search_name = peer_to_search_tuple[peer_name_index]
		peer_to_search_ip = peer_to_search_tuple[peer_ip_index]
		peer_to_search_p_port = peer_to_search_tuple[peer_p_port_index]
		
		# Build unvisited peers list from the dht_members returned by manager
		# This ensures non-DHT peers (like D) also have the correct list
		unvisited_peers = []
		for peer in dht_members:
			unvisited_peers.append(peer[peer_name_index])
		
		# List for tracking visited peer ids in order of visit
		id_seq = []
		
		print(f"\\n[QUERY] Starting query for event id {event_id}")
		print(f"[QUERY] Unvisited peers list: {unvisited_peers}")
		
		# Construct message
		header = "find_event"
		body = {
			"event_id": event_id,
			"S_name": peer_name,
			"S_ip": peer_ip,
			"S_port": p_port,
			"id_seq": id_seq,
			"unvisited_peers": unvisited_peers
		}
		
		print(f"[INFO] Sending find_event message for event id {event_id} to peer {peer_to_search_name} at {peer_to_search_ip}:{peer_to_search_p_port}")
		json_message = construct_json_message(header, body)
		send_to_peer((peer_to_search_ip, peer_to_search_p_port), json_message)
	else:
		invalid_command()


def get_query_event_id():
	user_input = input("[INFO] Enter event information: <event_id>")
	user_tokens = user_input.strip().split()

	if len(user_tokens) != 1:
		print("[ERROR] Proper usage: <event_id>")
		return get_query_event_id()

	event_id = user_tokens[0]
	return event_id


def register_with_manager(peer_name, m_port, p_port):
	command = "register"
	separator = " "
	message = peer_name + separator + peer_ip + separator + str(m_port) + separator + str(p_port)

	print()
	print(f"[INFO] Sending message to manager: {command} {message}")
	json_message = construct_json_message(command, message)
	send_to_manager(manager_address, json_message)

	manager_response, address = peer_to_manager_socket.recvfrom(4096)

	json_response = read_json_message(manager_response)
	print(f"[INFO] Manager response: {json_response['header']}")
	print()

def setup_dht(peer_name, dht_size, year):
	global self_peer_id
	global peer_list
	global ring_size
	global neighbor_index

	command = "setup_dht"
	separator = " "
	message = peer_name + separator + dht_size + separator + year

	json_message = construct_json_message(command, message)

	print(f"[INFO] Sending message to manager; {command} {message}")
	send_to_manager(manager_address, json_message)

	response, address = peer_to_manager_socket.recvfrom(4096)
	json_response = read_json_message(response)
	print(f"[INFO] Manager response: {json_response['header']}")
	if json_response.get('body'):
		print(f"[INFO] DHT Members received: {len(json_response['body'])} peers found.")
	
	# Set ID for each peer in peer list
	peer_list = json_response['body']
	leader_index = 0
	ring_size = len(peer_list)
	for index in range (len(peer_list)):
		# set own peer id to index 0 for leader
		if index == leader_index:
			print(f"[INFO] Setting peer ID to {index}")
			self_peer_id = index
			neighbor_index = (self_peer_id + 1) % ring_size
		else:
			peer_id = index
			send_set_peer_id(peer_id, ring_size, peer_list)
	
	# Check leader info
	print_leader_info()
	send_store_data(year)
	dht_complete(peer_name)

def invalid_command():
	print("[ERROR] Invalid command received, no mapping available")




# **********************************************
# *				UTILITY FUNCS				   *
# **********************************************

# Peer Command Line Interface
# PURPOSE: Command line interface for handling peer
# PARAMS: None
# RETURNS: args containing the following:
# - (list) port numbers for peer to manager and peer to peer interaction
# - (str) name for peer
def peer_cli():
	# instantiate parser
	parser = argparse.ArgumentParser(
		description = "Peer program for dht processing",
		formatter_class=argparse.RawDescriptionHelpFormatter,
		epilog='''
		Example for launching manager.py:
			python peer.py -p M_PORT_NUMBER P_PORT_NUMBER -n PEER_NAME
		'''
		)
	# argument for peer port numbers
	parser.add_argument("-p", "--port", nargs=2, required=True, help="Port numbers for peer to manager port and peer to peer port")
	parser.add_argument("-n", "--name", required=True, help="Peer name")

	# retrieve arguments
	args = parser.parse_args()

	# return args
	return args

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
    elif num % 2 == 0:
        return False
    
    sqr_root_value = int(num ** 0.5)
    for i in range (3, sqr_root_value + 1, 2):
        if (num % i == 0):
            return False

    return True
def construct_dict_from_record(record):
	return {
		"pos": record.pos,
		"owner_id": record.id,
		"event_id": record.event_id,
		"state": record.state,
		"year": record.year,
		"month_name": record.month_name,
		"event_type": record.event_type,
		"cz_type": record.cz_type,
		"cz_name": record.cz_name,
		"injuries_direct": record.injuries_direct,
		"injuries_indirect": record.injuries_indirect,
		"deaths_direct": record.deaths_direct,
		"deaths_indirect": record.deaths_indirect,
		"damage_property": record.damage_property,
		"damage_crops": record.damage_crops,
		"tor_f_scale": record.tor_f_scale
	}

#CUSTOM DATA STRUCTURES
#Record stores the 14 fields required as an object for easy parsing and storage as well as the pos and id
class Record:
    def __init__(self, pos, owner_id, event_id, state, year, month_name, event_type, cz_type, cz_name, injuries_direct, injuries_indirect, deaths_direct, deaths_indirect, damage_property, damage_crops, tor_f_scale):
        self.pos = pos
        self.id = owner_id
        self.event_id = event_id
        self.state = state
        self.year = year
        self.month_name = month_name
        self.event_type = event_type
        self.cz_type = cz_type
        self.cz_name = cz_name
        self.injuries_direct = injuries_direct
        self.injuries_indirect = injuries_indirect
        self.deaths_direct = deaths_direct
        self.deaths_indirect = deaths_indirect
        self.damage_property = damage_property
        self.damage_crops = damage_crops
        self.tor_f_scale = tor_f_scale


if __name__ == "__main__":
	main()