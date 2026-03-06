import socket
import csv
import os

# Define server and client connection details
HOST = '127.0.0.1'
SERVER_PORT = 6501
CLIENT_PORT = 6502

# TODO: Temp - needs to be changed
filepath = './Data'
filename = 'Stormdata_1950.csv'

# Initialize a UDP socket for the client
client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
# Bind the client to the specified host and port
client_socket.bind((HOST, CLIENT_PORT))

# Send an initial message to the server
message = "Hello from UDP client!"
client_socket.sendto(message.encode(), (HOST, SERVER_PORT))

# Wait for a response from the server and print it
message, server_address = client_socket.recvfrom(1024)
print(f"Message from server: {message.decode()}")

# Close the socket connection
client_socket.close()

# TODO: Temp - remove later
def temp_dht_simulator():
    """
    Simulates the setup phase of the Distributed Hash Table (DHT) by:
    1. Parsing a CSV file to count the number of storm events.
    2. Calculating the next prime number based on the event count to determine the hash table size.
    
    Parameters:
        None
    
    Returns:
        None
    """
    file = os.path.join(filepath, filename)
    num_events = count_storm_events_csv(file)
    next_prime = find_next_prime(num_events)
    print(f'Total events: {num_events}')
    print(f'Next Prime: {next_prime}')


def construct_local_dht(packet):
    node_id = get_node_id(packet)

    if self.node_id == node_id:
        store data locally

    else:
        neighbor_id = get_neighbor_id()
        send_to_neighbor(packet, neighbor_id)



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

# Source: https://nickyreinert.medium.com/how-to-find-prime-numbers-fast-8d0f7e8bd80f
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

    sqr_root_value = int(num ** 0.5)
    for i in range (3, sqr_root_value + 1, 2):
        if (num % i == 0):
            return False

    return True

# TODO: Remove later
temp_dht_simulator()