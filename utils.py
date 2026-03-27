import json
import socket
import csv

#UTILITY FUNCTIONS for manager and peer

#GENERAL FUNCTIONS
def send_message(sock, address, header, body=None):
    message = {"header": header, "body": body}
    payload = json.dumps(message).encode()
    sock.sendto(payload, address)

def read_message(payload):
    message = json.loads(payload.decode())
    return message["header"], message["body"]

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
    elif num == 3:
        return True

    sqr_root_value = int(num ** 0.5)
    for i in range (3, sqr_root_value + 1, 2):
        if (num % i == 0):
            return False

    return True

def print_record(record):
    #prints record
    print(f"Storm Event Found:")
    print(f"Event ID: {record['EVENT_ID']}")
    print(f"State: {record['STATE']}")
    print(f"Year: {record['YEAR']}")
    print(f"Month: {record['MONTH']}")
    print(f"Event Type: {record['EVENT_TYPE']}")
    print(f"CZ Type: {record['CZ_TYPE']}")
    print(f"CZ Name: {record['CZ_NAME']}")
    print(f"Direct Injuries: {record['INJURIES_DIRECT']}")
    print(f"Indirect Injuries: {record['INJURIES_INDIRECT']}")
    print(f"Direct Deaths: {record['DEATHS_DIRECT']}")
    print(f"Indirect Deaths: {record['DEATHS_INDIRECT']}")
    print(f"Property Damage: {record['DAMAGE_PROPERTY']}")
    print(f"Crops Damage: {record['DAMAGE_CROPS']}")
    print(f"Tornado F Scale: {record['TOR_F_SCALE']}")