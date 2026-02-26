import socket
import csv
import os

HOST = '127.0.0.1'
SERVER_PORT = 6501
CLIENT_PORT = 6502

# TODO: Temp - needs to be changed
filepath = './Data'
filename = 'Stormdata_1950.csv'

client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
client_socket.bind((HOST, CLIENT_PORT))

message = "Hello from UDP client!"
client_socket.sendto(message.encode(), (HOST, SERVER_PORT))

message, server_address = client_socket.recvfrom(1024)
print(f"Message from server: {message.decode()}")

client_socket.close()

# TODO: Temp - remove later
def temp_dht_simulator():
    file = os.path.join(filepath, filename)
    num_events = count_storm_events_csv(file)
    next_prime = find_next_prime(num_events)
    print(f'Total events: {num_events}')
    print(f'Next Prime: {next_prime}')


def count_storm_events_csv(filename):
    event_count = 0
    with open(filename, 'r', newline='') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)
        for row in csv_reader:
            event_count += 1
    return event_count


def find_next_prime(total_storm_events):
    current_num = total_storm_events * 2 + 1

    while True:
        if (is_prime(current_num)):
            return current_num
        else:
            current_num = current_num + 1

# Source: https://nickyreinert.medium.com/how-to-find-prime-numbers-fast-8d0f7e8bd80f
def is_prime(num):
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