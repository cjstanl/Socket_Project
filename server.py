import socket

HOST = '127.0.0.1'
SERVER_PORT = 6501

server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
server_socket.bind((HOST, SERVER_PORT))

print(f"UDP server is listening on {HOST}:{SERVER_PORT}")

while True:
    data, client_address = server_socket.recvfrom(1024)
    print(f"Received from {client_address}: {data.decode()}")
    server_socket.sendto(b'Hello from UDP server!', client_address)
