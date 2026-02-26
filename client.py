import socket

HOST = '127.0.0.1'
SERVER_PORT = 6501
CLIENT_PORT = 6502

client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
client_socket.bind((HOST, CLIENT_PORT))

message = "Hello from UDP client!"
client_socket.sendto(message.encode(), (HOST, SERVER_PORT))

message, server_address = client_socket.recvfrom(1024)
print(f"Message from server: {message.decode()}")

client_socket.close()