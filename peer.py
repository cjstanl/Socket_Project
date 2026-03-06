import socket
import sys

#PEER
#   Main Peer Function that...


#Call Manager function on startup
if __name__ == "__main__":
    

    for line in sys.stdin:
        if 'q' == line.rstrip():
            break
        print(f'Input:{line}')
    print('exit')