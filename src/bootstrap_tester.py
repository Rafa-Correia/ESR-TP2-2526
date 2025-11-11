#simple program to test if bootstrapper is working

import socket

addr = "10.0.1.10"
port = 1234

name = input("Node's name: ")

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((addr, port))
    s.sendall(name.encode('ascii'))
    data = s.recv(1024)
    
print(data.decode('ascii'))