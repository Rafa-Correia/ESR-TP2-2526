#simple program to test if bootstrapper is working

import socket

addr = "10.0.5.10"
port = 1234

name = input("Node's name: ")

def dec_data(data):
    data_str = data.decode('ascii')
    if data_str == '%':
        return '%'
    if data_str == '$':
        return '$'
    node_addr_str = data_str.split(';')
    neigh_addr_dict = {}
    for na_str in node_addr_str:
        lb = na_str.split(':')
        node = lb[0]
        addresses_str = lb[1]
        addresses = addresses_str.split(',')
        neigh_addr_dict[node] = addresses

    if not neigh_addr_dict:
        return None
    else:
        return neigh_addr_dict    

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((addr, port))
    s.sendall(name.encode('ascii'))
    data = s.recv(1024)
    
res = dec_data(data)

print(res)