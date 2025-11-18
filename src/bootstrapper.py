# Source for bootstrapper server.
import sys
import socket
import json
import signal

class Bootstrapper:
    def __init__(self, filepath):
        with open(filepath, 'r') as file:
            self.data = json.load(file)['nodes']
    
    def find(self, node_name, addr):
        if node_name in self.data.keys():
            n_addresses = self.data[node_name]['addresses']
            (ip, port) = addr
            if ip not in n_addresses:
                print(f"Node {node_name} doesn't have matching address {addr}.")
                return -1
            
            n_dict = {}
            neighbours = self.data[node_name]['neighbours']
            if not neighbours:
                return None #if no neighbours, don't return anything!
            for neighbour in neighbours:
                addresses = self.data[neighbour]['addresses']
                n_dict[neighbour] = addresses
            
            return n_dict
        else:
            return None
        
        
def main(argc, argv):
    if argc != 2:
        print("Wrong number of arguments provided.")
        return
    
    b = Bootstrapper(argv[1])

    address = ''
    port = 1234
    
    ssocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ssocket.bind((address, port))
    ssocket.listen()
    
    def handle_sigint(sig, frame):
        print("Attempting graceful shutdown...")
        ssocket.close()
        sys.exit(0)
        
    signal.signal(signal.SIGINT, handle_sigint)
    
    try:
        while True:
            connection, c_addr = ssocket.accept()
            with connection:
                data = connection.recv(1024) # temporary upper limit on data 
                                            # honestly just a safeguard or something                  
                if not data:
                    break #if data is non truthy then client has closed connection
                
                result = b.find(data.decode('ascii'), c_addr)
                data_string = ''
                if result == -1:
                    data_string = '$'
                elif result is not None:
                    #transform dictionary into serializable string
                    keys = result.keys()
                    for i, neighbour in enumerate(keys):
                        data_string += f'{neighbour}:'
                        addresses = result[neighbour]
                        address_string = ','.join(addresses)
                        data_string += address_string
                        if(i != len(keys) - 1):
                            data_string += ';'
                else:
                    data_string = '%'
                
                print(f"Got request from {data.decode('ascii')} at addr {c_addr}. Result: {data_string}.")
                
                connection.sendall(data_string.encode('ascii'))
                connection.close()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        ssocket.close()
        print("Socket closed...")

if __name__ == '__main__':
    main(len(sys.argv), sys.argv)