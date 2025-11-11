# Source for bootstrapper server.
import sys
import socket
import json
import signal

class Bootstrapper:
    def __init__(self, filepath):
        with open(filepath, 'r') as file:
            self.data = json.load(file)['nodes']
    
    def find(self, node_name):
        if node_name in self.data.keys():
            return self.data[node_name]['neighbours']
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
                
                result = b.find(data.decode('ascii'))
                if result:
                    result = ' - '.join(result)
                else:
                    result = 'Not found'
                
                print(f"Got request from {data.decode('ascii')} at addr {c_addr}. Result: {result}.")
                
                connection.sendall(result.encode('ascii'))
                connection.close()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        ssocket.close()
        print("Socket closed...")

if __name__ == '__main__':
    main(len(sys.argv), sys.argv)