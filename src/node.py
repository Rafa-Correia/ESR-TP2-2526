import asyncio
import time
import sys
import socket
import argparse
import json

# Global vars 
DEFAULT_HOST = '0.0.0.0' #localhost
TCP_PORT = 1234 #can be anything we want!
UDP_PORT_START = 5000 #can be anything we want!
UDP_PORT = UDP_PORT_START

BOOTSTRAP_ADDR = '10.0.3.10' # placeholder, should always be passed as argument
BOOTSTRAP_PORT = 4321 #can be anything

HEARTBEAT_INTERVAL = 5
HEARTBEAT_TIMEOUT  = HEARTBEAT_INTERVAL * 3


# Message definitions and stuff 

#ALL MESSAGES ARE STRUCTURED AS FOLLOWS
# [TYPE][FLAGS][FIELD *]
#   1B    1B      NB
# fields are separated by ;
# for now we'll ignore flags, though
# kinda maybe don't need them?

MSG_ERROR = 'E'                #         send an error:    [E][FLAGS][ERROR_MSG]
MSG_HANDSHAKE = 'H'            #       start handshake:    [H][FLAGS][SELF_NAME]
ANS_HANDSHAKE = 'K'            #  respond to handshake:    [K][FLAGS]
#needed? ^^^^

MSG_HEARTBEAT = 'B'            #        send heartbeat:    [B][FLAGS]

FLOOD_STREAM = 'F'             #       stream metadata:    [F][FLAGS][STREAM_ID][ ... other stuff ... ]
#FLOOD_ACK = 'C'               #     acknowledge flood:    [C][FLAGS]$

REQ_STREAM = 'S'               #      request a stream:    [S][FLAGS][STREAM_ID]
ANS_STREAM = 'A'               #      provide a stream:    [A][FLAGS][STREAM_ID] # we should be opening a UDP connection after receiving this message

MSG_METRIC = 'M'               #    req metric measure:    [M][FLAGS][METRIC_TYPE][ ... idk yet ... ]
# metric types
METRIC_LATENCY = 'L'
METRIC_BANDWIDTH = 'W'
METRIC_LOSS = 'S'   
# ---


class Node:
    def __init__(self, name : str, host : str = DEFAULT_HOST, bootstrap_addr : str = BOOTSTRAP_ADDR, server_manifest_path : str = None):
        self.name = name
        self.host = host

        self.bootstrap_addr = bootstrap_addr
        self.peer_addresses = {} #node id -> [node addresses (str)]
        
        self.peers = {} # str (node_id) -> (Reader, Writer)    |< this is the structure given by asyncio!
        self.peers_udp = {} # probably the same as above
        
        self.streams = {} # str (stream_id) -> (provider , consumers, metrics, <video_info>)
                                            #   node_id   [node_id]   [smth]    stuff

        self.latest_heartbeat = {} # str (node_id) -> float? (last time heartbeat was received)

        self.tcp_server = None
        self.udp_server = None
        
        # server only vars
        self.is_server = server_manifest_path is not None
        self.sv_manifest_path = server_manifest_path
        self.own_streams = {}


    def get_peers(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.bootstrap_addr, BOOTSTRAP_PORT))
            s.sendall(self.name.encode('ascii'))
            data = s.recv(1024)
            s.close()

        data_str = data.decode('ascii')

        if data_str == '%':
            return 1
        if data_str == '$':
            return 2

        node_addr_str = data_str.split(';')
        neigh_addr_dict = {}

        for na_str in node_addr_str:
            lb = na_str.split(':')
            node = lb[0]
            addresses_str = lb[1]
            addresses = addresses_str.split(',')
            neigh_addr_dict[node] = addresses

        if not neigh_addr_dict:
            return 3
        else:
            self.peer_addresses = neigh_addr_dict
            return 0


    async def heartbeat_loop(self):
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            for peer_id, (_, writer) in self.peers.items(): #get every neighbours stream writer
                try:
                    msg = MSG_HEARTBEAT + '\n'
                    writer.write(msg.encode('ASCII'))
                    await writer.drain()
                except Exception as e:
                    self.handle_death(peer_id)

    async def check_heartbeats(self):
        while True:
            await asyncio.sleep(2) #arbitrary, choose another good value later maybe
            now = time.time()
            for peer_id, last in self.latest_heartbeat.items():
                if now - last > HEARTBEAT_TIMEOUT:
                    self.handle_death(peer_id)
                    
            #print(self.streams)
            #if self.is_server:
            #    print(self.own_streams)


    async def connect_to_peers(self):
        for peer_id, paddress_list in self.peer_addresses.items():
            if peer_id in self.peers.keys():
                continue #peer was registered already (maybe he sent the connection just as we started)
            
            for address in paddress_list:
                try:
                    reader, writer = await asyncio.open_connection(address, TCP_PORT)
                    
                    writer.write(MSG_HANDSHAKE.encode('ASCII') + self.name.encode('ASCII') + b'\n')
                    await writer.drain()

                    data = await reader.readline()
                    msg = data.decode('ASCII').strip(' \n')

                    if msg[0] != ANS_HANDSHAKE:
                        print("Response to handshake wasn't ANS.")
                        break

                    self.peers[peer_id] = (reader, writer)
                    self.latest_heartbeat[peer_id] = time.time()
                    break
                except:
                    continue

            if peer_id in self.peers.keys():
                #if connection successfuly established
                asyncio.create_task(self.listen_to_peer(peer_id))
                
        await self.flood_all()


    async def listen_to_peer(self, peer_id):
        reader, writer = self.peers[peer_id]
        try:
            while True:
                data = await reader.readline()
                if not data:
                    break

                msg = data.decode('ASCII').strip(' \n')

                await self.process_request(peer_id, msg)

        except Exception as e:
            print(e)
        finally:
            self.handle_death(peer_id)


    async def process_request(self, peer_id, message):
        msg_type = message[0]
        msg = message[1:]
        
        #print(f"{msg_type} , {msg}"
        if msg_type == MSG_HEARTBEAT:
            self.latest_heartbeat[peer_id] = time.time()
            #print(f"Heartbeat received from {peer_id}.")
            
        elif msg_type == REQ_STREAM:
            # setup node as requester to start stream
            print(f"Stream {msg} requested from {peer_id}.")
            
        elif msg_type == ANS_STREAM:
            # open UDP channel and start stream
            print(f"Stream {msg} provided by {peer_id}.")
            
        elif msg_type == FLOOD_STREAM:
            msg_split = msg.split(';')
            stream_id = msg_split[0]
            n_jumps_incoming = int(msg_split[1]) + 1
            #print(f"Flood received from {peer_id} for stream {stream_id}.")
            if stream_id in self.streams.keys():
                n_jumps_stored, provider, consumers = self.streams[stream_id]
                if n_jumps_incoming < n_jumps_stored and peer_id not in consumers: #dont accept flood from consumer
                    self.streams[stream_id] = (n_jumps_incoming, peer_id, consumers)
                    await self.flood(stream_id)
            else:
                self.streams[stream_id] = (n_jumps_incoming, peer_id, set())
                await self.flood(stream_id)
            
            
      
        else:
            print(f"Unknown message type {msg_type} from {peer_id}.")


    async def flood(self, stream_id, is_own=False):
        
        if is_own:
            n_jumps_stored, provider, consumers, _ = self.own_streams[stream_id]
            #      0,         None,     set()
        else:  
            n_jumps_stored, provider, consumers = self.streams[stream_id]
        
        #who to send it to? 
        #anyone but their provider (or backup provider, if exists) and already existing consumers?
        for peer_id, (reader, writer) in self.peers.items():
            if peer_id == provider: #also check if in backup provider later!
                continue
            
            print(peer_id)
            
            consumers.add(peer_id)
            self.streams[stream_id] = (n_jumps_stored, provider, consumers)
            
            msg_str = FLOOD_STREAM + stream_id + ';' +  str(n_jumps_stored) + '\n'
            msg = msg_str.encode('ASCII')
            
            writer.write(msg)
            await writer.drain()
            
                 
    async def flood_all(self):
        if self.streams:
            print("Called flood_all, not empty")
        else:
            print("Called flood_all, empty")

        
        for stream_id, _ in self.streams.items():
            await self.flood(stream_id)
            
        if self.is_server:
            for stream_id, _ in self.own_streams.items():
                await self.flood(stream_id, is_own=True)


    async def handle_connection(self, reader, writer):
        handshake_raw = await reader.readline()
        handshake = handshake_raw.decode('ASCII').strip(' \n')

        if handshake[0] != MSG_HANDSHAKE:
            writer.write(MSG_ERROR.encode('ASCII') + b'\n')
            await writer.drain()
            return

        else:
            writer.write(ANS_HANDSHAKE.encode('ASCII') + b'\n')
            await writer.drain()

            peer_id = handshake[1:]

            print(f'Received handshake from {peer_id}.')

            self.peers[peer_id] = (reader, writer)
            self.latest_heartbeat[peer_id] = time.time()
            
            
            await self.flood_all()
            asyncio.create_task(self.listen_to_peer(peer_id))


    def handle_death(self, peer_id):
        #for now, we just need to peer from neighbours
        #later on we need to handle connencting to parent provider of neighbour
        peer = self.peers.pop(peer_id, None)
        self.latest_heartbeat.pop(peer_id, None)

        if peer:
            _, writer = peer
            writer.close()
            print(f"Closed connection with {peer_id}.")


    def load_manifest(self):
        if not self.is_server:
            #print("Attempting to load manifest while not a server.")
            return 1

        try:
            with open(self.sv_manifest_path, 'r') as file:
                data = json.load(file)['streams']
            if not data:
                return 2
            
            for stream_id in data.keys():
                self.own_streams[stream_id] = (0, None, set(), data[stream_id])
        except Exception as e:
            print(e)
            return 2
        

    async def start(self):
        res = self.get_peers()
        
        if res == 1:
            print(f'{self.name} does not exist! Please use a valid node_name!')
            return 1
        if res == 2:
            print(f"{self.name} is not this node's name! Maybe you've opened this on the wrong node?")
            return 1
        if res == 3:
            print('No neighbours exist... (what)')
            return 1
        
        if self.is_server:
            res = self.load_manifest()
            if res == 1:
                print("Attempting to load manifest while not a server.")
                return 1
            if res == 2:
                print(f'Manifest at "{self.sv_manifest_path}" is invalid.')
                return 1

        self.server = await asyncio.start_server(self.handle_connection, self.host, TCP_PORT)
        # from now on, server is listening

        asyncio.create_task(self.connect_to_peers())

        asyncio.create_task(self.heartbeat_loop())
        asyncio.create_task(self.check_heartbeats())

        async with self.server:
            await self.server.serve_forever() # keeps server alive ! :)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("node_name")
    parser.add_argument("-b", "--bootstrapper", type=str, default=BOOTSTRAP_ADDR)
    parser.add_argument("-a", "--address", type=str, default=DEFAULT_HOST)
    parser.add_argument("-s", "--server", type=str, dest="manifest")

    args = parser.parse_args()

    node_name = args.node_name
    bootstrap_addr = args.bootstrapper
    host = args.address
    manifest = args.manifest

    node = Node(name=node_name, host=host, bootstrap_addr=bootstrap_addr, server_manifest_path=manifest)
    
    asyncio.run(node.start())


if __name__ == "__main__":
    main()