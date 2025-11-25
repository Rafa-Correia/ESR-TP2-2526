import asyncio
import time
import sys
import socket
import argparse

# Global vars 
DEFAULT_HOST = '0.0.0.0' #localhost
TCP_PORT = 1234 #can be anything we want!

BOOTSTRAP_ADDR = '10.0.5.10' #change to bootstrapper address if given as argument, maybe?
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

MSG_HANDSHAKE = 'H'            #       start handshake:    [H][FLAGS][SELF_NAME]
ANS_HANDSHAKE = 'K'            #  respond to handshake:    [K][FLAGS]
#needed? ^^^^

MSG_HEARTBEAT = 'B'            #        send heartbeat:    [B][FLAGS]

FLOOD_STREAM = 'F'             #       stream metadata:    [F][FLAGS][STREAM_ID][ ... other stuff ... ]

REQ_STREAM = 'S'               #      request a stream:    [S][FLAGS][STREAM_ID]
ANS_STREAM = 'A'               #      provide a stream:    [A][FLAGS][STREAM_ID] # we should be opening a UDP connection after receiving this message

MSG_ERROR = 'E'                #         send an error:    [E][FLAGS][ERROR_MSG]

class Node:
    def __init__(self, name, host = DEFAULT_HOST, bootstrap_addr = BOOTSTRAP_ADDR):
        self.name = name
        self.host = host

        self.bootstrap_addr = bootstrap_addr

        self.peer_addresses = {}
        self.peers = {} # str (node_id) -> (Reader, Writer)    |< this is the structure given by asyncio!
        self.streams = {} # str (stream_id) -> (stream info, such as provider)

        self.latest_heartbeat = {} # str (node_id) -> float? (last time heartbeat was received)

        self.tcp_server = None


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
                    writer.write(MSG_HEARTBEAT.encode('ASCII'))
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


    async def connect_to_peers(self):
        for peer_id, paddress_list in self.peer_addresses.items():
            if peer_id in self.peers.keys():
                continue #peer was registered already (maybe he sent the connection just as we started)
            
            for address in paddress_list:
                try:
                    reader, writer = await asyncio.open_connection(address, TCP_PORT)
                    
                    writer.write(MSG_HANDSHAKE.encode('ASCII') + self.name.encode('ASCII'))
                    await writer.drain()

                    data = await reader.read(1024)
                    msg = data.decode('ASCII')

                    if msg[0] != ANS_HANDSHAKE:
                        #idk what to do here tbh
                        break

                    self.peers[peer_id] = (reader, writer)
                    self.latest_heartbeat[peer_id] = time.time()
                    break
                except:
                    continue

            if peer_id in self.peers.keys():
                #if connection successfuly established
                asyncio.create_task(self.listen_to_peer(peer_id))


    async def listen_to_peer(self, peer_id):
        reader, writer = self.peers[peer_id]
        try:
            while True:
                data = await reader.read(1024)
                if not data:
                    break

                msg = data.decode('ASCII')

                msg_type = msg[0]
                msg = msg[1:]

                if msg_type == MSG_HEARTBEAT:
                    self.latest_heartbeat[peer_id] = time.time()
                    print(f"Heartbeat received from {peer_id}.")
                elif msg_type == FLOOD_STREAM:
                    # build tree
                    self.streams[msg] = peer_id # placeholder, fill with actual info
                elif msg_type == REQ_STREAM:
                    # setup node as requester to start stream
                    print(f"Stream {msg} requested from {peer_id}.")
                elif msg_type == ANS_STREAM:
                    # open UDP channel and start stream
                    print(f"Stream {msg} provided by {peer_id}.")
                else:
                    print(f"Unknown message type {msg_type} from {peer_id}.")

        except Exception as e:
            pass 
        finally:
            self.handle_death(peer_id)


    async def handle_connection(self, reader, writer):
        handshake_raw = await reader.read(1024)
        handshake = handshake_raw.decode('ASCII')

        if handshake[0] != MSG_HANDSHAKE:
            # Maybe throw error or something
            return

        else:
            writer.write(ANS_HANDSHAKE.encode('ASCII'))
            await writer.drain()

            peer_id = handshake[1:]

            print(f'Received handshake from {peer_id}.')

            self.peers[peer_id] = (reader, writer)
            self.latest_heartbeat[peer_id] = time.time()

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
    parser.add_argument("-s", "--server", action='store_true')

    args = parser.parse_args()

    node_name = args.node_name
    bootstrap_addr = args.bootstrapper
    host = args.address
    is_server = args.server

    if is_server:
        #do something!
        print('THIS IS A SERVER')

    else:
        node = Node(name=node_name, host=host, bootstrap_addr=bootstrap_addr)
    
    asyncio.run(node.start())


if __name__ == "__main__":
    main()