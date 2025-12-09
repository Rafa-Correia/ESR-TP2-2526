import asyncio
import time
import signal
import sys
import socket
import argparse
import json

# Global vars 
DEFAULT_HOST = '0.0.0.0' #localhost
TCP_PORT = 1234 #can be anything we want!
UDP_PORT_START = 5000 #can be anything we want!
UDP_PORT = UDP_PORT_START

BOOTSTRAP_ADDR = '10.0.17.10' # placeholder, should always be passed as argument
BOOTSTRAP_PORT = 4321 #can be anything

HEARTBEAT_INTERVAL = 5
HEARTBEAT_TIMEOUT  = HEARTBEAT_INTERVAL * 3

# Message definitions and stuff 

#ALL MESSAGES ARE STRUCTURED AS FOLLOWS
# [TYPE][FIELD *][\n] 
#   1B     NB     1B
# fields are separated by ;


MSG_ERROR = 'E'                #         send an error:    [E][ERROR_MSG][\n]
MSG_HANDSHAKE = 'H'            #       start handshake:    [H][SELF_NAME][\n]
ANS_HANDSHAKE = 'K'            #  respond to handshake:    [K][\n]
#needed? ^^^^

MSG_HEARTBEAT = 'B'            #        send heartbeat:    [B][\n]

FLOOD_STREAM = 'F'             #       stream metadata:    [F][STREAM_ID][stream_1][stream_2*]...[stream_N*][\n]    *optional
# each stream is as follows:
#   stream_id [str]: n_jumps(for now) [int]
REQ_PARENT = 'P'               #  req. parent provider:    [P][STREAM_ID]
ANS_PARENT = 'R'               #  ans. parent provider:    [R][STREAM_ID][PARENT_NAME][PARENT_ADDRESS]

REQ_STREAM = 'S'               #      request a stream:    [S][STREAM_ID][\n]
ANS_STREAM = 'A'               #      provide a stream:    [A][STREAM_ID][\n] # we should be opening a UDP connection after receiving this message
UNREQ_STREAM = 'U'             #  'unrequest' a stream:    [U][STREAM_ID][\n] # basically, tell the receiving node that the stream will no longer be consumed
UNPROVIDE_STREAM = 'N'         #   notify stop provide:    [N][stream_1][stream_2*]...[stream_n*][\n]               *optional

MSG_METRIC = 'M'               #    req metric measure:    [M][METRIC_TYPE][ ... idk yet ... ][\n]
# metric types
METRIC_LATENCY = 'L'           # all of these sent by UDP?
METRIC_BANDWIDTH = 'W'
METRIC_LOSS = 'S'   

MSG_SHUTDOWN = 'D'             #        inform shutdown:   [D][\n]

MSG_FIN = 'C'                  #    shutdown of channel:   [C][\n]

# ---


class Node:
    def __init__(self, name : str, host : str = DEFAULT_HOST, bootstrap_addr : str = BOOTSTRAP_ADDR, server_manifest_path : str = None):
        self.name = name
        self.host = host

        self.bootstrap_addr = bootstrap_addr
        self.peer_addresses = {} #node id -> node address
        
        self.peers = {} # str (node_id) -> (Reader, Writer)    |< this is the structure given by asyncio!
        self.peers_udp = {} # probably the same as above
        
        self.streams = {}               # stream_id [str] -> {provider , consumers, metrics, <video_info>} <- still unclear what stream metadata we need
                                        #                     [str]       [[str]]   [smth]    stuff
        self.stream_backups = {}        # stream_id [str] -> {provider -> provider_ip, parent, metrics, <video_info>} <- provider ip will only be used to establish emergency connection to parent provider
                                        #                      [str]        [str]     [bool]   [smth]    stuff

        self.latest_heartbeat = {} # str (node_id) -> float? (last time heartbeat was received)

        self.parent_requests = [] # list of streams which parents are currently requested

        self.tcp_server = None
        self.udp_server = None
        
        # server only vars
        self.is_server = server_manifest_path is not None
        self.sv_manifest_path = server_manifest_path
        self.own_streams = {}

        #asyncio stuff, dont worry about it :))))
        self._shutdown_event = None
        self._async_tasks = []


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
            address = lb[1]
            neigh_addr_dict[node] = address

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
                    await self.handle_death(peer_id)

    async def check_heartbeats(self):
        while True:
            await asyncio.sleep(5) #arbitrary, choose another good value later maybe
            now = time.time()
            for peer_id, last in self.latest_heartbeat.items():
                if now - last > HEARTBEAT_TIMEOUT:
                    await self.handle_death(peer_id)
                    
            print(self.streams)
            if self.is_server:
                print(self.own_streams)
            print(self.stream_backups)
            print('')
            print('')


    async def connect_to_peer(self, peer_id, peer_address): #CAREFULL! MIGHT THROW EXCEPTION!
        if peer_id in self.peers.keys():
            return 1
        
        reader, writer = await asyncio.open_connection(peer_address, TCP_PORT)

        writer.write(MSG_HANDSHAKE.encode('ASCII') + self.name.encode('ASCII') + b'\n')
        await writer.drain()

        data = await reader.readline()
        msg = data.decode('ASCII').strip(' \n')

        if msg[0] != ANS_HANDSHAKE:
            return 2
        
        #self.peers[peer_id] = (reader, writer)
        self.latest_heartbeat[peer_id] = time.time()

        return (reader, writer)


    async def connect_to_peers(self):
        successes = 0
        peers = {}
        for peer_id, peer_address in self.peer_addresses.items():   
            try:
                print(f"Attempting to connect to peer {peer_id}...", end="")
                res = await self.connect_to_peer(peer_id, peer_address)

                if res == 1:
                    print(' peer was already registered. Continuing...')
                    continue
                elif res == 2:
                    print(" failed! Peer didn't answer handshake!")
                    continue
                
                else:
                    peers[peer_id] = res # res has (reader, writer) in case of success

            except:
                print(" failed!")
                continue

            print(" success!")
            successes += 1

        for peer_id, (reader, writer) in peers.items():
            self.peers[peer_id] = (reader, writer)
            asyncio.create_task(self.listen_to_peer(peer_id))

        if successes > 0:       
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
            await self.handle_death(peer_id)


    async def process_request(self, peer_id, message):
        reader, writer = self.peers[peer_id]

        msg_type = message[0]
        msg = message[1:]
        
        print(f"[{peer_id}]: {msg_type} , {msg}")
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
            #print(f'FLOOD Received from {peer_id}')

            streams_raw = msg.split(';')
            streams = []
            for stream in streams_raw:
                stream_id, info = stream.split(':')
                stream_info = info.split(',')
                #TODO : change later when using metrics
                jumps = stream_info[0]
                streams.append((stream_id, int(jumps) + 1))

            altered_streams = [] # just to keep track of what streams we need to flood

            for stream_id, n_jumps in streams:
                if stream_id not in self.streams:
                    self.streams[stream_id] = {
                        "n_jumps": n_jumps,
                        "provider": peer_id,
                        "consumers": set(),
                    }
                    altered_streams.append(stream_id)
                else:
                    if n_jumps < self.streams[stream_id]["n_jumps"]:
                        self.streams[stream_id]["n_jumps"] = n_jumps
                        self.streams[stream_id]["provider"] = peer_id
                        altered_streams.append(stream_id)
                    else:
                        backup = self.stream_backups.get(stream_id, None)

                        if backup is None:
                            await self.request_parent(stream_id, peer_id)
                        
                        elif len(backup) == 1:
                            (provider, backup_info), = backup.items()
                            if backup_info['parent']:
                                backup.pop(provider)
                        
                            self.stream_backups[stream_id][peer_id] = {
                                "n_jumps": n_jumps,
                                "provider_ip": None,
                                "parent": False
                            }
                        else:
                            self.stream_backups[stream_id][peer_id] = {
                                "n_jumps": n_jumps,
                                "provider_ip": None,
                                "parent": False
                            }
                        
            #flood
            await self.flood(altered_streams)
        
        elif msg_type == REQ_PARENT:
            stream_id = msg
            print(f'Parent requested from {peer_id} for {stream_id}.')

            if stream_id in self.streams.keys():
                #it exists!
                info = self.streams[stream_id]
                provider = info["provider"]
                provider_address = self.peer_addresses[provider]

                writer.write(f'{ANS_PARENT}{stream_id};{provider};{provider_address}\n'.encode('ASCII'))
                await writer.drain()
                
            else:
                writer.write(f'{MSG_ERROR}Provided stream_id is not known.\n'.encode('ASCII'))
                await writer.drain()

        elif msg_type == ANS_PARENT:
            fields = msg.split(';')
            stream_id = fields[0]
            parent_name = fields[1]
            parent_address = fields[2]

            if stream_id in self.parent_requests:
                self.parent_requests.pop(self.parent_requests.index(stream_id))

                if not self.stream_backups[stream_id]:
                    self.stream_backups[stream_id] = {}
                    self.stream_backups[stream_id][parent_name] = {
                                    "provider_ip": parent_address,
                                    "parent": True
                    }

            else:
                print(f"Request for parent of {stream_id} was either already satisfied or never existed.")    

        elif msg_type == UNPROVIDE_STREAM:
            stream_list = msg.split(';')

            #step 1, gather all streams peer provides and backups with matching IDS

            #step 2, remove backups provided by peer and remove provided streams and switch to backups

            #step 3, notify changes (if stream provider changed!)

        elif msg_type == MSG_SHUTDOWN:
            print(f'Shutdown from {peer_id}!')

            await self.handle_death(peer_id)

        elif msg_type == MSG_ERROR:
            print(f"Error received from {peer_id}: {msg}")

        else:
            print(f"Unknown message type {msg_type} from {peer_id}.")


    async def flood(self, stream_ids, is_own=False):
        streams = {}

        #setup !
        for stream_id in stream_ids:
            if is_own:
                stream_info = self.own_streams.get(stream_id, None)
                if stream_info is None:
                    print(f'Warning: attempting to flood own {stream_id} that does not exist. Ignoring...')
                    continue

                n_jumps = 0
                provider = None

                streams[stream_id] = (n_jumps, provider)

            else:
                stream_info = self.streams.get(stream_id, None)
                if stream_info is None:
                    print(f'Warning: attempting to flood {stream_id} that does not exist. Ignoring...')

                n_jumps = stream_info['n_jumps']
                provider = stream_info['provider']

                streams[stream_id] = (n_jumps, provider)

        for peer_id in self.peers.keys():
            reader, writer = self.peers[peer_id]
            to_flood = []

            for stream_id in streams.keys():
                n_jumps, provider = streams[stream_id]
                if peer_id != provider:
                    to_flood.append(stream_id)


            if not to_flood:
                continue
            
            msg_str = f"{FLOOD_STREAM}"
            for stream_id in to_flood:
                n_jumps, _ = streams[stream_id]
                #TODO: change to use metrics and add any necessary stuff later
                msg_str += f'{stream_id}:{n_jumps};'

            #substitute trailing ; by \n
            
            msg_str = msg_str[:-1] + '\n'

            msg = msg_str.encode('ASCII')

            writer.write(msg)
            await writer.drain()
            
                 
    async def flood_all(self):
        #if self.streams:
        #    print("Called flood_all, not empty")
        #else:
        #    print("Called flood_all, empty")

        if self.is_server:
            await self.flood(list(self.own_streams.keys()), True)

        await self.flood(list(self.streams.keys()))


    async def request_parent(self, stream_id, peer_id, peer = None):
        if peer:
            _, writer = peer
        else:
            _, writer = self.peers[peer_id]

        msg_str = f'{REQ_PARENT}{stream_id}\n'
        msg = msg_str.encode('ASCII')

        if stream_id not in self.parent_requests:
            self.parent_requests.append(stream_id)

            writer.write(msg)
            await writer.drain()


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

            self._async_tasks.append(asyncio.create_task(self.listen_to_peer(peer_id)))


    #this version handles death when peer sends a shutdown signal
    async def handle_death(self, peer_id):
        peer = self.peers.pop(peer_id, None)
        self.latest_heartbeat.pop(peer_id, None)
        self.peer_addresses.pop(peer_id)

        if peer:           
            #step 1 - gather all streams peer provides
            streams = []

            for stream_id, stream_info in self.streams.items():
                if peer_id == stream_info["provider"]:
                    streams.append(stream_id)
            
            #step 2 - delete backups that peer provides
            for _, dict in self.stream_backups.items():
                dict.pop(peer_id)
            
            #step 3 - choose best backup for each stream
                
            #TODO
            # 1st, check if backup is neighbour node.
            # 2nd, if not, connect to parent node.
            # 3rd, new provider for stream is now backup node.
            for stream_id in streams:
                backup = self.stream_backups.get(stream_id, None)

                #now, this shouldnt really happen. by this point AT LEAST 1 backup should be in place.
                if not backup:
                    self.stream_backups[stream_id] = {}

                    provider = self.streams[stream_id]["provider"]
                    if provider == peer_id:
                        await self.request_parent(peer_id=peer_id, peer=peer)
                    else:
                        await self.request_parent(provider)


                #choose best backup provider
                best_heuristic = 1000000 #for now use n_jumps
                best_provider = None
                for provider, provider_metrics in backup.items():
                    n_jumps = provider_metrics["n_jumps"]
                    if n_jumps < best_heuristic:
                        best_provider = provider
                        best_heuristic = n_jumps

                self.streams[stream_id]["provider"] = best_provider
                self.streams[stream_id]["n_jumps"] = best_heuristic

                self.stream_backups[stream_id].pop(best_provider) #remove from backup

                if not self.stream_backups[stream_id]:
                    self.stream_backups[stream_id] = {}

                    provider = self.streams[stream_id]["provider"]
                    if provider == peer_id:
                        await self.request_parent(peer_id=peer_id, peer=peer)
                    else:
                        await self.request_parent(provider)

            #step 4 - trigger flood for altered streams

            await self.flood(streams)
            
            

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
                stream_info = {"n_jumps": 0, "provider": None, "consumers": set(), "metadata": data[stream_id]}
                self.own_streams[stream_id] = stream_info
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

        self._shutdown_event = asyncio.Event()
        loop = asyncio.get_running_loop()

        loop.add_signal_handler(signal.SIGINT, self._on_signal)
        loop.add_signal_handler(signal.SIGTERM, self._on_signal)

        self.server = await asyncio.start_server(self.handle_connection, self.host, TCP_PORT)
        # from now on, server is listening

        self._async_tasks.append(asyncio.create_task(self.connect_to_peers()))
        self._async_tasks.append(asyncio.create_task(self.heartbeat_loop()))
        self._async_tasks.append(asyncio.create_task(self.check_heartbeats()))

        async with self.server:
            await self._shutdown_event.wait()

        await self.shutdown()


    def _on_signal(self):
        self._shutdown_event.set()


    async def shutdown(self):
        print('Shutting down...', end='')
        for _, (reader, writer) in self.peers.items():
            writer.write(f"{MSG_SHUTDOWN}\n".encode('ASCII'))
            await writer.drain()

            while True:
                msg = await reader.readline()
                msg = msg.decode('ASCII').strip(' \n')

                msg_type = msg[0]
                msg_content = msg[1:]

                if msg_type == REQ_PARENT:
                    #TODO: HANDLE PARENT REQUEST ON DEATH
                    print('TODO')
                elif msg_type == MSG_FIN:
                    break
                else:
                    writer.write(f"{MSG_ERROR}SHUTDOWN ALREADY SENT! NO LONGER ACCEPTING MESSAGES.\n".encode('ASCII'))
                    await writer.drain()

        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()

        for task in self._async_tasks:
            task.cancel()

        await asyncio.gather(self._async_tasks, return_exceptions=True)

        print(' done!') 


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