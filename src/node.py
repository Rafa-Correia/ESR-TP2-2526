import asyncio
import time
import signal
import sys
import socket
import argparse
import json
import functools

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
#   stream_id:root_provider_id,version,n_jumps,heuristic,<metrics here>
#      str       str            int     int      float       ???

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

        self.link_stats = {} # peer_id -> {metrics, link cost?}
        
        self.streams = {}               
        # dict

        #format:
        #self.streams = {
        #   'stream_id': {
        #       'current_root': 'root_id',
        #       'provisions': {
        #           'root_id': {                                #root id will be the id of the providing server
        #               'version': int (current tree version)
        #               'best': {
        #                   'provider': str,        (parent id)
        #                   'n_jumps': int,         (jumps from root to self, might be useful?)
        #                   'heuristic': float,     (local cost, which means provider cost + link cost) (COST IS FROM THIS NODE TO ROOT, NOT PARENT TO ROOT)
        #                   'metrics': {...}        (these are parent only, link costs are stored somewhere else)
        #               },
        #               'backup': {
        #                   'backup_provider': {
        #                       'parent': bool,             (if backup is parent provider for self-healing)
        #                       'parent_ip': str | None,    (only set if parent)
        #                       'n_jumps': int,             (jumps from root to self)
        #                       'heuristic': float,         (local cost)
        #                       'metrics': {...}            (parent only)
        #                   }
        #               }
        #           },
        #           ...
        #       }
        #   }
        #}


        self.latest_heartbeat = {} # str (node_id) -> float? (last time heartbeat was received)

        self.parent_requests = [] # list of streams which parents are currently requested

        self.tcp_server = None
        self.udp_server = None
        
        # server only vars
        self.is_server = server_manifest_path is not None
        self.sv_manifest_path = server_manifest_path
        self.own_streams = {}
        self.current_version = 0

        #asyncio stuff, dont worry about it :))))
        self._async_tasks = []


    #server only
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
                stream_info = {"consumers": set(), "metadata": data[stream_id]}
                self.own_streams[stream_id] = stream_info
        except Exception as e:
            print(e)
            return 2


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
                    await self.catastrophy(peer_id)

    async def check_heartbeats(self):
        while True:
            await asyncio.sleep(20) #arbitrary, choose another good value later maybe
            now = time.time()
            for peer_id, last in self.latest_heartbeat.items():
                if now - last > HEARTBEAT_TIMEOUT:
                    await self.catastrophy(peer_id)
                    
            #self.print_streams()



    def print_streams(self):
        if not self.streams:
            print('\nNo streams yet!')
            return 
         
        print('')
        for stream_id, stream_state in self.streams.items():
            current_root = stream_state["current_root"]

            print(f'Stream: {stream_id}')
            print(f'\t| Best provision: {current_root}')
            for root_id, provision_state in stream_state["provisions"].items():
                tree_version = provision_state["version"]
                best = provision_state["best"]
                backup = provision_state["backup"]
                print(f'\t| Root {root_id} (version {tree_version}):')
                print(f'\t\t| Best: {best["provider"]} (total cost: {best["heuristic"]}, jumps: {best["n_jumps"]})')
                print(f'\t\t| Backups: ')
                for backup_id, backup_state in backup.items():
                    print(f'\t\t\t| Provider {backup_id}:  cost: {backup_state["heuristic"]}, jumps: {backup_state["n_jumps"]}')

            print('\n')

        if self.is_server:
            for stream_id, stream_info in self.own_streams.items():
                print(f'[OWN] Stream: {stream_id}')

    def print_peers(self):
        if not self.peers:
            print('\nNo peers yet!')
            return
        
        print(f'\nPeers:')
        for peer in self.peers.keys():
            print(f'\t- {peer}')



    async def connect_to_peer(self, peer_id, peer_address): #CAREFULL! MIGHT THROW EXCEPTION!
        if peer_id in self.peers.keys():
            return 1
        

        #print(f'[{peer_id}] open connetion on {peer_address}:{TCP_PORT}')
        reader, writer = await asyncio.open_connection(peer_address, TCP_PORT)

        #print(f'[{peer_id}] send handshake')
        writer.write(f'{MSG_HANDSHAKE}{self.name}\n'.encode('ASCII'))
        await writer.drain()

        #print(f'[{peer_id}] wait for response')
        data = await reader.readline()
        msg = data.decode('ASCII').strip(' \n')

        #print(f'[{peer_id}] response received')
        if msg[0] != ANS_HANDSHAKE:
            return 2
        
        #self.peers[peer_id] = (reader, writer)
        self.latest_heartbeat[peer_id] = time.time()

        return (reader, writer)

    async def connect_to_peers(self):
        successes = 0
        peers = {}
        for peer_id, peer_address in self.peer_addresses.items():   
            #print(f"Attempting to connect to peer {peer_id}...", end="")
            try:
                res = await self.connect_to_peer(peer_id, peer_address)
            except Exception as e:
                #print(f" failed!")
                continue

            if res == 1:
                #print(' peer was already registered. Continuing...')
                continue
            elif res == 2:
                #print(" failed! Peer didn't answer handshake!")
                continue
            
            else:
                peers[peer_id] = res # res has (reader, writer) in case of success
                #print(" success!")
                successes += 1

        for peer_id, (reader, writer) in peers.items():
            self.peers[peer_id] = (reader, writer)
            asyncio.create_task(self.listen_to_peer(peer_id))

        if successes > 0:       
            await self.flood_all()


    #def update_stream_register(self, stream_id : str, root_provider_id : str, provider_id : str):


    async def listen_to_peer(self, peer_id):
        #print(f'listening to {peer_id}')
        reader, _ = self.peers[peer_id]
        try:
            while True:
                #print(f'wait for read {peer_id}')
                data = await reader.readline()
                if not data:
                    break

                msg = data.decode('ASCII').strip(' \n')

                await self.process_request(peer_id, msg)

        except Exception as e:
            print(e)
        finally:
            await self.catastrophy(peer_id)


    def pop_parent_backup(self, stream_id):
        if len(self.streams[stream_id]['backup']) == 1:
            (provider, info), = self.streams[stream_id]['backup'].items()
            if info['parent'] == True:
                self.streams[stream_id]['backup'].pop(provider)

    def remove_from_backup(self, stream_id, provider_id):
        self.streams[stream_id]['backup'].pop(provider_id)
    

    async def request_parent(self, stream_id, peer_id, peer = None):
        print(f'Requesting parent for stream {stream_id} from peer {peer_id}.')

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

    async def process_request(self, peer_id, message):
        _, writer = self.peers[peer_id]

        msg_type = message[0]

        if len(message) == 1:
            msg = ''
        else:
            msg = message[1:]
        
        #print(f"[{peer_id}]: {msg_type} , {msg}")

        if msg_type == MSG_HEARTBEAT:
            self.latest_heartbeat[peer_id] = time.time()
            #print(f"Heartbeat received from {peer_id}.")
            
        elif msg_type == REQ_STREAM:
            # setup node as requester to start stream
            print(f"Stream {msg} requested from {peer_id}.")
            
        elif msg_type == ANS_STREAM:
            # open UDP channel and start stream
            print(f"Stream {msg} provided by {peer_id}.")
            
        elif msg_type == UNPROVIDE_STREAM:
            #TODO: finish this !
            
            stream_list = msg.split(';')

            #step 1, gather all streams peer provides and backups with matching IDS

            #step 2, remove backups provided by peer and remove provided streams and switch to backups

            #step 3, notify changes (if stream provider changed!)

        elif msg_type == FLOOD_STREAM:
            print(f'FLOOD Received from {peer_id}')

            streams_raw = msg.split(';')
            altered_streams = []
            link_cost = 1 #temporary value!!!!!!, use metrics when i have it TODO

            for stream in streams_raw:
                stream_id, info = stream.split(':')
                stream_info = info.split(',')
                root_provider = stream_info[0]
                tree_version = stream_info[1]
                jumps = int(stream_info[2]) + 1
                heuristic = float(stream_info[3]) + link_cost
                #streams.append((stream_id, root_provider, tree_version, jumps, heuristic))
                if self.handle_flood(peer_id, stream_id, root_provider, tree_version, jumps, heuristic):
                    altered_streams.append((stream_id, root_provider))

            #flood necessary
            await self.flood(altered_streams)
        
        elif msg_type == REQ_PARENT:
            stream_id = msg
            #print(f'Parent requested from {peer_id} for {stream_id}.')

            if self.is_server:
                if stream_id in self.own_streams.keys():
                    writer.write(f'{ANS_PARENT}{stream_id};{self.name};0.0.0.0\n'.encode('ASCII'))
                    await writer.drain()
                    return

            if stream_id in self.streams.keys():
                #it exists!
                #note, since the stream is being provided, the parent provider is
                # GUARANTEED to be a peer. 
                info = self.streams[stream_id]['best']
                provider = info["provider"]
                provider_address = self.peer_addresses[provider]

                writer.write(f'{ANS_PARENT}{stream_id};{provider};{provider_address}\n'.encode('ASCII'))
                await writer.drain()
                
            else:
                writer.write(f'{MSG_ERROR}Provided stream_id is not known.\n'.encode('ASCII'))
                await writer.drain()

        elif msg_type == ANS_PARENT:
            fields = msg.split(';')

            if len(fields) != 3:
                print(f'SOMETHING WENT AWRY: {peer_id}: [{msg_type}] {msg}')
                return
            
            stream_id = fields[0]
            parent_id = fields[1]
            parent_address = fields[2]

            if stream_id in self.parent_requests:
                self.parent_requests.pop(self.parent_requests.index(stream_id))

                #TODO: Check if provider is root, if so, set 'root' field to true
                if parent_id == self.streams[stream_id]["best"]["provider"]:
                    print('THIS IS A ROOT PROVIDER, TODO')

                self.pop_parent_backup(stream_id)
                if not self.streams[stream_id]["backup"]:
                    backup = {
                        'parent': True,
                        'parent_ip': parent_address
                    }

                    self.streams[stream_id]["backup"][parent_id] = backup

            else:
                print(f"Request for parent of {stream_id} was either already satisfied or never existed.")    

        elif msg_type == MSG_SHUTDOWN:
            print(f'Shutdown from {peer_id}!')

            await self.handle_death(peer_id)

        elif msg_type == MSG_ERROR:
            print(f"Error received from {peer_id}: {msg}")

        else:
            print(f"Unknown message type {msg_type} from {peer_id}.")

    #return True if needs reflooding, False otherwise
    def handle_flood(self, peer_id, stream_id, root_id, tree_version, n_jumps, heuristic): #later on, pass on metrics
        stream_state = self.streams.get(stream_id, None)
        if not stream_state:
            #stream itself doesnt have registry
            self.streams[stream_id] = {
                "current_root": root_id,
                "provisions": {
                    root_id: {
                        "version": tree_version,
                        "best": {
                            "provider": peer_id,
                            "n_jumps": n_jumps,
                            "heuristic": heuristic
                            #metrics: {...}
                        },
                        "backup": {}
                    }
                }
            }
            return True

        provision_state = stream_state["provisions"].get(root_id, None)
        if not provision_state:
            #specific provision from root doesnt have registry
            provision = {
                "version": tree_version,
                "best": {
                    "provider": peer_id,
                    "n_jumps": n_jumps,
                    "heuristic": heuristic
                    #metrics: { ... }
                },
                "backup": {}
            }
            self.streams[stream_id]["provisions"][root_id] = provision
            self.choose_best_provision(stream_id)
            return True
        

        if provision_state["best"]["provider"] == peer_id:
            #best is already peer, update stuff and reflood
            best = {
                "provider": peer_id,
                "n_jumps": n_jumps,
                "heuristic": heuristic
                #metrics: { ... }
            }

            self.streams[stream_id]["provisions"][root_id]["best"] = best
            
            #TODO: Only trigger reflood in case of drastic change when I have metrics
            return True
        
        else:
            #best isnt peer.
            backup = {
                "parent": False,
                "parent_ip": None,
                "n_jumps": n_jumps,
                "heuristic": heuristic
                #metrics: { ... }
            }

            self.streams[stream_id]["provisions"][root_id]["backup"][peer_id] = backup
            return self.choose_best_path(stream_id, root_id)

    def choose_best_provision(self, stream_id):
        stream_state = self.streams.get(stream_id, None)
        if not stream_state:
            return
        
        best_heuristic = float("inf")
        best_root = stream_state["current_root"]
        for root_id, root_state in stream_state["provisions"].items():
            if root_state["best"]["heuristic"] < best_heuristic:
                best_heuristic = root_state["best"]["heuristic"]
                best_root = root_id

        self.streams[stream_id]["current_root"] = best_root

    def choose_best_path(self, stream_id, root_id):
        stream_state = self.streams.get(stream_id, None)
        if not stream_state:
            return False
        
        provision_state = stream_state["provisions"].get(root_id, None)
        if not provision_state:
            return False
        
        #BOTH OF THESE RETURNS ARE JUST BACKUP CHECKS AND SHOULD NEVER BE TRIGGERED IN NORMAL OPERATION

        best_provider = provision_state["best"]["provider"]
        best_heuristic = provision_state["best"]["heuristic"]

        for backup_provider, backup_state in provision_state["backup"].items():
            if backup_state["heuristic"] < best_heuristic:
                best_provider = backup_provider
                best_heuristic = backup_state["heuristic"]

        if best_provider != provision_state["best"]["provider"]:
            old_best = provision_state["best"]

            new_best = {
                "provider": best_provider,
                "n_jumps": provision_state["backup"][best_provider]["n_jumps"],
                "heuristic": best_heuristic
            }

            self.streams[stream_id]["provisions"][root_id]["backup"].pop(best_provider)
            self.streams[stream_id]["provisions"][root_id]["best"] = new_best

            self.streams[stream_id]["provisions"][root_id]["backup"][old_best["provider"]] = {
                "parent": False,
                "parent_ip": None,
                "n_jumps": old_best["n_jumps"],
                "heuristic": old_best["heuristic"]
            }

            return True
        
        return False


    """ async def flood(self, stream_ids, is_own=False):
        if not stream_ids:
            print('EMPTY FLOOD, RETURNING')
            return
        streams = {}
        print(f'Flood called with {stream_ids}')

        #setup !
        for stream_id in stream_ids:
            if is_own:
                best_provision = self.own_streams.get(stream_id, None)
                if best_provision is None:
                    print(f'Warning: attempting to flood own {stream_id} that does not exist. Ignoring...')
                    continue

                heuristic = 0
                provider = None

                streams[stream_id] = (heuristic, provider)

            else:
                stream = self.streams.get(stream_id, None)
                if not stream:
                    print(f'Warning: attempting to flood {stream_id} that does not exist. Ignoring...')

                best_provision = stream["best"]

                provider = best_provision["provider"]
                heuristic = best_provision["heuristic"]

                streams[stream_id] = (heuristic, provider)

        for peer_id in self.peers.keys():
            _, writer = self.peers[peer_id]

            to_flood = 0
            msg_str = f"{FLOOD_STREAM}"

            for stream_id in streams.keys():
                heuristic, provider = streams[stream_id]
                if self.is_server and is_own:
                    msg_str += f'{stream_id}:{heuristic};'
                    to_flood += 1
                elif peer_id != provider and peer_id not in self.streams[stream_id]["backup"].keys(): 
                    #dont send to backup providers!
                    msg_str += f'{stream_id}:{heuristic};'
                    to_flood += 1

            if to_flood == 0:
                # no streams to be flooded, no work to be done
                print(f'Flooding nothing to {peer_id}.')
                continue


            msg_str = msg_str[:-1] + '\n'
            print(f'Sending following message to {peer_id}: {msg_str}', end='')

            msg = msg_str.encode('ASCII')

            writer.write(msg)
            await writer.drain()
 """
    
    async def flood(self, items, is_own=False):
        #items is a list of (stream_id, root_provider_id) pairs
        if not items:
            return
        
        streams = {}

        for stream_id, root_id in items:
            if is_own:
                heuristic = 0
                n_jumps = 0
                version = self.current_version
                streams[(stream_id, self.name)] = (heuristic, n_jumps, version)
            else:
                stream_state = self.streams.get(stream_id, None)
                if not stream_state:
                    continue
                root_state = stream_state["provisions"].get(root_id, None)
                if not root_state:
                    continue

                heuristic = root_state["best"]["heuristic"]
                n_jumps = root_state["best"]["n_jumps"]
                version = root_state["version"]

                streams[(stream_id, root_id)] = (heuristic, n_jumps, version)

        if not streams:
            return
        
        for peer_id, (_, writer) in self.peers.items():
            msg = f'{FLOOD_STREAM}'
            to_flood = 0

            for (stream_id, root_id), (heuristic, n_jumps, version) in streams.items():
                self_provider = peer_id
                if not is_own:
                    self_provider = self.streams[stream_id]["provisions"][root_id]["best"]["provider"]

                if not is_own and peer_id == self_provider:
                    continue

                msg += f'{stream_id}:{root_id},{version},{n_jumps},{heuristic};'
                to_flood += 1

            if to_flood == 0:
                continue

            msg = msg[:-1] + '\n'
            writer.write(msg.encode('ASCII'))
            await writer.drain()

    async def flood_all(self):
        items = []
        if self.is_server:
            for stream_id in self.own_streams.keys():
                items.append((stream_id, None))
            await self.flood(items, True)

        items = []
        for stream_id, stream_state in self.streams.items():
            for root_id in stream_state["providers"].keys():
                items.append(stream_id, root_id)

        await self.flood(items)



    async def handle_connection(self, reader, writer):
        #print('CONNECTION!')

        handshake_raw = await reader.readline()
        handshake = handshake_raw.decode('ASCII').strip(' \n')

        if handshake[0] != MSG_HANDSHAKE:
            #print('not handshake!')
            writer.write(MSG_ERROR.encode('ASCII') + b'\n')
            await writer.drain()
            return

        else:
            peer_id = handshake[1:]
            #print(f'[{peer_id}] is handshake, sending response')

            writer.write(ANS_HANDSHAKE.encode('ASCII') + b'\n')
            await writer.drain()

            #print(f'[{peer_id}] sent')

            #print(f'Received handshake from {peer_id}.')

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
                        await self.request_parent(stream_id, peer_id=peer_id, peer=peer)
                    else:
                        await self.request_parent(stream_id, provider)


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

    #this version handles death of catastrophic proportions
    async def catastrophy(self, peer_id):
        print(f'Peer {peer_id} has died without notifying.')
        #TODO: handle sudden death 
        print(f'TODO')
        self.peers.pop(peer_id)
        #TODO: REMEMBER TO REMOVE ALL STREAMS THAT IT PROVIDED, ALONG WITH BACKUPS
        # if backup was parent, establish connection.
        # if root provider (server crashed) notify that stream is no longer being provided



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

        await self.connect_to_peers()

        self.server = await asyncio.start_server(self.handle_connection, self.host, TCP_PORT)
        # from now on, server is listening
        
        self._async_tasks.append(asyncio.create_task(self.heartbeat_loop()))
        self._async_tasks.append(asyncio.create_task(self.check_heartbeats()))
        self._async_tasks.append(asyncio.create_task(self.menu()))

        async with self.server:
            await self.server.serve_forever()


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


    async def menu(self):
        #await asyncio.sleep(2)
        while True:
            # DISPLAY OPTIONS FIRST!!!!!!
            print('\n\n\n>==================================================<')
            print('1 - Display Streams')
            print('2 - Display Peers')
            print('3 - Shutdown')

            uinput = await async_input("\nChoice: ")
            try:
                choice = int(uinput)
            except:
                print(f'Invalid choice {uinput}')
                continue

            if choice == 1:
                self.print_streams()
            elif choice == 2:
                self.print_peers()
            elif choice == 3:
                print('Come back later :)')
            else:
                print(f'Invalid choice {uinput}')


#messy code to have asynchronous input!!!!!
#ignore

async def to_thread(func, /, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None, functools.partial(func, *args, **kwargs)
    )

async def async_input(prompt : str = ""):
    return await to_thread(input, prompt)


#back to normal :)

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
    
    
    
#START SCRIPT

"""

su - core \
cd ESR-TP2-2526 \
python3 src/node.py node1

"""