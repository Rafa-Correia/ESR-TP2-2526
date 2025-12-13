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

BOOTSTRAP_ADDR = '10.0.9.10' # placeholder, should always be passed as argument
BOOTSTRAP_PORT = 4321 #can be anything

HEARTBEAT_INTERVAL = 0.25
HEARTBEAT_TIMEOUT  = HEARTBEAT_INTERVAL * 5

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
#   stream_id:root_provider_id,parent_provider_id,version,n_jumps,heuristic,<metrics here>
#      str       str                str            int     int      float       ???

REQ_PARENT = 'P'               #  req. parent provider:    [P][STREAM_ID][ROOT_ID]
ANS_PARENT = 'R'               #  ans. parent provider:    [R][STREAM_ID][ROOT_ID][PARENT_NAME][PARENT_ADDRESS]

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

        self.link_metrics = {} # peer_id -> {metrics, link cost?}
        
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
        #                   'grand_parent': str     (provider's parent id)
        #                   'n_jumps': int,         (jumps from root to self, might be useful?)
        #                   'heuristic': float,     (local cost, which means provider cost + link cost) (COST IS FROM THIS NODE TO ROOT, NOT PARENT TO ROOT)
        #                   'metrics': {...}        (these are parent only, link costs are stored somewhere else)
        #               },
        #               'backup': {
        #                   'backup_provider': {
        #                       'grand_parent': str         (provider's parent id)
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
        self.listen_tasks = {} # peer_id -> task

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

            # Iterate over a snapshot to avoid concurrent mutation issues
            for peer_id, (reader, writer) in list(self.peers.items()):
                try:
                    print(f'attempting heartbeat to {peer_id} with reader {id(reader)}')
                    writer.write(f'{MSG_HEARTBEAT}\n'.encode('ASCII'))
                    await writer.drain()
                except Exception:
                    continue

    async def check_heartbeats(self):
        while True:
            await asyncio.sleep(1)  # arbitrary; tune later
            now = time.time()

            # iterate over a snapshot to avoid "dict changed size during iteration"
            for peer_id, last in list(self.latest_heartbeat.items()):
                if now - last > HEARTBEAT_TIMEOUT:
                    await self.catastrophy(peer_id)

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
                    if backup_state["parent"]:
                        print(f'\t\t\t| PARENT BACKUP ON {backup_id}:{backup_state["parent_ip"]}')
                    else:
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
                print("Connecting on connect to peers")
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
            self.listen_tasks[peer_id] = asyncio.create_task(self.listen_to_peer(peer_id))

        if successes > 0:       
            await self.flood_all()


    async def listen_to_peer(self, peer_id):
        reader, _ = self.peers[peer_id]
        print(f'listening to {peer_id} with reader {id(reader)}')
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
            return #maybe continue if dont want to kill process
        finally:
            #DO NOT DECLARE AS CATASTROPHY WHEN CHANNEL CLOSES, ONLY RETURN THE PROCESS
            #await self.catastrophy(peer_id)
            print(f'no longer listening to {peer_id}')
            return

    async def process_request(self, peer_id, message):
        
        # peer might already have been removed by a concurrent failure path
        peer = self.peers.get(peer_id)
        if not peer:
            return

        _, writer = peer

        msg_type = message[0]
        msg = '' if len(message) == 1 else message[1:]
        

        #if msg_type != MSG_HEARTBEAT:
        print(f'From {peer_id}: [{msg_type}] {msg}')

        if msg_type == MSG_HEARTBEAT:
            self.latest_heartbeat[peer_id] = time.time()
            return

        elif msg_type == REQ_STREAM:
            # A neighbour is asking us to provide/forward a stream.
            # Message format: [S][STREAM_ID]
            stream_id = msg.strip()
            if not stream_id:
                writer.write(f'{MSG_ERROR}REQ_STREAM missing stream_id.\n'.encode('ASCII'))
                await writer.drain()
                return

            print(f"Stream {stream_id} requested from {peer_id}.")

            # This codebase does not yet implement the UDP data-plane, but we should at least
            # respond deterministically so requesters do not hang.
            writer.write(f'{ANS_STREAM}{stream_id}\n'.encode('ASCII'))
            await writer.drain()

        elif msg_type == ANS_STREAM:
            # Stream provisioning acknowledgement (data-plane not implemented yet)
            stream_id = msg.strip()
            print(f"Stream {stream_id} provided by {peer_id}.")

        elif msg_type == UNPROVIDE_STREAM:
            # A neighbour indicates it will stop providing a stream (best effort handling).
            provisions = msg.split(';')
            unprovided = []

            for provision in provisions:
                provision_fields = provision.split(',')
                stream_id = provision_fields[0]
                root_id = provision_fields[1]

                #TODO: LOGIC
                #if completely out of options, signal unprovide onwards


            await self.unprovide(unprovided)

        elif msg_type == UNREQ_STREAM:
            # A neighbour no longer wants a stream from us.
            stream_id = msg.strip()
            print(f"Stream {stream_id} unrequested by {peer_id}.")
            # No-op for now (data-plane not implemented)

        elif msg_type == FLOOD_STREAM:
            streams_raw = [s for s in msg.split(';') if s]
            altered_streams = []
            link_cost = 1  # TODO: replace with real metric when available

            for stream in streams_raw:
                try:
                    stream_id, info = stream.split(':')
                    stream_info = info.split(',')
                    root_provider = stream_info[0]
                    parent_provider = stream_info[1]
                    tree_version = stream_info[2]
                    jumps = int(stream_info[3]) + 1
                    heuristic = float(stream_info[4])
                except Exception:
                    writer.write(f'{MSG_ERROR}Malformed FLOOD entry: {stream}\n'.encode('ASCII'))
                    await writer.drain()
                    return

                changed = await self.handle_flood(
                    peer_id,
                    stream_id,
                    root_provider,
                    parent_provider,
                    tree_version,
                    jumps,
                    heuristic,
                    link_cost
                )
                if changed:
                    altered_streams.append((stream_id, root_provider))

                if self.in_emergency(stream_id, root_provider) and not self.is_root(stream_id, root_provider):
                    await self.request_parent(stream_id, root_provider, peer_id)

            # Reflood only what changed
            await self.flood(altered_streams)

        elif msg_type == REQ_PARENT:
            message_fields = msg.split(';')
            if len(message_fields) != 2:
                writer.write(f'{MSG_ERROR}REQ_PARENT malformed.\n'.encode('ASCII'))
                await writer.drain()
                return

            stream_id, root_id = message_fields[0], message_fields[1]

            if self.is_server:
                if stream_id in self.own_streams.keys() and root_id == self.name:
                    writer.write(f'{ANS_PARENT}{stream_id};{self.name};{self.name};0.0.0.0\n'.encode('ASCII'))
                    await writer.drain()
                    return

            stream_state = self.streams.get(stream_id, None)
            if not stream_state:
                writer.write(f'{MSG_ERROR}No stream {stream_id} known.\n'.encode('ASCII'))
                await writer.drain()
                return

            provision_state = stream_state["provisions"].get(root_id)
            if not provision_state:
                writer.write(f'{MSG_ERROR}No provision {root_id} is known.\n'.encode('ASCII'))
                await writer.drain()
                return

            provider = provision_state["best"]["provider"]
            provider_ip = self.peer_addresses.get(provider)

            if not provider_ip:
                writer.write(f'{MSG_ERROR}No address known for provider {provider}.\n'.encode('ASCII'))
                await writer.drain()
                return

            # If the requesting peer is currently our best provider, return our own best backup as "parent"
            if peer_id == provision_state["best"]["provider"]:
                # choose our best backup provider (lowest heuristic)
                backups = provision_state.get('backup', {})
                candidates = [(pid, st) for pid, st in backups.items() if st.get('heuristic') not in (None, float('inf'))]
                if not candidates:
                    writer.write(f'{MSG_ERROR}No parent backup available.\n'.encode('ASCII'))
                    await writer.drain()
                    return
                parent_id = min(candidates, key=lambda x: x[1]['heuristic'])[0]
                parent_ip = self.peer_addresses.get(parent_id, '0.0.0.0')
                writer.write(f'{ANS_PARENT}{stream_id};{root_id};{parent_id};{parent_ip}\n'.encode('ASCII'))
                await writer.drain()
                return

            # Otherwise, return our best provider for that stream as "parent"
            writer.write(f'{ANS_PARENT}{stream_id};{root_id};{provider};{provider_ip}\n'.encode('ASCII'))
            await writer.drain()

        elif msg_type == ANS_PARENT:
            fields = msg.split(';')
            if len(fields) != 4:
                print(f'SOMETHING WENT AWRY: {peer_id}: [{msg_type}] {msg}')
                return

            stream_id, root_id, parent_id, parent_address = fields

            if (stream_id, root_id) in self.parent_requests:
                self.parent_requests.pop(self.parent_requests.index((stream_id, root_id)))

                # NOTE: pop_parent_request() actually removes "parent backup" placeholders; keep this behaviour
                # but do it intentionally.
                self.pop_parent_request(stream_id, root_id)
                backup = {
                    'parent': True,
                    'parent_ip': parent_address,
                    'heuristic': None
                }
                self.streams[stream_id]["provisions"][root_id]["backup"][parent_id] = backup
            else:
                print(f"Request for parent of {stream_id} was either already satisfied or never existed.")

        elif msg_type == MSG_SHUTDOWN:
            # Peer intends to leave gracefully. Acknowledge and then clean up our state.
            print(f'Shutdown from {peer_id}!')
            try:
                writer.write(f'{MSG_FIN}\n'.encode('ASCII'))
                await writer.drain()
            except Exception:
                pass
            await self.handle_death(peer_id)

        elif msg_type == MSG_FIN:
            # Shutdown acknowledgement. Used by shutdown() to avoid blocking indefinitely.
            acks = getattr(self, "_shutdown_acks", None)
            if acks is not None:
                acks.add(peer_id)

        elif msg_type == MSG_ERROR:
            print(f"Error received from {peer_id}: {msg}")

        else:
            print(f"Unknown message type {msg_type} from {peer_id}.")




    async def handle_flood(self, peer_id, stream_id, root_id, parent_id, tree_version, n_jumps, heuristic, link_cost): #later on, pass on metrics
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
                            "grand_parent": parent_id,
                            "n_jumps": n_jumps,
                            "heuristic": heuristic + link_cost
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
                    "grand_parent": parent_id,
                    "n_jumps": n_jumps,
                    "heuristic": heuristic + link_cost
                    #metrics: { ... }
                },
                "backup": {}
            }
            self.streams[stream_id]["provisions"][root_id] = provision
            self.choose_best_provision(stream_id)
            return True
        
        return_value = False
             
        if provision_state["best"]["provider"] == peer_id:
            #best is already peer, update stuff and reflood
            old_best = self.streams[stream_id]["provisions"][root_id]["best"]

            new_best = {
                "provider": peer_id,
                "grand_parent": parent_id,
                "n_jumps": n_jumps,
                "heuristic": heuristic + link_cost
                #metrics: { ... }
            }

            self.streams[stream_id]["provisions"][root_id]["best"] = new_best
            
            if old_best["grand_parent"] != new_best["grand_parent"]:
                if self.pop_parent_request(stream_id, root_id) and not self.is_root(stream_id, root_id):
                    await self.request_parent(stream_id, root_id, peer_id)
                

            #TODO: Only trigger reflood in case of drastic change when I have metrics
            #if change is greater than threshold, return true, else return whatever choose_best_path tells you
            return_value = True
            self.choose_best_path(stream_id, root_id)
            self.choose_best_provision(stream_id)
        
        else:
            #best isnt peer.
            backup = {
                "grand_parent": parent_id,
                "parent": False,
                "parent_ip": None,
                "n_jumps": n_jumps,
                "heuristic": heuristic + link_cost
                #metrics: { ... }
            }

            old_backup = self.streams[stream_id]["provisions"][root_id]["backup"].get(peer_id, None)
            if old_backup and not old_backup["parent"]:     
                if old_backup["grand_parent"] != backup["grand_parent"]:
                    if self.pop_parent_request(stream_id, root_id) and not self.is_root(stream_id, root_id):
                        await self.request_parent(stream_id, root_id, peer_id)

            self.streams[stream_id]["provisions"][root_id]["backup"][peer_id] = backup
            return_value = self.choose_best_path(stream_id, root_id)
            self.choose_best_provision(stream_id)
            
            if not self.only_has_grandparent(stream_id, root_id, parent_id):
                self.pop_parent_request(stream_id, root_id)
            
            
        #if parent id is in provision, check which is best.
        #if best from peer, remove parent request if exists
        #if not peer best, and more providers other than parent, just register peer
        #if not peer best, no more providers other than parent, then ask for parent (you are now responsible)
        if parent_id == provision_state["best"]["provider"]  or parent_id in provision_state["backup"].keys():
            if parent_id == provision_state["best"]["provider"]:
                if heuristic > provision_state["best"]["heuristic"]:
                    self.pop_parent_request(stream_id, root_id)
                
                elif heuristic < provision_state["best"]["heuristic"]:
                    if self.only_has_grandparent(stream_id, root_id, parent_id) and not self.is_root(stream_id, root_id):
                        await self.request_parent(stream_id, root_id, parent_id)
                        
                else:
                    if self.name < peer_id:
                        if self.only_has_grandparent(stream_id, root_id, parent_id) and not self.is_root(stream_id, root_id):
                            await self.request_parent(stream_id, root_id, parent_id)
                            
            else:
                backup = provision_state["backup"][parent_id]
                if heuristic > backup["heuristic"]:
                    self.pop_parent_request(stream_id, root_id)
                
                elif heuristic < backup["heuristic"]:
                    if self.only_has_grandparent(stream_id, root_id, parent_id) and not self.is_root(stream_id, root_id):
                        await self.request_parent(stream_id, root_id, parent_id)
                        
                else:
                    if self.name < peer_id:
                        if self.only_has_grandparent(stream_id, root_id, parent_id) and not self.is_root(stream_id, root_id):
                            await self.request_parent(stream_id, root_id, parent_id)        
    
        return return_value


    def in_emergency(self, stream_id, root_id):
        if not self.streams[stream_id]["provisions"][root_id]["backup"]:
            return True
        return False

    def only_parent(self, stream_id, root_id):
        if len(self.streams[stream_id]["provisions"][root_id]["backup"]) == 1:
            (provider_id, backup_state), = self.streams[stream_id]["provisions"][root_id]["backup"].items()
            return self.streams[stream_id]["provisions"][root_id]["backup"][provider_id]["parent"]
        return False

    def is_root(self, stream_id, root_id):
        stream_state = self.streams.get(stream_id, None)
        if not stream_state:
            return False
        
        provision_state = stream_state["provisions"].get(root_id, None)
        if not provision_state:
            return False
        
        return root_id == provision_state["best"]["provider"]

    async def request_parent(self, stream_id, root_id, peer_id, peer = None):
        #print(f'Requesting parent for stream {stream_id} from peer {peer_id}.')

        if peer:
            _, writer = peer
        else:
            _, writer = self.peers[peer_id]

        msg_str = f'{REQ_PARENT}{stream_id};{root_id}\n'
        msg = msg_str.encode('ASCII')

        if (stream_id, root_id) not in self.parent_requests:
            self.parent_requests.append((stream_id, root_id))

            writer.write(msg)
            await writer.drain()

    def remove_from_backup(self, peer_id, stream_id, root_id):
        self.streams[stream_id]["provisions"][root_id]["backup"].pop(peer_id, None)

    def pop_parent_request(self, stream_id, root_id):
        stream_state = self.streams.get(stream_id, None)
        if not stream_state:
            return False
        
        provision_state = stream_state["provisions"].get(root_id, None)
        if not provision_state:
            return False
        
        parent_backup = ""
        for provider, backup_state in provision_state["backup"].items():
            if backup_state["parent"]:
                parent_backup = provider
                break
            
        if parent_backup:
            if self.streams[stream_id]["provisions"][root_id]["backup"].pop(parent_backup, None):
                return True
            else:
                return False
            
        return False


    def only_has_grandparent(self, stream_id, root_id, grandparent_id):
        stream_state = self.streams.get(stream_id, None)
        if not stream_state:
            return False
        
        provision_state = stream_state["provisions"].get(root_id, None)
        if not provision_state:
            return False
        
        if grandparent_id != provision_state["best"]["grand_parent"]:
            return False
        
        for _, backup_state in provision_state["backup"].items():
            if grandparent_id != backup_state["grand_parent"]:
                return False
            
        return True

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
            if backup_state["parent"]:
                continue
            if backup_state["heuristic"] < best_heuristic:
                best_provider = backup_provider
                best_heuristic = backup_state["heuristic"]

        if best_provider != provision_state["best"]["provider"]:
            old_best = provision_state["best"]

            new_best = {
                "provider": best_provider,
                "grand_parent": provision_state["backup"][best_provider]["grand_parent"],
                "n_jumps": provision_state["backup"][best_provider]["n_jumps"],
                "heuristic": best_heuristic
            }

            self.streams[stream_id]["provisions"][root_id]["backup"].pop(best_provider, None)
            self.streams[stream_id]["provisions"][root_id]["best"] = new_best

            #only reinsert if it is valid
            if old_best["heuristic"] != float("inf"):
                self.streams[stream_id]["provisions"][root_id]["backup"][old_best["provider"]] = {
                    "grand_parent": old_best["grand_parent"],
                    "parent": False,
                    "parent_ip": None,
                    "n_jumps": old_best["n_jumps"],
                    "heuristic": old_best["heuristic"]
                }

            return True
        
        return False




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
                parent = self.name
                streams[(stream_id, self.name)] = (heuristic, n_jumps, version, parent)
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
                parent = root_state["best"]["provider"]

                streams[(stream_id, root_id)] = (heuristic, n_jumps, version, parent)

        if not streams:
            return
        
        for peer_id, (_, writer) in self.peers.items():
            try:
                msg = f'{FLOOD_STREAM}'
                to_flood = 0

                for (stream_id, root_id), (heuristic, n_jumps, version, parent) in streams.items():
                    self_provider = peer_id
                    if not is_own:
                        self_provider = self.streams[stream_id]["provisions"][root_id]["best"]["provider"]

                    if not is_own and peer_id == self_provider:
                        continue

                    msg += f'{stream_id}:{root_id},{parent},{version},{n_jumps},{heuristic};'
                    to_flood += 1

                if to_flood == 0:
                    continue

                msg = msg[:-1] + '\n'
                writer.write(msg.encode('ASCII'))
                await writer.drain()
            except:
                continue

    async def flood_all(self):
        print(f"Flood all called with current peers: {self.peers.keys()}")
        items = []
        if self.is_server:
            for stream_id in self.own_streams.keys():
                items.append((stream_id, None))
            await self.flood(items, True)

        items = []
        for stream_id, stream_state in self.streams.items():
            for root_id in stream_state["provisions"].keys():
                items.append((stream_id, root_id))

        await self.flood(items)

    async def unprovide(self, items):
        msg = f'{UNPROVIDE_STREAM}'

        for stream_id, root_id in items:
            msg += f'{stream_id}:{root_id};'
        msg = msg[:-1] + '\n'

        for _, (_, writer) in self.peers.items():
            try:
                writer.write(msg.encode('ASCII'))
                await writer.drain()
            except:
                continue



    async def handle_connection(self, reader, writer):
        print('Connection received:', end='')

        handshake_raw = await reader.readline()
        handshake = handshake_raw.decode('ASCII').strip(' \n')

        if handshake[0] != MSG_HANDSHAKE:
            print(' not handshake!')
            writer.write(MSG_ERROR.encode('ASCII') + b'\n')
            await writer.drain()
            return

        else:
            peer_id = handshake[1:]
            print(f'[{peer_id}] is handshake, sending response...')

            writer.write(ANS_HANDSHAKE.encode('ASCII') + b'\n')
            await writer.drain()

            #print(f'[{peer_id}] sent')

            #print(f'Received handshake from {peer_id}.')

            self.peers[peer_id] = (reader, writer)
            self.latest_heartbeat[peer_id] = time.time()
            
            await self.flood_all()

            self.listen_tasks[peer_id] = asyncio.create_task(self.listen_to_peer(peer_id))

    #this version handles death when peer sends a shutdown signal
    async def handle_death(self, peer_id, catastrophy = False):
        if catastrophy:
            print(f'Peer {peer_id} has failed catastrophically.')

        peer = self.peers.pop(peer_id, None)
        self.latest_heartbeat.pop(peer_id, None)
        self.peer_addresses.pop(peer_id, None)

        if peer:
            #steps:
            #1 - gather all streams peer either directly provides or serves as backup.
            #2 - pop all backups.
            #3 - remove direct provision and promote best backup
            
            # if any of these steps leaves a stream in an emergency state, request parent
            # if any of these steps had parent backup only, be sure to establish connection.

            unprovided = []

            for stream_id, stream_state in self.streams.items():
                for root_id, provision_state in stream_state["provisions"].items():

                    #yield cpu here
                    await asyncio.sleep(0)

                    #three cases:
                    # 1 - it is a direct provider
                    # 2 - it is a backup
                    # 3 - neither

                    #1
                    if peer_id == provision_state["best"]["provider"]:
                        #this will force switch between current and best backup
                        #UNLESS ONLY BACKUP IS PARENT BACKUP
                        # TODO: CHECK THIS CONDITION!!!!
                        # if condition is true then do the following
                        if self.is_root(stream_id, root_id):
                            backup = provision_state["backup"]
                            if not backup:
                                unprovided.append((stream_id, root_id))
                                continue

                        if self.only_parent(stream_id, root_id):
                            backup = provision_state["backup"]
                            (parent_id, backup_state), = backup.items()
                            
                            if parent_id == peer_id:
                                print(f'BASICALLY; JUST TRIGGER UNPROVIDE OF STREAMS THAT SERVER PROVIDES')
                                print(f'TODO')
                                return
                            
                            if parent_id not in self.peer_addresses.keys():
                                self.peer_addresses[parent_id] = backup_state["parent_ip"]
                            if parent_id not in self.peers.keys():
                                print(f'Attempting to connect to {parent_id}:{backup_state["parent_ip"]}')
                                reader, writer = await self.connect_to_peer(parent_id, backup_state["parent_ip"])
                                self.peers[parent_id] = (reader, writer)
                                self.listen_tasks[parent_id] = asyncio.create_task(self.listen_to_peer(parent_id))
                        
                        # after this, the parent node will advertise it's streams, so keeping the heuristic high is still valid

                        # hmmm, how to delete then? keep inf and note that inf means invalid????

                        self.streams[stream_id]["provisions"][root_id]["best"]["heuristic"] = float("inf")



                        self.choose_best_path(stream_id, root_id)
                        self.choose_best_provision(stream_id)

                        

                    #1 and 2
                    self.remove_from_backup(peer_id, stream_id, root_id)

                    # here we need to check if stream is left in emergency and ask for parent if so
                    if self.in_emergency(stream_id, root_id) and not self.is_root(stream_id, root_id):
                        current_provider = self.streams[stream_id]["provisions"][root_id]["best"]["provider"]
                        await self.request_parent(stream_id, root_id, current_provider)
                
                self.choose_best_provision(stream_id)

            #await self.unprovide(unprovided)

            _, writer = peer
            writer.close()
            await writer.wait_closed()

            task = self.listen_tasks.get(peer_id, None)
            if task:
                task.cancel()

            print(f"Closed connection with {peer_id}.")

    #this version handles death of catastrophic proportions
    async def catastrophy(self, peer_id):
        # yeah, the logic is the same, so just
        # call the other one
        await self.handle_death(peer_id, True)



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
        """
        Graceful shutdown:
        - Send MSG_SHUTDOWN to all peers
        - Wait briefly for MSG_FIN acknowledgements (best effort)
        - Close transports and stop background tasks
        """
        print('Shutting down...', end='')

        # Best-effort coordination state (created here to keep __init__ unchanged)
        self._shutdown_acks = set()

        peers_snapshot = list(self.peers.items())

        # Send shutdown notice
        for peer_id, (_, writer) in peers_snapshot:
            try:
                writer.write(f"{MSG_SHUTDOWN}\n".encode('ASCII'))
                await writer.drain()
            except Exception:
                # Peer may already be gone
                pass

        # Wait for FIN acknowledgements (do not block indefinitely)
        deadline = time.time() + 2.0
        while time.time() < deadline and len(self._shutdown_acks) < len(peers_snapshot):
            await asyncio.sleep(0.05)

        # Close connections
        for peer_id, (_, writer) in peers_snapshot:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                continue

        # Cancel background tasks (listener loops, heartbeat loops, etc.)
        for t in self._async_tasks:
            try:
                t.cancel()
            except Exception:
                continue

        for _, task in self.listen_tasks.items():
            try:
                task.cancel()
            except:
                continue

        print(' done!')

    async def menu(self):
        while True:
            print('\n\n\n>==================================================<')
            print('1 - Display Streams')
            print('2 - Display Peers')
            print('3 - Shutdown')

            uinput = await async_input("\nChoice: ")

            try:
                choice = int(uinput)
            except Exception:
                print(f'Invalid choice {uinput}')
                continue

            if choice == 1:
                self.print_streams()
            elif choice == 2:
                self.print_peers()
            elif choice == 3:
                await self.shutdown()
                return
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