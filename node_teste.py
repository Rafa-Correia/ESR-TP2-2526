import asyncio
import time
import sys
import socket
import argparse
import json
import subprocess
import shlex

from RtpPacket import RtpPacket  # classe para pacotes RTP

# Global vars 
DEFAULT_HOST = '0.0.0.0'
TCP_PORT = 1234
UDP_PORT_START = 5000
UDP_PORT = UDP_PORT_START

BOOTSTRAP_ADDR = '10.0.17.10'
BOOTSTRAP_PORT = 4321

HEARTBEAT_INTERVAL = 5
HEARTBEAT_TIMEOUT  = HEARTBEAT_INTERVAL * 3

# Message definitions
MSG_ERROR = 'E'
MSG_HANDSHAKE = 'H'
ANS_HANDSHAKE = 'K'
MSG_HEARTBEAT = 'B'
FLOOD_STREAM = 'F'
REQ_STREAM = 'S'
ANS_STREAM = 'A'
MSG_METRIC = 'M'
METRIC_LATENCY = 'L'
METRIC_BANDWIDTH = 'W'
METRIC_LOSS = 'S'

class Node:
    def __init__(self, name : str, host : str = DEFAULT_HOST, bootstrap_addr : str = BOOTSTRAP_ADDR, server_manifest_path : str = None):
        self.name = name
        self.host = host
        self.bootstrap_addr = bootstrap_addr
        self.peer_addresses = {}
        self.peers = {}
        self.peers_udp = {}
        self.streams = {}
        self.latest_heartbeat = {}
        self.tcp_server = None
        self.udp_server = None

        # server vars
        self.is_server = server_manifest_path is not None
        self.sv_manifest_path = server_manifest_path
        self.own_streams = {}
        self.streams_udp_ports = {}

        # client vars
        self.is_client = False
        self.request_stream_on_join = None
        self.request_udp_port = None
        self._requested_once = False

    # ================================
    # Peer discovery & heartbeat
    # ================================
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
            for peer_id, (_, writer) in self.peers.items():
                try:
                    msg = MSG_HEARTBEAT + '\n'
                    writer.write(msg.encode('ASCII'))
                    await writer.drain()
                except:
                    self.handle_death(peer_id)

    async def check_heartbeats(self):
        while True:
            await asyncio.sleep(10)
            now = time.time()
            for peer_id, last in self.latest_heartbeat.items():
                if now - last > HEARTBEAT_TIMEOUT:
                    self.handle_death(peer_id)
            print(self.streams)
            if self.is_server:
                print(self.own_streams)

    # ================================
    # Peer connection & requests
    # ================================
    async def connect_to_peers(self):
        for peer_id, peer_address in self.peer_addresses.items():
            if peer_id in self.peers.keys():
                continue
            
            try:
                print(f"Attempting to connect to peer {peer_id}...", end="")
                reader, writer = await asyncio.open_connection(peer_address, TCP_PORT)
                writer.write(MSG_HANDSHAKE.encode('ASCII') + self.name.encode('ASCII') + b'\n')
                await writer.drain()
                data = await reader.readline()
                msg = data.decode('ASCII').strip(' \n')
                if msg[0] != ANS_HANDSHAKE:
                    print("Response to handshake wasn't ANS.")
                    continue
                self.peers[peer_id] = (reader, writer)
                self.latest_heartbeat[peer_id] = time.time()
            except:
                print(" failed!")
                continue

            print(" success!")

            if peer_id in self.peers.keys():
                asyncio.create_task(self.listen_to_peer(peer_id))

                # --- AUTO REQUEST STREAM PARA CLIENTES ---
                if self.is_client and not self._requested_once:
                    stream_id = self.request_stream_on_join or "videoA"
                    udp_port = self.request_udp_port if self.request_udp_port else self.get_free_udp_port()
                    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    try:
                        udp_sock.bind((self.host, udp_port))
                    except Exception as e:
                        print(f"[{self.name}] Erro ao abrir UDP {udp_port}: {e}")
                        udp_sock.close()
                    else:
                        try:
                            writer.write((REQ_STREAM + stream_id + ";" + str(udp_port) + "\n").encode("ASCII"))
                            await writer.drain()
                            print(f"[{self.name}] CLIENTE -> pediu stream '{stream_id}' ao peer {peer_id} (udp {udp_port})")
                            asyncio.create_task(self.receive_stream(stream_id, udp_sock))
                            self._requested_once = True
                        except Exception as e:
                            print(f"[{self.name}] Falha a enviar REQ_STREAM: {e}")
                            udp_sock.close()
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

    def get_free_udp_port(self):
        global UDP_PORT
        UDP_PORT += 1
        return UDP_PORT

    # ================================
    # Process incoming messages
    # ================================
    async def process_request(self, peer_id, message):
        msg_type = message[0]
        msg = message[1:]
        
        if msg_type == MSG_HEARTBEAT:
            self.latest_heartbeat[peer_id] = time.time()
            
        elif msg_type == REQ_STREAM:
            parts = msg.split(';')
            if len(parts) < 2:
                return
            stream_id = parts[0]
            try:
                requester_udp_port = int(parts[1])
            except:
                return

            if stream_id not in self.streams:
                self.streams[stream_id] = {
                    "provider": peer_id, 
                    "n_jumps": 1, 
                    "consumers": set(), 
                    "consumers_udp": {}, 
                    "listening_udp_port": None
                }

            self.streams[stream_id]["consumers"].add(peer_id)
            requester_ip = self.peer_addresses.get(peer_id)
            if requester_ip:
                self.streams[stream_id]["consumers_udp"][peer_id] = (requester_ip, requester_udp_port)

            reader, writer = self.peers[peer_id]
            writer.write((ANS_STREAM + stream_id + '\n').encode('ASCII'))
            await writer.drain()

            if self.is_server and stream_id in self.own_streams:
                asyncio.create_task(self.stream_local_origin_to_consumers(stream_id))
            else:
                provider = self.streams[stream_id].get("provider")
                if provider and stream_id not in getattr(self, "requested_upstream", set()):
                    listen_port = self.get_free_udp_port()
                    self.streams[stream_id]["listening_udp_port"] = listen_port
                    pr_reader, pr_writer = self.peers[provider]
                    pr_writer.write((REQ_STREAM + stream_id + ';' + str(listen_port) + '\n').encode('ASCII'))
                    await pr_writer.drain()
                    if not hasattr(self, "requested_upstream"):
                        self.requested_upstream = set()
                    self.requested_upstream.add(stream_id)

        elif msg_type == ANS_STREAM:
            stream_id = msg
            listen_port = self.get_free_udp_port()
            self.streams[stream_id]["listening_udp_port"] = listen_port
            udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            udp_sock.bind((self.host, listen_port))
            asyncio.create_task(self.receive_stream(stream_id, udp_sock))
            print(f"[{self.name}] Listening for stream {stream_id} on UDP port {listen_port}")

        elif msg_type == FLOOD_STREAM:
            msg_split = msg.split(';')
            stream_id = msg_split[0]
            n_jumps_incoming = int(msg_split[1]) + 1
            
            if stream_id in self.streams.keys():
                stream_info = self.streams[stream_id]
                n_jumps_stored = stream_info["n_jumps"]
                if n_jumps_incoming < n_jumps_stored:
                    stream_info["n_jumps"]  = n_jumps_incoming
                    self.streams[stream_id] = stream_info
                    await self.flood(stream_id)
            else:
                stream_info = {"n_jumps": n_jumps_incoming, "provider": peer_id, "consumers": set()}
                self.streams[stream_id] = stream_info
                await self.flood(stream_id)
        else:
            print(f"Unknown message type {msg_type} from {peer_id}.")

    async def flood(self, stream_id, is_own=False):
        if is_own:
            stream_info = self.own_streams[stream_id]
            n_jumps_stored = 0
            provider = None
            consumers = stream_info["consumers"]
        else:  
            stream_info = self.streams[stream_id]
            n_jumps_stored = stream_info["n_jumps"]
            provider = stream_info["provider"]
            consumers = stream_info["consumers"]
        
        for peer_id, (reader, writer) in self.peers.items():
            if peer_id == provider:
                continue
            consumers.add(peer_id)
            stream_info["consumers"] = consumers
            self.streams[stream_id] = stream_info
            msg_str = FLOOD_STREAM + stream_id + ';' +  str(n_jumps_stored) + '\n'
            msg = msg_str.encode('ASCII')
            writer.write(msg)
            await writer.drain()

    async def flood_all(self):
        for stream_id in self.streams.keys():
            await self.flood(stream_id)
        if self.is_server:
            for stream_id in self.own_streams.keys():
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
        peer = self.peers.pop(peer_id, None)
        self.latest_heartbeat.pop(peer_id, None)
        if peer:
            _, writer = peer
            writer.close()
            for stream_id, stream_info in list(self.streams.items()):
                if peer_id == stream_info["provider"]:
                    self.streams.pop(stream_id)
            print(f"Closed connection with {peer_id}.")

    # ================================
    # Manifest
    # ================================
    def load_manifest(self):
        if not self.is_server:
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

    # ================================
    # Streaming with ffmpeg/ffplay
    # ================================
    async def receive_stream(self, stream_id, udp_socket):
        print(f"[{self.name}] Receiving stream {stream_id} via UDP...")
        ffplay_proc = subprocess.Popen(
            ['ffplay', '-f', 'mjpeg', '-i', '-'],
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        while True:
            data, _ = await asyncio.get_event_loop().run_in_executor(None, udp_socket.recvfrom, 65536)
            rtp = RtpPacket()
            rtp.decode(data)
            payload = rtp.getPayload()
            try:
                ffplay_proc.stdin.write(payload)
                ffplay_proc.stdin.flush()
            except BrokenPipeError:
                print(f"[{self.name}] ffplay fechou.")
                break

    async def stream_local_origin_to_consumers(self, stream_id):
        meta = self.own_streams.get(stream_id, {})
        filename = meta.get("file", "movie.Mjpeg")
        cmd = f"ffmpeg -i {shlex.quote(filename)} -f mjpeg -q:v 5 pipe:1"
        ffmpeg_proc = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        seq = 0
        udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        while True:
            frame = ffmpeg_proc.stdout.read(65536)
            if not frame:
                break
            pkt = RtpPacket()
            pkt.encode(version=2, padding=0, extension=0, cc=0,
                       seqnum=seq, marker=0, pt=26, ssrc=12345, payload=frame)
            data = pkt.getPacket()
            for peer_id, (cip, cport) in self.streams[stream_id].get("consumers_udp", {}).items():
                udp_sock.sendto(data, (cip, cport))
            seq = (seq + 1) & 0xFFFF
            await asyncio.sleep(1/24)

    # ================================
    # Node start
    # ================================
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
        asyncio.create_task(self.connect_to_peers())
        asyncio.create_task(self.heartbeat_loop())
        asyncio.create_task(self.check_heartbeats())
        async with self.server:
            await self.server.serve_forever()

# ================================
# Main
# ================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("node_name")
    parser.add_argument("-b", "--bootstrapper", type=str, default=BOOTSTRAP_ADDR)
    parser.add_argument("-a", "--address", type=str, default=DEFAULT_HOST)
    parser.add_argument("-s", "--server", type=str, dest="manifest")
    parser.add_argument("--client", action="store_true", help="torna este nó um cliente que pede stream ao entrar")
    parser.add_argument("--request-stream", type=str, default="videoA", help="stream a pedir automaticamente")
    parser.add_argument("--request-port", type=int, default=0, help="porta UDP (0 = automático)")
    args = parser.parse_args()

    node_name = args.node_name
    bootstrap_addr = args.bootstrapper
    host = args.address
    manifest = args.manifest

    node = Node(name=node_name, host=host, bootstrap_addr=bootstrap_addr, server_manifest_path=manifest)
    node.is_client = args.client
    node.request_stream_on_join = args.request_stream
    if args.request_port > 0:
        node.request_udp_port = args.request_port
    
    asyncio.run(node.start())

if __name__ == "__main__":
    main()
