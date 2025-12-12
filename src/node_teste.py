# node_rtp_full.py
import asyncio
import time
import sys
import socket
import argparse
import json
import os

from RtpPacket import RtpPacket
from VideoStream import VideoStream

# -----------------------
# Global settings
# -----------------------
DEFAULT_HOST = '0.0.0.0'
TCP_PORT = 1234
UDP_PORT_START = 50000   # base para portas UDP locais
UDP_PORT = UDP_PORT_START

BOOTSTRAP_ADDR = '10.0.17.10'
BOOTSTRAP_PORT = 4321

HEARTBEAT_INTERVAL = 5
HEARTBEAT_TIMEOUT = HEARTBEAT_INTERVAL * 3

# Message types
MSG_ERROR = 'E'
MSG_HANDSHAKE = 'H'
ANS_HANDSHAKE = 'K'
MSG_HEARTBEAT = 'B'
FLOOD_STREAM = 'F'
REQ_STREAM = 'S'
ANS_STREAM = 'A'
MSG_METRIC = 'M'
TEARDOWN = 'T'


class Node:
    def __init__(self, name: str, host: str = DEFAULT_HOST,
                 bootstrap_addr: str = BOOTSTRAP_ADDR,
                 server_manifest_path: str = None):
        # identidade / endereço local
        self.name = name
        self.host = host
        self.bootstrap_addr = bootstrap_addr

        # Informação de peers
        # peer_addresses: node_id -> ip string
        self.peer_addresses = {}

        # peers: node_id -> (reader, writer)
        self.peers = {}
        # último heartbeat: node_id -> timestamp
        self.latest_heartbeat = {}

        # stream routing table:
        # stream_id -> {
        #    "provider": node_id or None,
        #    "n_jumps": int,
        #    "consumers": set(node_id, ...),
        #    "consumers_udp": { node_id: (ip, port), ... },
        #    "upstream_udp_port": int or None
        # }
        self.streams = {}

        # servidor de origem?
        self.is_server = server_manifest_path is not None
        self.sv_manifest_path = server_manifest_path
        # streams originadas aqui
        self.own_streams = {}

        # streams para as quais já pedimos upstream
        self.requested_upstream = set()

        # modo cliente (auto-request)
        self.is_client = False
        self.request_stream_on_join = None
        self.request_udp_port = None
        self._requested_once = False

        # TCP server asyncio
        self.server = None

    # -----------------------
    # Bootstrap: obter vizinhos
    # -----------------------
    def get_peers(self):
        """
        Contacta o bootstrapper (TCP simples) para obter lista de vizinhos.
        Retorno típico: "O2:10.0.0.2;O3:10.0.0.3;..."
        "%" -> node_name desconhecido
        "$" -> pedido para um node_name que não coincide com o cliente
        """
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.bootstrap_addr, BOOTSTRAP_PORT))
                s.sendall(self.name.encode('ascii'))
                data = s.recv(65536)
        except Exception as e:
            print(f"[{self.name}] Erro ao contactar bootstrapper: {e}")
            return 1

        data_str = data.decode('ascii')
        if data_str == '%':
            return 1
        if data_str == '$':
            return 2

        node_addr_str = [x for x in data_str.split(';') if x]
        neigh_addr_dict = {}
        for na_str in node_addr_str:
            parts = na_str.split(':')
            if len(parts) >= 2:
                node = parts[0]
                address = parts[1]
                neigh_addr_dict[node] = address

        if not neigh_addr_dict:
            return 3
        self.peer_addresses = neigh_addr_dict
        return 0

    # -----------------------
    # Heartbeat tasks
    # -----------------------
    async def heartbeat_loop(self):
        """Envia heartbeats periódicos para todos os peers TCP."""
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            for peer_id, (_, writer) in list(self.peers.items()):
                try:
                    writer.write((MSG_HEARTBEAT + '\n').encode('ASCII'))
                    await writer.drain()
                except Exception:
                    self.handle_death(peer_id)

    async def check_heartbeats(self):
        """Verifica timeouts de heartbeats e remove peers mortos."""
        while True:
            await asyncio.sleep(10)
            now = time.time()
            for peer_id, last in list(self.latest_heartbeat.items()):
                if now - last > HEARTBEAT_TIMEOUT:
                    self.handle_death(peer_id)

    # -----------------------
    # Conectar a peers & aceitar conexões
    # -----------------------
    async def connect_to_peers(self):
        """Inicia conexões TCP para os peers obtidos do bootstrap."""
        for peer_id, peer_address in list(self.peer_addresses.items()):
            if peer_id in self.peers:
                continue
            try:
                print(f"[{self.name}] Attempting to connect to peer {peer_id}...", end="")
                reader, writer = await asyncio.open_connection(peer_address, TCP_PORT)

                # handshake: H<node_name>\n
                writer.write(MSG_HANDSHAKE.encode('ASCII') + self.name.encode('ASCII') + b'\n')
                await writer.drain()

                data = await reader.readline()
                if not data:
                    print(" failed (no response).")
                    continue
                msg = data.decode('ASCII').strip()
                if not msg or msg[0] != ANS_HANDSHAKE:
                    print(" failed (bad handshake).")
                    continue

                self.peers[peer_id] = (reader, writer)
                self.latest_heartbeat[peer_id] = time.time()
                print(" success!")

                asyncio.create_task(self.listen_to_peer(peer_id))

            except Exception as e:
                print(" failed!", e)
                continue

        # Depois de estabelecer ligações, faz flood das streams conhecidas
        asyncio.create_task(self.flood_all())

    async def listen_to_peer(self, peer_id):
        """Loop de leitura para um peer TCP. Encaminha mensagens para process_request()."""
        if peer_id not in self.peers:
            return
        reader, writer = self.peers[peer_id]
        try:
            while True:
                data = await reader.readline()
                if not data:
                    break
                msg = data.decode('ASCII').strip()
                await self.process_request(peer_id, msg)
        except Exception as e:
            print(f"[{self.name}] listen_to_peer {peer_id} exception: {e}")
        finally:
            self.handle_death(peer_id)

    # -----------------------
    # Utility
    # -----------------------
    def get_free_udp_port(self):
        """Devolve uma porta UDP 'livre' (contador simples)."""
        global UDP_PORT
        UDP_PORT += 1
        return UDP_PORT

    # -----------------------
    # Processamento de mensagens, flooding, manifest
    # -----------------------
    async def process_request(self, peer_id, message):
        """
        Dispatcher para mensagens TCP recebidas dos peers.
        Mensagens: B (heartbeat), F (flood stream), S (REQ_STREAM), A (ANS_STREAM), ...
        """
        if not message:
            return
        msg_type = message[0]
        msg = message[1:]

        # HEARTBEAT
        if msg_type == MSG_HEARTBEAT:
            self.latest_heartbeat[peer_id] = time.time()
            return

        # FLOOD: F<stream_id>;<n_jumps>
        if msg_type == FLOOD_STREAM:
            parts = msg.split(';')
            if len(parts) < 2:
                return
            stream_id = parts[0]
            try:
                incoming_jumps = int(parts[1]) + 1
            except:
                return

            if stream_id in self.streams:
                stored = self.streams[stream_id]
                if incoming_jumps < stored.get("n_jumps", 9999):
                    stored["n_jumps"] = incoming_jumps
                    stored["provider"] = peer_id
                    self.streams[stream_id] = stored
                    await self.flood(stream_id)
            else:
                self.streams[stream_id] = {
                    "provider": peer_id,
                    "n_jumps": incoming_jumps,
                    "consumers": set(),
                    "consumers_udp": {},
                    "upstream_udp_port": None
                }
                await self.flood(stream_id)
            return

        # REQ_STREAM: S<stream_id>;<requester_udp_port>
        if msg_type == REQ_STREAM:
            parts = msg.split(';')
            if len(parts) < 2:
                return
            stream_id = parts[0]
            try:
                requester_udp_port = int(parts[1])   # sem +1, porta exata
                print(f"[{self.name}] Peer {peer_id} requested stream {stream_id} on UDP port {requester_udp_port}")
            except:
                return

            # garantir entrada do stream
            if stream_id not in self.streams:
                self.streams[stream_id] = {
                    "provider": peer_id,       # por agora, é quem nos falou deste stream
                    "n_jumps": 1,
                    "consumers": set(),
                    "consumers_udp": {},
                    "upstream_udp_port": None
                }

            # registar consumidor downstream
            self.streams[stream_id]["consumers"].add(peer_id)
            requester_ip = self.peer_addresses.get(peer_id)
            if not requester_ip:
                try:
                    sock = self.peers[peer_id][1].get_extra_info('socket')
                    if sock:
                        requester_ip = sock.getpeername()[0]
                        self.peer_addresses[peer_id] = requester_ip
                except Exception as e:
                    print(f"[{self.name}] Falha ao obter IP de {peer_id}: {e}")

            if requester_ip:
                self.streams[stream_id]["consumers_udp"][peer_id] = (requester_ip, requester_udp_port)
                print(f"[{self.name}] Registered consumer {peer_id} @ {requester_ip}:{requester_udp_port} for stream {stream_id}")

            # enviar ANS_STREAM (ACK) para o peer downstream
            try:
                r_reader, r_writer = self.peers[peer_id]
                r_writer.write((ANS_STREAM + stream_id + '\n').encode('ASCII'))
                await r_writer.drain()
            except Exception as e:
                print(f"[{self.name}] Falha ao enviar ANS_STREAM para {peer_id}: {e}")

            # se este nó for origem, inicia streaming
            if self.is_server and stream_id in self.own_streams:
                asyncio.create_task(self.stream_local_origin_to_consumers(stream_id))
            else:
                # nó intermédio: se ainda não está a receber upstream, comporta-se como cliente
                provider = self.streams[stream_id].get("provider")
                if provider and stream_id not in self.requested_upstream:
                    await self._start_upstream_from_provider(stream_id, provider)
            return

        # ANS_STREAM: A<stream_id> (apenas ACK, sem gerir portas)
        if msg_type == ANS_STREAM:
            stream_id = msg
            print(f"[{self.name}] ANS_STREAM recebido para {stream_id} de {peer_id} (ACK).")
            # não mexemos em UDP aqui; a porta já foi escolhida por quem pediu
            return

        # TEARDOWN: T<stream_id>
        if msg_type == TEARDOWN:
            stream_id = msg
            info = self.streams.get(stream_id)
            if not info:
                return

            print(f"[{self.name}] TEARDOWN recebido de {peer_id} para stream {stream_id}")

            # remover este peer das listas de consumidores
            if peer_id in info.get("consumers", set()):
                info["consumers"].discard(peer_id)
            if peer_id in info.get("consumers_udp", {}):
                info["consumers_udp"].pop(peer_id, None)

            # se, depois disto, este nó também não tiver mais consumidores downstream,
            # e não for origem nem cliente para esta stream, envia TEARDOWN a montante.
            if (not info.get("consumers_udp") and
                stream_id not in self.own_streams and
                not (self.is_client and self.request_stream_on_join == stream_id)):

                provider = info.get("provider")
                if provider and provider in self.peers:
                    _, pr_writer = self.peers[provider]
                    try:
                        pr_writer.write((TEARDOWN + stream_id + '\n').encode('ASCII'))
                        await pr_writer.drain()
                        info["upstream_udp_port"] = None
                        self.requested_upstream.discard(stream_id)
                        print(f"[{self.name}] Reencaminhou TEARDOWN de {stream_id} para provider {provider}")
                    except Exception as e:
                        print(f"[{self.name}] Falha ao reencaminhar TEARDOWN para {provider}: {e}")

            return


    async def _start_upstream_from_provider(self, stream_id: str, provider_id: str):
        """
        Nó intermédio (relay) a pedir stream ao provider:
        - escolhe porta local,
        - abre UDP socket nessa porta,
        - inicia receive_stream(),
        - envia REQ_STREAM <stream_id>;<porta> ao provider.
        """
        if provider_id not in self.peers:
            print(f"[{self.name}] Não há ligação TCP a provider {provider_id} para stream {stream_id}.")
            return

        # evitar pedidos duplicados
        if stream_id in self.requested_upstream:
            return

        udp_port = self.get_free_udp_port()
        try:
            udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            udp_sock.bind((self.host, udp_port))
        except Exception as e:
            print(f"[{self.name}] Falha ao abrir UDP {udp_port} (upstream) para stream {stream_id}: {e}")
            return

        # regista porta upstream
        self.streams[stream_id]["upstream_udp_port"] = udp_port
        print(f"[{self.name}] Relay a ouvir upstream de {provider_id} para {stream_id} em UDP {udp_port}")

        # começa já a receber RTP nessa porta
        asyncio.create_task(self.receive_stream(stream_id, udp_sock))

        # envia REQ_STREAM ao provider
        _, pr_writer = self.peers[provider_id]
        try:
            pr_writer.write((REQ_STREAM + stream_id + ';' + str(udp_port) + '\n').encode('ASCII'))
            await pr_writer.drain()
            self.requested_upstream.add(stream_id)
            print(f"[{self.name}] Relay pediu upstream de {provider_id} para stream '{stream_id}' (udp {udp_port})")
        except Exception as e:
            print(f"[{self.name}] Falha ao enviar REQ_STREAM a provider {provider_id} para {stream_id}: {e}")
            try:
                udp_sock.close()
            except:
                pass

    # -----------------------
    # Flooding helpers
    # -----------------------
    async def flood(self, stream_id, is_own=False):
        """
        Flood de anúncio de stream para todos os vizinhos excepto o provider.
        Formato: F<stream_id>;<n_jumps>
        """
        if is_own:
            n_jumps = 0
            provider = None
        else:
            info = self.streams.get(stream_id)
            if not info:
                return
            n_jumps = info.get("n_jumps", 0)
            provider = info.get("provider", None)

        for peer_id, (_, writer) in list(self.peers.items()):
            if peer_id == provider:
                continue
            try:
                writer.write((FLOOD_STREAM + stream_id + ';' + str(n_jumps) + '\n').encode('ASCII'))
                await writer.drain()
            except Exception:
                continue

    async def flood_all(self):
        """Envia flood para todas as streams conhecidas (próprias e aprendidas)."""
        for sid in list(self.streams.keys()):
            await self.flood(sid)
        if self.is_server:
            for sid in list(self.own_streams.keys()):
                await self.flood(sid, is_own=True)

    # -----------------------
    # Incoming TCP handler (server-side)
    # -----------------------
    async def handle_connection(self, reader, writer):
        """
        Chamado para cada nova conexão TCP de um peer.
        Primeira linha deve ser handshake: H<node_name>
        """
        try:
            handshake_raw = await reader.readline()
            handshake = handshake_raw.decode('ASCII').strip()
        except Exception:
            writer.write((MSG_ERROR + '\n').encode('ASCII'))
            await writer.drain()
            writer.close()
            return

        if not handshake or handshake[0] != MSG_HANDSHAKE:
            writer.write((MSG_ERROR + '\n').encode('ASCII'))
            await writer.drain()
            writer.close()
            return

        # responder ACK
        writer.write((ANS_HANDSHAKE + '\n').encode('ASCII'))
        await writer.drain()

        peer_id = handshake[1:]
        self.peers[peer_id] = (reader, writer)

        if peer_id not in self.peer_addresses:
            sock = writer.get_extra_info('socket')
            if sock:
                try:
                    peer_ip = sock.getpeername()[0]
                    self.peer_addresses[peer_id] = peer_ip
                except:
                    pass
        self.latest_heartbeat[peer_id] = time.time()

        await self.flood_all()
        asyncio.create_task(self.listen_to_peer(peer_id))

    def _schedule_teardown_if_leaf(self, stream_id: str):
            """
            Agenda (no event loop) uma verificação se este nó é um leaf sem consumidores
            para o stream_id, e se for o caso envia TEARDOWN ao provider.
            """
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                # não há loop ativo (deve ser durante shutdown), ignorar
                return

            loop.create_task(self._teardown_if_leaf(stream_id))

    async def _teardown_if_leaf(self, stream_id: str):
            """
            Se este nó não tiver consumidores downstream para stream_id,
            e não for nem origem nem cliente final desse stream,
            envia TEARDOWN ao provider e limpa estado upstream.
            """
            info = self.streams.get(stream_id)
            if not info:
                return

            # Se ainda há consumidores downstream, não fazemos nada.
            if info.get("consumers_udp"):
                return

            # Se este nó é a ORIGEM da stream, não faz sentido teardown upstream.
            if stream_id in self.own_streams:
                return

            # Se este nó é cliente direto desta stream, também não corta upstream.
            if self.is_client and self.request_stream_on_join == stream_id:
                return

            provider = info.get("provider")
            if not provider or provider not in self.peers:
                return

            _, writer = self.peers[provider]

            try:
                writer.write((TEARDOWN + stream_id + '\n').encode('ASCII'))
                await writer.drain()
                info["upstream_udp_port"] = None
                self.requested_upstream.discard(stream_id)
                print(f"[{self.name}] Enviou TEARDOWN de {stream_id} para {provider} (sem consumidores downstream).")
            except Exception as e:
                print(f"[{self.name}] Falha ao enviar TEARDOWN para {provider} ({stream_id}): {e}")

    # -----------------------
    # Peer removal
    # -----------------------
    def handle_death(self, peer_id):
        """
        Remove um peer de:
          - tabela de peers (TCP),
          - heartbeats,
          - streams onde era provider,
          - streams onde era consumidor (consumers / consumers_udp).

        Se, após remover o peer, algum stream ficar sem consumidores downstream
        neste nó, agenda TEARDOWN para o provider desse stream.
        """
        # remover da tabela de peers e fechar ligação TCP
        peer = self.peers.pop(peer_id, None)
        try:
            self.latest_heartbeat.pop(peer_id, None)
        except Exception:
            pass
        if peer:
            _, writer = peer
            try:
                writer.close()
            except:
                pass

        removed_provided = 0
        changed_sids = set()

        # varrer todos os streams conhecidos neste nó
        for sid, info in list(self.streams.items()):
            # 1) Se este peer era o provider deste stream, removemos o stream todo
            if info.get("provider") == peer_id:
                self.streams.pop(sid, None)
                removed_provided += 1
                continue  # próximo stream

            # 2) Se este peer era consumidor downstream, removê-lo das estruturas
            before_cons = len(info.get("consumers", set()))
            before_udp  = len(info.get("consumers_udp", {}))

            if peer_id in info.get("consumers", set()):
                info["consumers"].discard(peer_id)
            if peer_id in info.get("consumers_udp", {}):
                info["consumers_udp"].pop(peer_id, None)

            after_cons = len(info.get("consumers", set()))
            after_udp  = len(info.get("consumers_udp", {}))

            # se houve alteração como consumidor, marcamos este stream
            if (before_cons != after_cons) or (before_udp != after_udp):
                changed_sids.add(sid)

        # Para todos os streams onde houve alteração, verificar se este nó ficou leaf
        for sid in changed_sids:
            self._schedule_teardown_if_leaf(sid)

        print(
            f"[{self.name}] Closed connection with {peer_id}. "
            f"Streams removidos como provider: {removed_provided}, "
            f"streams alterados como consumer: {len(changed_sids)}."
        )



    # -----------------------
    # Manifest loader (server)
    # -----------------------
    def load_manifest(self):
        """
        Carrega manifest JSON:
        { "streams": { "videoA": {"file": "movie.Mjpeg"}, ... } }
        """
        if not self.is_server:
            return 1
        try:
            with open(self.sv_manifest_path, 'r') as f:
                data = json.load(f).get('streams', {})
        except Exception as e:
            print(f"[{self.name}] Error loading manifest: {e}")
            return 2

        if not data:
            return 2

        for sid, meta in data.items():
            self.own_streams[sid] = meta
            self.streams[sid] = {
                "provider": None,
                "n_jumps": 0,
                "consumers": set(),
                "consumers_udp": {},
                "upstream_udp_port": None
            }
        return 0

    # -----------------------
    # UDP receive/forward e origin streaming
    # -----------------------
    async def forward_rtp_packet(self, stream_id, packet_bytes, send_sock):
        info = self.streams.get(stream_id)
        if not info:
            return

        rtp = RtpPacket()
        rtp.decode(packet_bytes)
        seq = rtp.seqNum()

        consumers = info.get("consumers_udp", {})
        for peer_id, (cip, cport) in list(consumers.items()):
            try:
                send_sock.sendto(packet_bytes, (cip, cport))
                print(f"[ROUTER {self.name}] Reencaminhado seq={seq} para {peer_id} @ {cip}:{cport}")
            except Exception as e:
                print(f"[ROUTER {self.name}] Falha ao reenviar seq={seq} para {peer_id}: {e}")
                continue

    async def receive_stream(self, stream_id, udp_recv_sock):
        """
        Recebe pacotes RTP via UDP:
        - Reencaminha para downstreams (overlay).
        - Se for cliente, grava payload em 'cache-<seq>.jpg' (modelo Kurose).
        """
        print(f"[{self.name}] UDP recv socket ativo — recebendo stream {stream_id}...")

        loop = asyncio.get_event_loop()
        udp_send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        try:
            while True:
                try:
                    data, addr = await loop.run_in_executor(
                        None,
                        udp_recv_sock.recvfrom,
                        65536
                    )
                except Exception as e:
                    print(f"[{self.name}] recvfrom erro: {e}")
                    break

                if not data:
                    continue

                rtp = RtpPacket()
                try:
                    rtp.decode(data)
                except Exception as e:
                    print(f"[{self.name}] Pacote RTP inválido de {addr}: {e}")
                    continue

                payload = rtp.getPayload()
                seq = rtp.seqNum()

                print(f"[{self.name}] RTP seq={seq}, payload={len(payload)} bytes")

                # reencaminhar
                try:
                    await self.forward_rtp_packet(stream_id, data, udp_send_sock)
                except Exception as e:
                    print(f"[{self.name}] Falha reencaminhar RTP: {e}")

                # cliente: grava frame
                if self.is_client:
                    # diretório base: cache/<nome_do_nó>/<stream_id>/
                    base_dir = os.path.join("cache", self.name, stream_id)
                    os.makedirs(base_dir, exist_ok=True)

                    filename = os.path.join(base_dir, f"cache-{seq}.jpg")
                    try:
                        with open(filename, "wb") as f:
                            f.write(payload)
                    except Exception as e:
                        print(f"[{self.name}] Erro ao gravar frame {filename}: {e}")

                    # Limpeza automática de frames antigas (buffer circular de 500 frames)
                    old_seq = (seq - 500) & 0xFFFF
                    old_file = os.path.join(base_dir, f"cache-{old_seq}.jpg")
                    try:
                        if os.path.exists(old_file):
                            os.remove(old_file)
                    except Exception as e:
                        print(f"[{self.name}] Erro ao apagar {old_file}: {e}")



        finally:
            # limpeza sockets
            try:
                udp_recv_sock.close()
            except:
                pass
            try:
                udp_send_sock.close()
            except:
                pass

            # MUITO IMPORTANTE: permitir novo pedido upstream no futuro
            try:
                self.requested_upstream.discard(stream_id)
            except:
                pass

            info = self.streams.get(stream_id)
            if info:
                info["upstream_udp_port"] = None

            print(f"[{self.name}] receive_stream({stream_id}) terminou.")


    async def stream_local_origin_to_consumers(self, stream_id):
        """
        Servidor de origem:
        - Usa VideoStream(movie.Mjpeg) tal como no ZIP.
        - Envia as frames em RTP.
        - Faz loop do vídeo ENQUANTO houver pelo menos um consumidor downstream.
        - Se deixar de haver consumidores (por teardown, morte de clientes, etc.),
          pára o streaming para esse stream_id.
        """
        meta = self.own_streams.get(stream_id, {})
        filename = meta.get("file", "movie.Mjpeg")

        udp_send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        seq = 0
        ssrc = 12345  # arbitrary

        print(f"[{self.name}] Origin streaming {stream_id} a partir de '{filename}' (loop enquanto houver consumidores).")

        try:
            while True:
                # 1) Antes de começar (ou recomeçar) o vídeo, verificar se ainda há consumidores
                info = self.streams.get(stream_id)
                if not info or not info.get("consumers_udp"):
                    print(f"[{self.name}] Origin: stream {stream_id} sem consumidores -> parar streaming.")
                    break

                # 2) Abrir um novo VideoStream para uma passagem completa do ficheiro
                try:
                    vs = VideoStream(filename)
                except Exception as e:
                    print(f"[{self.name}] Falha ao abrir VideoStream('{filename}'): {e}")
                    break

                print(f"[{self.name}] Origin: início de nova passagem do vídeo '{filename}' para stream {stream_id}.")

                while True:
                    frame = vs.nextFrame()
                    if not frame:
                        # fim do ficheiro -> sair deste ciclo interno
                        print(f"[{self.name}] Fim do vídeo '{filename}' para stream {stream_id}.")
                        break

                    # atualizar info (consumers podem mudar a meio)
                    info = self.streams.get(stream_id)
                    consumers_udp = info.get("consumers_udp", {}) if info else {}

                    # Se entretanto os consumidores desapareceram, paramos no fim desta passagem
                    if not consumers_udp:
                        print(f"[{self.name}] Origin: consumidores desapareceram a meio da passagem de {stream_id}; "
                              f"não há mais envios para esta stream.")
                        break

                    # Empacotar frame em RTP
                    pkt = RtpPacket()
                    pkt.encode(
                        version=2,
                        padding=0,
                        extension=0,
                        cc=0,
                        seqnum=seq,
                        marker=0,
                        pt=26,
                        ssrc=ssrc,
                        payload=frame
                    )
                    data = pkt.getPacket()

                    # Enviar para todos os consumers atuais
                    for peer_id, (cip, cport) in list(consumers_udp.items()):
                        try:
                            udp_send_sock.sendto(data, (cip, cport))
                            # podes deixar o print comentado se for demasiado spammy
                            # print(f"[SERVER {self.name}] Enviado frame seq={seq} para {peer_id} @ {cip}:{cport}")
                        except Exception as e:
                            print(f"[SERVER {self.name}] Falha ao enviar frame seq={seq} para {peer_id}: {e}")

                    seq = (seq + 1) & 0xFFFF
                    await asyncio.sleep(1/25)

                # Aqui terminou UMA passagem pelo ficheiro.
                # O while True externo decide se volta a recomeçar ou termina.
                # Se não houver consumidores, o próximo ciclo externo faz break logo no início.

        finally:
            try:
                udp_send_sock.close()
            except:
                pass
            print(f"[{self.name}] Origin streaming {stream_id} terminado.")



    # -----------------------
    # Auto-request (cliente) e start
    # -----------------------
    async def auto_request_stream(self, delay: float = 1.0):
        """
        Nó iniciado com --client:
        - Espera ter peers.
        - Escolhe porta UDP local, abre socket, inicia receive_stream(),
        - Envia REQ_STREAM <stream_id>;<porta> ao primeiro peer.
        """
        await asyncio.sleep(delay)

        if not self.is_client:
            return

        if self._requested_once:
            return

        waited = 0.0
        while not self.peers and waited < 5.0:
            await asyncio.sleep(0.2)
            waited += 0.2

        if not self.peers:
            print(f"[{self.name}] Nenhum peer disponível para pedir stream.")
            return

        peer_id = next(iter(self.peers.keys()))
        try:
            reader, writer = self.peers[peer_id]
        except Exception:
            print(f"[{self.name}] Erro ao obter writer para peer {peer_id}")
            return

        stream_id = self.request_stream_on_join or "videoA"
        udp_port = self.request_udp_port if self.request_udp_port else self.get_free_udp_port()

        try:
            udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            udp_sock.bind((self.host, udp_port))
        except Exception as e:
            print(f"[{self.name}] Falha ao abrir UDP {udp_port}: {e}")
            return

        # começa já a receber nessa porta
        asyncio.create_task(self.receive_stream(stream_id, udp_sock))

        # envia REQ_STREAM ao peer
        try:
            writer.write((REQ_STREAM + stream_id + ';' + str(udp_port) + '\n').encode('ASCII'))
            await writer.drain()
            print(f"[{self.name}] AUTO-REQUEST: pediu stream '{stream_id}' ao peer {peer_id} (udp {udp_port})")
            self._requested_once = True
        except Exception as e:
            print(f"[{self.name}] Falha ao enviar REQ_STREAM ao peer {peer_id}: {e}")
            try:
                udp_sock.close()
            except:
                pass


    async def start(self):
        """Bootstrap -> TCP server -> connect_to_peers -> tarefas de fundo."""
        res = self.get_peers()
        if res == 1:
            print(f'{self.name} does not exist! Please use a valid node_name!')
            return 1
        if res == 2:
            print(f"{self.name} is not this node's name! Maybe you used wrong node_name?")
            return 1
        if res == 3:
            print('No neighbours exist... (check bootstrapper configuration)')
            return 1

        if self.is_server:
            r = self.load_manifest()
            if r == 1:
                print("Attempting to load manifest while not a server.")
                return 1
            if r == 2:
                print(f'Manifest at "{self.sv_manifest_path}" is invalid.')
                return 1

        try:
            self.server = await asyncio.start_server(self.handle_connection, self.host, TCP_PORT)
        except Exception as e:
            print(f"[{self.name}] Failed to start TCP server on {self.host}:{TCP_PORT}: {e}")
            return 1

        asyncio.create_task(self.connect_to_peers())
        asyncio.create_task(self.heartbeat_loop())
        asyncio.create_task(self.check_heartbeats())

        if self.is_client:
            asyncio.create_task(self.auto_request_stream(delay=1.0))

        async with self.server:
            await self.server.serve_forever()


# -----------------------
# CLI entry point
# -----------------------
def main():
    parser = argparse.ArgumentParser(description="Overlay Node with RTP forward (node_rtp_full.py)")
    parser.add_argument("node_name", help="identifier for this node (must match bootstrapper)")
    parser.add_argument("-b", "--bootstrapper", type=str, default=BOOTSTRAP_ADDR, help="bootstrapper IP")
    parser.add_argument("-a", "--address", type=str, default=DEFAULT_HOST, help="local bind address")
    parser.add_argument("-s", "--server", type=str, dest="manifest", help="manifest JSON file (makes this node a server)")
    parser.add_argument("--client", action="store_true", help="auto-request a stream on join (client mode)")
    parser.add_argument("--request-stream", type=str, default="videoA", help="stream id to request when client")
    parser.add_argument("--request-port", type=int, default=0, help="optional UDP port to request (0 = auto)")
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

    try:
        asyncio.run(node.start())
    except KeyboardInterrupt:
        print("Interrupted by user, exiting.")


if __name__ == "__main__":
    main()
