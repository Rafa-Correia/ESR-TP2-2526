# stream.py
import socket
import threading
import time

class StreamSender:
    def __init__(self):
        self.destinations = []  # lista de tuplos (ip, port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def set_destinations(self, dest_list):
        """Define exatamente os destinos para este stream"""
        self.destinations = dest_list

    def send_frame(self, data):
        for (ip, port) in self.destinations:
            self.sock.sendto(data, (ip, port))

class StreamReceiver:
    def __init__(self, host, port, callback):
        self.host = host
        self.port = port
        self.callback = callback
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((host, port))
        self.running = False

    def start(self):
        self.running = True
        threading.Thread(target=self.receive_loop, daemon=True).start()

    def stop(self):
        self.running = False
        self.sock.close()

    def receive_loop(self):
        while self.running:
            try:
                data, addr = self.sock.recvfrom(65536)
                self.callback(data, addr)
            except:
                break

class StreamLoop:
    """Auxiliar para enviar frames periodicamente (ex.: vídeo frame a frame)"""
    def __init__(self, sender: StreamSender, frames: list, interval: float):
        self.sender = sender
        self.frames = frames
        self.interval = interval
        self.running = False

    def start(self):
        self.running = True
        threading.Thread(target=self.loop, daemon=True).start()

    def stop(self):
        self.running = False

    def loop(self):
        while self.running:
            for frame in self.frames:
                if not self.running:
                    break
                self.sender.send_frame(frame)
                time.sleep(self.interval)



##########################################################################
LATER ADD NO NODE.PY

# node_stream_helpers.py

from stream import StreamSender, StreamReceiver, StreamLoop
import time

# Dentro da classe Node, adiciona:

# 1. Callback para frames recebidos

def on_frame(self, data, addr):
    stream_id = self.identify_stream(data)  # supõe-se que há função para identificar o stream pelo payload
    if stream_id in self.streams:
        # envia apenas para os children que pediram
        child_destinations = [child.split(':') for child in self.streams[stream_id]['children']]
        self.sender.set_destinations([(ip, int(port)) for ip, port in child_destinations])
        self.sender.send_frame(data)


# 2. Receber pedido de stream de um cliente

def handle_req_stream(self, stream_id, requester):
    if self.is_server_node(stream_id):
        # sou o servidor: iniciar stream e ativar apenas para requester
        self.streams[stream_id]['active'] = True
        self.streams[stream_id]['children'] = [requester]
        self.handle_activate_stream(stream_id, requester)
    else:
        # não sou servidor: adiciona requester como child e propaga para parent
        if stream_id not in self.streams:
            self.streams[stream_id] = {'parent': self.parent_for_stream(stream_id), 'children': set(), 'active': False}
        self.streams[stream_id]['children'].add(requester)
        self.send_req_to_parent(stream_id)


# 3. Propagar ativação da stream para o filho que pediu

def handle_activate_stream(self, stream_id, child_id):
    self.streams[stream_id]['active'] = True
    self.sender.set_destinations([tuple(child_id.split(':'))])
    # enviar ACTIVATE apenas para child
    self.send_activate_msg(child_id, stream_id)


# 4. Iniciar stream no servidor

def start_stream(self, stream_id, frames, interval):
    loop = StreamLoop(self.sender, frames, interval)
    loop.start()
    print(f'Stream {stream_id} iniciada no servidor')
    return loop


# 5. Adicionar child que solicitou a stream

def add_child_requester(self, stream_id, child_id):
    if stream_id not in self.streams:
        self.streams[stream_id] = {'parent': None, 'children': set(), 'active': False}
    self.streams[stream_id]['children'].add(child_id)
