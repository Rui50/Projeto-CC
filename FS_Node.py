import os
import socket
import sys
import re
import threading
import time

from FS_TransferProtocol import FS_TransferProtocol
from FS_TrackProtocol import FS_TrackProtocol  # FS_TrackProtocol

MTU = 1024  # restantes 4 bytes para numerar os blocos
BLOCK_ID_SIZE = 4


class FS_Node:

    def __init__(self, address, server_address, port, folder_to_share):
        self.address = address
        self.server_address = server_address
        self.port = port
        self.folder_to_share = folder_to_share
        self.node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.shared_files = self.get_shared_files() if folder_to_share else {}  # caso de nao ter pasta

    # a partir do folder pega nos ficheiros que vai partilhar
    def get_shared_files(self):
        shared_files = {}
        try:
            files = os.listdir(self.folder_to_share)
            for file in files:
                file_path = os.path.join(self.folder_to_share, file)
                if os.path.isfile(file_path):  # verifica se é um ficheiro
                    blocks = self.calculate_blocks_per_file(file_path)
                    shared_files[file] = blocks
        except FileNotFoundError:
            print("Error: The specified folder does not exist.")
            sys.exit(1)
        return shared_files

    # CALCULA QUANTOS BLOCOS UM FICHEIRO VAI TER
    # NAO EFETUA A DIVISAO EM BLOCKS EM SI
    def calculate_blocks_per_file(self, file_path):
        file_size = os.path.getsize(file_path)
        block_size = MTU - BLOCK_ID_SIZE
        numero_blocos = file_size // block_size
        resto = file_size % block_size

        blocks = []
        for block_id in range(numero_blocos):
            blocks.append(block_id)

        # adiciona o ultimo bloco se restar data
        if resto > 0:
            blocks.append(numero_blocos)

        return blocks

    # DIVIDE UM FICHEIRO EM BLOCOS
    # APENAS DEVE SER USADA QUANDO O FICHEIRO É INICIALMENTE PARTILHADO
    def divide_file_into_blocks(self, file_path):
        blocks = []
        block_tag = 0

        with open(file_path, 'rb') as file:
            while True:
                data = file.read(MTU - BLOCK_ID_SIZE)
                if not data:
                    break

                block = block_tag.to_bytes(BLOCK_ID_SIZE, 'big') + data
                # print(block)
                blocks.append(block)
                block_tag += 1
        return blocks

    def print_shared_files(self):
        for file_name, blocks_count in self.shared_files.items():
            print(f"File: {file_name}, Blocks: {blocks_count}")

    def get_shared_files_info(self):
        shared_files_info = {}
        for file_name, blocks in self.shared_files.items():
            num_blocks = blocks
            shared_files_info[file_name] = num_blocks
        return shared_files_info

    # CONNECTION

    def connect_to_tracker(self):
        try:
            self.node_socket.connect((self.server_address, self.port))
            print(f"Conectado ao FS_Tracker em {self.server_address}:{self.port}")

            self.send_register_message()
            self.print_shared_files()

        except ConnectionRefusedError:
            print("Erro: Não foi possível conectar ao FS_Tracker.")

        while True:
            command = input("Enter a command (e.g., GET <file_name>, LIST, LOCATE, EXIT): ")
            if command.startswith("LIST"):
                self.send_list_message()  # envia a mensagem para listar os ficheiros
                self.receive_list_message()  # lidar com a mensagem que recebe
            if command.startswith("LOCATE"):
                file_name = command.split(" ")[1]  # Extrai o nome do ficheiro do comando
                self.send_locate_message(file_name)
                self.receive_locate_message()
            if command.startswith("GET"):
                file_name = command.split(" ")[1]
                self.send_get_message(file_name)
                info = self.receive_get_message()
            # if command.startswith("EXIT"):
            #    self.close_connection()

    # FUNCOES PARA ENVIAR E RECEBER AS MESSAGES
    def send_register_message(self):
        node_info = {
            "address": self.address,
            "port": 9090,
            "files_info": self.get_shared_files_info()
        }
        register_message = FS_TrackProtocol.create_register_message(node_info)
        self.node_socket.send(register_message.encode())

    def send_list_message(self):
        list_message = FS_TrackProtocol.create_list_request_message()
        self.node_socket.send(list_message.encode())

    def send_locate_message(self, file_name):
        locate_message = FS_TrackProtocol.create_locate_message(file_name)
        self.node_socket.send(locate_message.encode())

    def send_get_message(self, file_name):
        get_message = FS_TrackProtocol.create_get_message(file_name)
        self.node_socket.send(get_message.encode())

    # LIDAR COM MENSAGENS RECEBIDAS DO TRACKER

    def receive_list_message(self):
        received_message = self.node_socket.recv(MTU).decode()
        print(f"{received_message}")

    def receive_locate_message(self):
        received_message = self.node_socket.recv(MTU).decode()
        print(f"{received_message}")

    def receive_get_message(self):
        received_message = self.node_socket.recv(MTU).decode()
        blocks_info, file_name = self.parse_get_response(received_message)
        self.decide_blocks_to_download(blocks_info, file_name)
        return received_message

    def parse_get_response(self, response):
        blocks_info = {}
        file_name = None

        responses = response.split('|')[1:]

        for resp in responses:
            parts = resp.split(' with blocks ')
            node_info = parts[0]
            blocks = [int(block) for block in parts[1].split(',')]
            blocks_info[node_info.split('-')[-1]] = blocks  # Extracting just the IP and node
            if not file_name:
                file_name = node_info.split('-')[0].strip()  # Extracting the file name
        print("FROM PARSE_GET_RESPONSE")
        print(blocks_info, "blocks_info")
        print(file_name, "file_name")
        return blocks_info, file_name

    def decide_blocks_to_download(self, blocks_info, file_name):
        max_blocks = max(len(blocks) for blocks in blocks_info.values())

        for node_info, blocks in blocks_info.items():
            if len(blocks) == max_blocks:
                self.connect_and_request_blocks(node_info, blocks, file_name)
                break

    def connect_and_request_blocks(self, node_info, blocks, file_name):
        print("FROM CONNECT AND REQUEST BLOCKS")
        print(node_info, "nodoinfo")
        print(blocks, "blocks")
        print(file_name, "filename")
        peer_address, peer_port = node_info.split(':')

        print(peer_address)
        print(peer_port)

        # Attempting to connect to the peer
        try:
            # Cria uma socket UDP
            peer_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

            request_message = FS_TransferProtocol.create_request_message(file_name, blocks)
            print(request_message)
            peer_socket.sendto(request_message.encode(), (peer_address, 9090))

            # Receive blocks
            received_blocks = []
            expected_block_count = len(blocks)
            while len(received_blocks) < expected_block_count:
                block = peer_socket.recv(MTU)
                if not block:
                    break
                received_blocks.append(block)
                print(f"Received block {len(received_blocks) - 1}")  # bloco recebido

            # Process received blocks
            self.process_received_blocks(received_blocks, file_name)
            peer_socket.close()

        except ConnectionError as e:
            print(f"Connection failed: {e}")

    # FUNCOES QUE LIDAM COM AS CONEXÔES UDP
    def start_udp_listener(self):
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_socket.bind(('0.0.0.0', self.port))  # Binding to the node's address and port

        print(f"UDP listener started on {self.address}:{self.port}")

        while True:
            data, addr = udp_socket.recvfrom(MTU)
            message = data.decode()
            # message = data.decode('utf-8')
            print(f"Received UDP message from {addr}: {message}")

            if message.startswith("REQUEST"):
                requested_file_name, requested_blocks = self.parse_request_message(message)
                print(requested_blocks, "blocos requested")
                print(requested_file_name, "file requested")
                self.send_requested_blocks(addr, requested_file_name, requested_blocks)

        # falta implementar quando fechar a udp socket
        udp_socket.close()

    def process_received_blocks(self, blocks, file_name):
        # sort a lista dos blocos pela tag
        sorted_blocks = sorted(blocks, key=lambda x: int.from_bytes(x[:BLOCK_ID_SIZE], 'big'))

        file_data = b''  # inicializar
        for block in sorted_blocks:
            file_data += block[BLOCK_ID_SIZE:]  # remove a tag

        with open(f"{file_name}", "wb") as file:
            file.write(file_data)

    def parse_request_message(self, request_message):
        parametros = request_message.split('|')[1]
        file_name, blocks = parametros.split('-')
        requested_blocks = [int(block) for block in blocks.split(',')]

        return file_name, requested_blocks

    def send_requested_blocks(self, requester_addr, file_name, blocks):
        if file_name in self.shared_files:  # Check if the node has the complete file
            complete_file_blocks = self.divide_file_into_blocks(self.folder_to_share + "/" + file_name)

            peer_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            for block_tag in blocks:
                block_tag_bytes = block_tag.to_bytes(BLOCK_ID_SIZE, 'big')
                for block in complete_file_blocks:
                    if block.startswith(block_tag_bytes):
                        peer_socket.sendto(block, requester_addr)
                        print(f"Sent block {block_tag} to {requester_addr}")

                        time.sleep(0.1)
                        break  # passar ao prox bloco

            peer_socket.close()
        else:
            print("Node does not have the requested file.")

    # FECHAR A CONEXAO
    def close_connection(self):
        self.node_socket.close()


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python FS_Node.py <Server-Address> <Port> [Folder_To_Share]")
        sys.exit(1)

    address = sys.argv[1]
    port = int(sys.argv[2])
    folder_to_share = sys.argv[3] if len(sys.argv) >= 4 else None

    if folder_to_share and not os.path.isdir(folder_to_share):
        print("Error: The specified folder does not exist.")
        sys.exit(1)

    host = socket.gethostname()
    fs_node = FS_Node(host, address, port, folder_to_share)

    # threads para udp e tcp
    udp_listener = threading.Thread(target=fs_node.start_udp_listener)
    udp_listener.start()

    tcp_listener = threading.Thread(target=fs_node.connect_to_tracker)
    tcp_listener.start()

    udp_listener.join()
    tcp_listener.join()
