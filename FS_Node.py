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


def get_local_ip():
    ip_address = ''
    try:
        # Create a temporary socket to get the local IP
        temp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        temp_socket.connect(("8.8.8.8", 80))  # Google's DNS server
        ip_address = temp_socket.getsockname()[0]
        temp_socket.close()
    except socket.error as e:
        print(f"Error occurred: {e}")
    return ip_address


class FS_Node:

    def __init__(self, address, server_address, port, folder_to_share):
        self.address = address
        self.server_address = server_address
        self.port = port
        self.folder_to_share = folder_to_share
        self.node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.shared_files = self.get_shared_files() if folder_to_share else {}  # caso de nao ter pasta
        self.received_blocks = {}

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
            self.node_socket.connect((self.server_address, 9090))
            print(f"Conectado ao FS_Tracker em {self.server_address}:{9090}")

            self.send_register_message()
            self.print_shared_files()

        except ConnectionRefusedError:
            print("Erro: Não foi possível conectar ao FS_Tracker.")

        while True:
            command = input("Enter a command (e.g., GET <file_name>, LIST, LOCATE): ")
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
            if command.startswith("EXIT"):
                self.send_exit_message()

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

    def send_exit_message(self):
        exit_message = FS_TrackProtocol.create_exit_message()
        self.node_socket.send(exit_message.encode())

    def send_update_to_tracker(self, file_name, block):
        update_message = FS_TrackProtocol.create_update_message(file_name, block, self.address)
        self.node_socket.send(update_message.encode())

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
        blocks_to_nodes, max_blocks = self.distribute_blocks(blocks_info)

        print(blocks_to_nodes, "BLOCOS TO NODES")
        for node, blocks in blocks_to_nodes.items():
            self.connect_and_request_blocks(node, blocks, file_name, max_blocks)
        return received_message

    def parse_get_response(self, response):
        print(response)
        blocks_info = {}
        file_name = None

        responses = response.split('|')[1:]

        for resp in responses:
            parts = resp.split('-')
            for part in parts:
                if 'with blocks' in part:
                    node_info = part.split(' with blocks ')[0]
                    blocks = [int(block) for block in part.split(' with blocks ')[1].split(',')]
                    blocks_info[node_info.split('-')[-1]] = blocks  # Extracting just the IP and node
                if not file_name:
                    file_name = part.split('-')[0].strip()  # Extracting the file name
        return blocks_info, file_name

    def distribute_blocks(self, nodes_blocks):
        # algoritmo - simple round-robin load balancing strategy
        nodes = list(nodes_blocks.keys())
        num_nodes = len(nodes)
        node_index = 0
        blocks_to_nodes = {}

        max_blocks = max([max(blocks) for blocks in nodes_blocks.values()]) + 1

        for file_block in range(max_blocks):
            # encontra o proximo nodo que tenha o bloco
            while file_block not in nodes_blocks[nodes[node_index]]:
                # passa ao proximo nodo
                node_index = (node_index + 1) % num_nodes
            # Atribuir este bloco a este nó
            if nodes[node_index] not in blocks_to_nodes:
                blocks_to_nodes[nodes[node_index]] = []
            blocks_to_nodes[nodes[node_index]].append(file_block)
            # passa ao proximo nodo
            node_index = (node_index + 1) % num_nodes

        print(blocks_to_nodes)
        return blocks_to_nodes, max_blocks

    def connect_and_request_blocks(self, node_info, blocks, file_name, max_blocks):
        peer_address, peer_port = node_info.split(':')

        # Attempting to connect to the peer
        try:
            # Connection to the peer
            peer_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

            # Send the request
            request_message = FS_TransferProtocol.create_request_message(file_name, blocks)
            print(request_message)
            peer_socket.sendto(request_message.encode(), (peer_address, 9090))

            # Receive blocks
            x = 0
            expected_block_count = len(blocks)

            if file_name not in self.received_blocks:
                self.received_blocks[file_name] = []

            while x < expected_block_count:
                block = peer_socket.recv(MTU)
                self.received_blocks[file_name].append(block)

                print(f"Received block {len(self.received_blocks[file_name]) - 1}")  # block received
                block_tag = int.from_bytes(block[:BLOCK_ID_SIZE], 'big')
                self.send_update_to_tracker(file_name, block_tag)
                x = x+1

                if file_name not in self.shared_files:
                    self.shared_files[file_name] = []
                self.shared_files[file_name].append(block_tag)

            # Process received blocks
            self.process_received_blocks(file_name, max_blocks)
            peer_socket.close()

        except ConnectionError as e:
            print(f"Connection failed: {e}")

    """def connect_and_request_blocks(self, node_info, blocks, file_name, max_blocks):
        peer_address, peer_port = node_info.split(':')

        # Attempting to connect to the peer
        try:
            # Connection to the peer
            peer_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

            # Send the request
            request_message = FS_TransferProtocol.create_request_message(file_name, blocks)
            print(request_message)
            peer_socket.sendto(request_message.encode(), (peer_address, 9090))
            
            # Receive blocks
            received_blocks = []
            expected_block_count = len(blocks)
            while len(received_blocks) < expected_block_count:
                block = peer_socket.recv(MTU)
                received_blocks.append(block)

                if file_name not in self.received_blocks:
                    self.received_blocks[file_name] = {}

                print(f"Received block {len(received_blocks) - 1}")  # block received
                block_tag = int.from_bytes(block[:BLOCK_ID_SIZE], 'big')
                self.send_update_to_tracker(file_name, block_tag)

                if file_name not in self.shared_files:
                    self.shared_files[file_name] = []
                self.shared_files[file_name].append(block_tag)

            # Process received blocks
            self.process_received_blocks(received_blocks, file_name, max_blocks)
            peer_socket.close()

        except ConnectionError as e:
            print(f"Connection failed: {e}")"""

    # FUNCOES QUE LIDAM COM AS CONEXÔES UDP
    def start_udp_listener(self):
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_socket.bind(('0.0.0.0', 9090))  # Binding to the node's address and port

        print(f"UDP listener started on {self.address}:{9090}")

        while True:
            data, addr = udp_socket.recvfrom(MTU)
            message = data.decode()
            # message = data.decode('utf-8')
            print(f"Received UDP message from {addr}: {message}")

            if message.startswith("REQUEST"):
                requested_file_name, requested_blocks = self.parse_request_message(message)
                print(requested_blocks, "blocos requested")
                self.send_requested_blocks(addr, requested_file_name, requested_blocks)

        # falta implementar quando fechar a udp socket
        udp_socket.close()

    def process_received_blocks(self, file_name, max_blocks):

        if len(self.received_blocks[file_name]) == max_blocks:
            # Sort blocks based on their tags (first 4 bytes)
            sorted_blocks = sorted(self.received_blocks[file_name], key=lambda block: block[:BLOCK_ID_SIZE])

            # Remove the tag
            block_data = [block[BLOCK_ID_SIZE:] for block in sorted_blocks]

            # Write the data to the file
            with open(f"{self.folder_to_share}/{file_name}", "wb") as file:
                for data in block_data:
                    file.write(data)

            # Optionally, you can clear the received_blocks list for this file
            self.received_blocks[file_name] = []


    """def process_received_blocks(self, received_blocks, file_name, max_blocks):
        # Check if we already have some blocks for this file
        #if file_name not in self.received_blocks:
        #    self.received_blocks[file_name] = {}

        # Process each received block
        for block in received_blocks:
            # Extract the block number and data
            block_number = int.from_bytes(block[:BLOCK_ID_SIZE], 'big')
            block_data = block[BLOCK_ID_SIZE:]

            # Store the block data in the dictionary
            self.received_blocks[file_name][block_number] = block_data

        # Check if we have all the blocks
        if len(self.received_blocks[file_name]) == max_blocks:
            # Create a sorted list of all the block numbers
            block_numbers = sorted(self.received_blocks[file_name].keys())

            # Open the file for writing
            with open(f"{self.folder_to_share}/{file_name}", "wb") as file:
                # Write each block to the file in order
                for block_number in block_numbers:
                    file.write(self.received_blocks[file_name][block_number])

            # Clear the blocks for this file
            del self.received_blocks[file_name]"""

    def parse_request_message(self, request_message):
        parametros = request_message.split('|')[1]  # Splitting to get 'logs.txt-0,1,2,3'
        file_name, blocks = parametros.split('-')  # Separating filename and block numbers
        requested_blocks = [int(block) for block in blocks.split(',')]  # Extracting block numbers as integers

        return file_name, requested_blocks

    def send_requested_blocks(self, requester_addr, file_name, blocks):
        # Para o caso de o ficheiro ainda esteja a ser downloaded
        if file_name in self.received_blocks:
            peer_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            for block_tag in blocks:
                block_tag_bytes = block_tag.to_bytes(BLOCK_ID_SIZE, 'big')
                for block in self.received_blocks[file_name]:
                    if block.startswith(block_tag_bytes):
                        peer_socket.sendto(block, requester_addr)
                        print(f"Sent block {block_tag_bytes} to {requester_addr}")
                        # Ensure a small delay between block transmissions to prevent packet loss
                        time.sleep(5)
                        break  # Move to the next requested block tag

            peer_socket.close()

        # Caso em que o nodo tem o ficheiro inteiro
        elif file_name in self.shared_files:
            complete_file_blocks = self.divide_file_into_blocks(self.folder_to_share + "/" + file_name)

            peer_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            for block_tag in blocks:
                block_tag_bytes = block_tag.to_bytes(BLOCK_ID_SIZE, 'big')
                for block in complete_file_blocks:
                    if block.startswith(block_tag_bytes):
                        peer_socket.sendto(block, requester_addr)
                        print(f"Sent block {block_tag_bytes} to {requester_addr}")
                        # Ensure a small delay between block transmissions to prevent packet loss
                        time.sleep(5)
                        break  # Move to the next requested block tag

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

    ip = get_local_ip()
    fs_node = FS_Node(ip, address, port, folder_to_share)

    udp_listener = threading.Thread(target=fs_node.start_udp_listener)
    udp_listener.start()

    tcp_listener = threading.Thread(target=fs_node.connect_to_tracker)
    tcp_listener.start()

    udp_listener.join()
    tcp_listener.join()

