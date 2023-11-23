import os
import socket
import sys

from FS_TrackProtocol import FS_TrackProtocol  # FS_TrackProtocol

MTU = 1024  # restantes 4 bytes para numerar os blocos
BLOCK_ID_SIZE = 4

class FS_Node:

    def __init__(self, address, port, folder_to_share):
        self.address = address
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

    def divide_file_into_blocks(self, file_path):
        blocks = []
        block_tag = 0

        with open(file_path, 'rb') as file:
            while True:
                data = file.read(MTU - BLOCK_ID_SIZE)
                if not data:
                    break

                block = block_tag.to_bytes(BLOCK_ID_SIZE, 'big') + data
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
            self.node_socket.connect((self.address, self.port))
            print(f"Conectado ao FS_Tracker em {self.address}:{self.port}")

            self.send_register_message()
            self.print_shared_files()
            self.listen_for_commands()

        except ConnectionRefusedError:
            print("Erro: Não foi possível conectar ao FS_Tracker.")

    def connect_to_node(self):
        # falta ligar com o caso de ser ele a enviar ou ser ele a receber
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_socket.bind(('0.0.0.0', 9090))
        while True:
            data, addr = udp_socket.recvfrom(1024)


    # RESPONSAVEL POR OUVIR E LIDAR COM INPUTS DO UTILIZADOR
    def listen_for_commands(self):
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

                print(1)
                #envia a mensagem get

    # RESPONSAVEL POR LIDAR COM OS REQUESTS
    def listen_for_requests(self):
        print(1)

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
        received_message = self.node_socket.recv(1024).decode()
        print(f"{received_message}")

    def receive_locate_message(self):
        received_message = self.node_socket.recv(1024).decode()
        print(f"{received_message}")

    # FECHAR A CONEXAO
    def close_connection(self):
        self.node_socket.close()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python FS_Node.py <Address> <Port> [Folder_To_Share]")
        sys.exit(1)

    address = sys.argv[1]
    port = int(sys.argv[2])
    folder_to_share = sys.argv[3] if len(sys.argv) >= 4 else None

    if folder_to_share and not os.path.isdir(folder_to_share):
        print("Error: The specified folder does not exist.")
        sys.exit(1)

    fs_node = FS_Node(address, port, folder_to_share)
    fs_node.connect_to_tracker()
