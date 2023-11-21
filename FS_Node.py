import os
import socket
import sys

from FS_TrackProtocol import FS_TrackProtocol  # FS_TrackProtocol

MTU = 1020  # 4 bytes to number the block


class FS_Node:

    def __init__(self, address, port, folder_to_share):
        self.address = address
        self.port = port
        self.folder_to_share = folder_to_share
        self.node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.shared_files = self.get_shared_files()  # Retrieve the list of shared files with blocks

    # BLOCK RELATED FUNCTIONS
    def get_shared_files(self):
        shared_files = {}
        try:
            files = os.listdir(self.folder_to_share)
            for file in files:
                file_path = os.path.join(self.folder_to_share, file)
                if os.path.isfile(file_path):  # Check if it's a file (not a directory)
                    blocks_count = self.calculate_blocks_per_file(file_path)
                    shared_files[file] = blocks_count
        except FileNotFoundError:
            print("Error: The specified folder does not exist.")
            sys.exit(1)
        return shared_files

    # CALCULA QUANTOS BLOCOS UM FICHEIRO VAI TER
    # NAO EFETUA A DIVISAO EM BLOCKS EM SI
    def calculate_blocks_per_file(self, file_path):
        file_size = os.path.getsize(file_path)
        if file_size % MTU != 0:
            blocks = file_size // MTU + 1
        else:
            blocks = file_size // MTU
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

    # RESPONSAVEL POR OUVIR E LIDAR COM INPUTS DO UTILIZADOR
    def listen_for_commands(self):
        while True:
            command = input("Enter a command (e.g., GET <file_name>, LIST): ")
            if command.startswith("LIST"):
                self.send_list_message()    # envia a mensagem para listar os ficheiros
                self.receive_list_message() # lidar com a mensagem que recebe
            if command.startswith("GET"):
                file_name = command.split(" ")[1]  # Extract the file name from the command
                # self.send_get_message(file_name)

    # FUNCOES PARA ENVIAR E RECEBER AS MESSAGES
    def send_register_message(self):
        node_info = {
            "address": self.address,
            "port": 9090,  # Replace with actual port
            "files_info": self.get_shared_files_info()  # Include file info
        }
        register_message = FS_TrackProtocol.create_register_message(node_info)
        self.node_socket.send(register_message.encode())

    def send_list_message(self):
        list_message = FS_TrackProtocol.create_list_request_message()
        self.node_socket.send(list_message.encode())

    # LIDAR COM MENSAGENS RECEBIDAS DO TRACKER

    def receive_list_message(self):
        received_message = self.node_socket.recv(1024).decode()
        print(f"{received_message}")  # Print the shared files received from the tracker

    # FECHAR A CONEXAO
    def close_connection(self):
        self.node_socket.close()


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python FS_Node.py <Address> <Port> <Folder_To_Share>")
        sys.exit(1)

    address = sys.argv[1]
    port = int(sys.argv[2])
    folder_to_share = sys.argv[3]

    if not os.path.isdir(folder_to_share):
        print("Error: The specified folder does not exist.")
        sys.exit(1)

    fs_node = FS_Node(address, port, folder_to_share)
    fs_node.connect_to_tracker()
