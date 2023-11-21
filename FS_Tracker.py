import socket
import threading

from FS_TrackProtocol import FS_TrackProtocol


class FS_Tracker:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.tracker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connected_nodes = {}
        self.current_sharing_files = {}  # mudar para {} se for necessario
        print(f"Servidor escutando em {self.host}: porta {self.port}")

    def start(self):
        self.tracker_socket.bind((self.host, self.port))
        self.tracker_socket.listen()

        while True:
            client_socket, address = self.tracker_socket.accept()
            print(f"Nodo conectado: {address[0]}:{address[1]}")

            node_handler = threading.Thread(target=self.handle_node_connection, args=(client_socket, address))
            node_handler.start()

    # FUNCAO RESPONSAVEL POR LIDAR COM AS MENSAGENS DO NODO
    def handle_node_connection(self, client_socket, address):
        node_id = f"{address[0]}:{address[1]}"
        shared_files = {}  # Files being shared

        try:
            while True:
                received_message = client_socket.recv(1024).decode()
                parsed_message = self.parse_message(received_message)

                if parsed_message["type"] == "REG":
                    node_info = parsed_message["node_info"]
                    self.connected_nodes[node_id] = node_info
                    print(f"Registered FS_Node: {node_info}")

                    # Check if shared_files present in node_info
                    if "shared_files" in node_info:
                        shared_files = node_info["shared_files"]
                        self.save_shared_files(node_id, shared_files)
                        self.update_shared_files(shared_files)

                elif parsed_message["type"] == "LIST":
                    # Send the list of files being shared
                    self.list_files_being_shared(client_socket)

        except ConnectionResetError:
            print(f"Connection closed with {address[0]}:{address[1]}")
            if node_id in self.connected_nodes:
                del self.connected_nodes[node_id]
            self.remove_shared_files(node_id)
            client_socket.close()

    # FUNCAO RESPONSAVEL POR CRIAR E ENVIAR A MENSAGEM DE LISTAGEM DE FICHEIROS A SER ENVIADOS
    def list_files_being_shared(self, client_socket):
        # Prepare message to send based on shared_files
        files_info = self.current_sharing_files if self.current_sharing_files else {}
        message = FS_TrackProtocol.create_list_send_message(files_info)
        client_socket.send(message.encode())

    # FUNCAO QUE DA UPDATE AOS FICHEIROS QUE ESTAO A SER PARTILHADOS
    # adiciona os "novos" ficheiros a lista de partilha
    def update_shared_files(self, shared_files):
        self.current_sharing_files.update(shared_files)

    def save_shared_files(self, node_id, shared_files):
        # Guarda os ficheiros que estão a ser shared e o numero de blocks
        self.connected_nodes[node_id]["shared_files"] = shared_files

    # FUNCAO PARA REMOVER FICHEIROS DA LISTA DE PARTILHA EM CASO DE CONEXAO FECHADA
    def remove_shared_files(self, node_id):
        if node_id in self.connected_nodes:
            shared_files = self.connected_nodes[node_id].get("shared_files", {})
            for file_name in shared_files.keys():
                if file_name in self.current_sharing_files:
                    del self.current_sharing_files[file_name]

    # FUNCAO QUE FAZ O PARSING DA MENSAGEM
    @staticmethod
    def parse_message(message):
        parts = message.split('|')
        message_type = parts[0]

        if message_type == "REG":
            node_info = {
                "address": parts[1],
                "port": int(parts[2]),
                "shared_files": {}  # Dictionary to store shared files
            }
            # Verifica se tem ficheiros a partilhar
            if len(parts) > 3:
                file_info = parts[3].split(',')
                for file_data in file_info:
                    # para verificar se tem file_name : numero blocos
                    file_components = file_data.split(':')
                    if len(file_components) == 2:
                        file_name, block_count = file_components
                        node_info["shared_files"][file_name] = int(block_count)

            return {"type": message_type, "node_info": node_info}

        elif message_type == "LIST":
            return {"type": message_type}

        else:
            return {"type": message_type, "data": message}


if __name__ == "__main__":
    host = '127.0.0.1'
    port = 9090
    tracker = FS_Tracker(host, port)
    tracker.start()
