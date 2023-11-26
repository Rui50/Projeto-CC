import socket
import threading

from FS_TrackProtocol import FS_TrackProtocol


# transforma uma string num dicionario
def string_to_dict(input_string):
    pairs = input_string.split('/')

    dicionario = {}

    # Iterating through each pair in the list
    for pair in pairs:
        # Splitting each pair by colon to separate key and value
        parts = pair.split(':')

        # Checking if the pair has both key and value
        if len(parts) == 2:
            key, value = parts
            # Processing the key
            key = key.strip().strip('"')  # Removing spaces and quotes from the key

            # Processing the value
            if value == '[]':
                dicionario[key] = []
            else:
                # Removing brackets from the value and converting it to a list
                value = value.strip().strip('[]')
                value = [int(num) for num in value.split(',') if num]  # Converting to integers
                dicionario[key] = value

    return dicionario


class FS_Tracker:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.tracker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connected_nodes = {}  # guardar os nodos conetados
        self.current_sharing_files = {}  # vai guardar os ficheiros que estão a ser partilhados
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

                    if "shared_files" in node_info:
                        shared_files = node_info["shared_files"]
                        self.save_shared_files(node_id, shared_files)
                        self.update_shared_files(shared_files)

                elif parsed_message["type"] == "LIST":
                    # Send the list of files being shared
                    message = self.list_files_being_shared(client_socket)
                    client_socket.send(message.encode())

                elif parsed_message["type"] == "LOCATE":
                    file_name = parsed_message["file_name"]
                    message = self.get_file_details_from_node(file_name)
                    if message is None:
                        message = "file not found"
                    client_socket.send(message.encode())

                elif parsed_message["type"] == "GET":
                    file_name = parsed_message["file_name"]
                    blocks_info = self.get_blocks_for_file(file_name)
                    response_message = FS_TrackProtocol.create_get_response_message(blocks_info, file_name)
                    client_socket.send(response_message.encode())

        except ConnectionResetError:
            print(f"Connection closed with {address[0]}:{address[1]}")
            if node_id in self.connected_nodes:
                self.remove_currently_sharing(node_id)  # remove os seus ficheiros da lista de partilha
                del self.connected_nodes[node_id]  # remove o nodo da lista de nodos conectados
            client_socket.close()

    # FUNCAO RESPONSAVEL POR A MENSAGEM DE LISTAGEM DE FICHEIROS A SER ENVIADOS
    def list_files_being_shared(self, client_socket):
        # Prepare message to send based on shared_files
        files_info = self.current_sharing_files if self.current_sharing_files else {}
        message = FS_TrackProtocol.create_list_send_message(files_info)
        return message

    # FUNCAO QUE DA UPDATE AOS FICHEIROS QUE ESTAO A SER PARTILHADSO
    # adiciona os "novos" ficheiros a lista de partilha

    def update_shared_files(self, shared_files):
        self.current_sharing_files.update(shared_files)

    def save_shared_files(self, node_id, shared_files):
        # Guarda os ficheiros que estão a ser shared e o numero de blocks
        self.connected_nodes[node_id]["shared_files"] = shared_files

    # funcao temporaria (para apenas 1 fs_node com o ficheiro)
    def get_file_details_from_node(self, file_name):
        for nodo_id in self.connected_nodes:
            if file_name in self.connected_nodes[nodo_id]["shared_files"]:
                address = self.connected_nodes[nodo_id]["address"]
                port = self.connected_nodes[nodo_id]["port"]
                file = file_name
                blocks = self.connected_nodes[nodo_id]["shared_files"][file_name]
                message = FS_TrackProtocol.create_located_message(address, port, file, len(blocks))
                return message

    def get_blocks_for_file(self, file_name):
        blocks_info = {}

        # Iterate through connected nodes and gather block information
        for node_id, node_info in self.connected_nodes.items():
            if "shared_files" in node_info and file_name in node_info["shared_files"]:
                blocks_info[node_id] = node_info["shared_files"][file_name]

        return blocks_info

    # REMOVE FICHEIROS DE UM NODO DISCONECTADO DA LISTA DE CURRENTLY SHARING
    def remove_currently_sharing(self, node_id):
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
                "shared_files": {}  # Initialize an empty dictionary to store shared files
            }
            dicionario = string_to_dict(parts[3])
            node_info["shared_files"] = dicionario
            return {"type": message_type, "node_info": node_info}

        elif message_type == "LIST":
            return {"type": message_type}

        elif message_type == "GET":
            file_name = parts[1]
            return {"type": message_type, "file_name": file_name}

        elif message_type == "LOCATE":
            file_name = parts[1]
            return {"type": message_type, "file_name": file_name}

        else:
            return {"type": message_type, "data": message}


if __name__ == "__main__":
    host = '127.0.0.1'
    port = 9090
    tracker = FS_Tracker(host, port)
    tracker.start()
