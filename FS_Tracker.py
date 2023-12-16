import socket
import threading

from FS_TrackProtocol import FS_TrackProtocol


# transforma uma string num dicionario
def string_to_dict(input_string):
    pairs = input_string.split('/')

    dicionario = {}

    for pair in pairs:
        parts = pair.split(':')

        if len(parts) == 2:
            key, value = parts
            key = key.strip().strip('"')

            if value == '[]':
                dicionario[key] = []
            else:
                value = value.strip().strip('[]')
                value = [int(num) for num in value.split(',') if num]  # Converting to integers
                dicionario[key] = value

    return dicionario


class FS_Tracker:
    def __init__(self, host, port, host_name):
        self.host = host
        self.port = port
        self.host_name = host_name
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
        node_id,_,_ = socket.gethostbyaddr(address[0])
        print(node_id)
        #node_id = f"{address[0]}:{address[1]}"

        try:
            while True:
                received_message = client_socket.recv(1024).decode()
                if not received_message:
                    #print(f"Connection closed with {address[0]}:{address[1]}")
                    print(f"Connection closed with {node_id}")
                    if node_id in self.connected_nodes:
                        self.remove_files_when_disconnect(node_id)  # remove all files from sharedfiles
                        #self.remove_currently_sharing(node_id)  # remove os seus ficheiros da lista de partilha
                        del self.connected_nodes[node_id]  # remove o nodo da lista de nodos conectados
                        print(self.current_sharing_files)
                    break

                messages = received_message.split("\n")  # Split the data into separate messages
                for message in messages:
                    parsed_message = self.parse_message(message)

                    if parsed_message["type"] == "REG":
                        node_info = parsed_message["node_info"]
                        self.connected_nodes[node_id] = node_info
                        print(f"Registered FS_Node: {node_info}")

                        if "shared_files" in node_info:
                            shared_files = node_info["shared_files"]
                            self.save_shared_files(node_id, shared_files)
                            self.update_shared_files(shared_files)

                    elif parsed_message["type"] == "LIST":
                        # Envia a lista dos ficheiros que estão a ser partilhados
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

                    elif parsed_message["type"] == "UPDATE":
                        file_name = parsed_message["file_name"]
                        block_tag = parsed_message["block_tag"]
                        self.update_node(file_name, block_tag, node_id)
                        print(self.connected_nodes)

                    elif parsed_message["type"] == "EXIT":
                        # print(f"Connection closed with {address[0]}:{address[1]}")
                        print(f"Connection closed with {node_id}")
                        if node_id in self.connected_nodes:
                            self.remove_files_when_disconnect(node_id)  # remove os seus ficheiros da lista de partilha
                            del self.connected_nodes[node_id]  # remove o nodo da lista de nodos conectados
                        client_socket.close()
                        break

        except ConnectionResetError:
            # print(f"Connection closed with {address[0]}:{address[1]}")
            print(f"Connection closed with {node_id}")
            if node_id in self.connected_nodes:
                self.remove_files_when_disconnect(node_id)  # remove os seus ficheiros da lista de partilha
                del self.connected_nodes[node_id]  # remove o nodo da lista de nodos conectados
        finally:
            client_socket.close()

    def remove_files_when_disconnect(self, node_id):
        shared_files = self.connected_nodes[node_id]['shared_files']

        for file_name in shared_files:
            other_nodes_sharing_file = [id_nodo for id_nodo in self.connected_nodes if
                                        file_name in self.connected_nodes[id_nodo][
                                            'shared_files'] and id_nodo != node_id]

            if not other_nodes_sharing_file:
                del self.current_sharing_files[file_name]

    # FUNCAO RESPONSAVEL POR A MENSAGEM DE LISTAGEM DE FICHEIROS A SER ENVIADOS
    def list_files_being_shared(self, client_socket):
        files_info = self.current_sharing_files if self.current_sharing_files else {}
        message = FS_TrackProtocol.create_list_send_message(files_info)
        return message

    def update_node(self, file_name, block, node_id):
        if node_id in self.connected_nodes:
            if file_name not in self.connected_nodes[node_id]["shared_files"]:
                # caso ainda nao tenha o ficheiro
                self.connected_nodes[node_id]["shared_files"][file_name] = [block]
            else:
                # caso so tenha de adicionar o bloco
                self.connected_nodes[node_id]["shared_files"][file_name].append(block)

    # adiciona os "novos" ficheiros a lista de partilha
    def update_shared_files(self, shared_files):
        self.current_sharing_files.update(shared_files)

    def save_shared_files(self, node_id, shared_files):
        # Guarda os ficheiros que estão a ser shared e o numero de blocks
        self.connected_nodes[node_id]["shared_files"] = shared_files

    # funcao temporaria (para apenas 1 fs_node com o ficheiro)
    def get_file_details_from_node(self, file_name):
        nodes_info = []
        for node_id, node_info in self.connected_nodes.items():
            if "shared_files" in node_info and file_name in node_info["shared_files"]:
                address = node_info["address"]
                blocks = node_info["shared_files"][file_name]
                nodes_info.append({
                    "address": address,
                    "file_name": file_name,
                    "blocks": blocks
                })

        if nodes_info:
            message = FS_TrackProtocol.create_located_message(nodes_info)
        else:
            message = "File not found on any node."
        return message

    def get_blocks_for_file(self, file_name):
        blocks_info = {}

        for node_id, node_info in self.connected_nodes.items():
            if "shared_files" in node_info and file_name in node_info["shared_files"]:
                blocks_info[node_id] = node_info["shared_files"][file_name]

        return blocks_info

    # FUNCAO QUE FAZ O PARSING DA MENSAGEM
    @staticmethod
    def parse_message(message):

        parts = message.split('|')
        message_type = parts[0]

        if message_type == "REG":
            node_info = {
                "address": parts[1],
                "port": int(parts[2]),
                "shared_files": {}
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

        elif message_type == "EXIT":
            return {"type": message_type}

        elif message_type == "UPDATE":
            parts = parts[1].split('-')
            file_name = parts[0]
            block_tag = int(parts[1])
            node_id = parts[2]
            return {"type": message_type, "file_name": file_name, "block_tag": block_tag, "node_id": node_id}

        else:
            return {"type": message_type, "data": message}


if __name__ == "__main__":
    host = '10.4.4.1'
    port = 9090
    host_name = socket.gethostname()
    tracker = FS_Tracker(host, port, host_name)
    tracker.start()
