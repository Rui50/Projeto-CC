class FS_TrackProtocol:
    @staticmethod
    def create_register_message(node_info):
        address = node_info['address']
        port = node_info['port']
        files_info = node_info.get('files_info', {})

        # construção de mensagem de registo
        message = f"REG|{address}|{port}|"
        for file_name, num_blocks in files_info.items():
            message += f"{file_name}:{num_blocks},"

        return message.rstrip(',')

    @staticmethod
    def create_get_message(file_name):
        return f"GET|{file_name}"

    @staticmethod
    def create_located_message(address, port, file_name, blocks):
        return f"File {file_name} with {blocks} blocks found in {address}:{port}"

    @staticmethod
    def create_list_request_message():
        return "LIST"

    @staticmethod
    def create_list_send_message(files_info):
        message = "LIST"
        if files_info:
            file_list = ', '.join([f"{file_name}:{blocks}" for file_name, blocks in files_info.items()])
            message += f"|{file_list}"
        return message
