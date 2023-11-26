class FS_TrackProtocol:
    @staticmethod
    def create_register_message(node_info):
        address = node_info['address']
        port = node_info['port']
        files_info = node_info.get('files_info', {})

        # construção de mensagem de registo
        message = f"REG|{address}|{port}|"
        for file_name, block_list in files_info.items():
            message += f"{file_name}:[{','.join(map(str, block_list))}]/"

        # remover ultima virgula
        if message.endswith('/'):
            message = message[:-1]

        print(message)
        return message.rstrip(',')

    @staticmethod
    def create_locate_message(file_name):
        return f"LOCATE|{file_name}"

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
            file_list = '/'.join([f"{file_name}:{blocks}" for file_name, blocks in files_info.items()])
            message += f"|{file_list}"
        return message
    @staticmethod
    def create_get_message(file_name):
        return f"GET|{file_name}"

    @staticmethod
    def create_get_response_message(blocks_info, file_name):
        message = f"GET_RESPONSE|{file_name}"
        if blocks_info:
            for node_id, blocks in blocks_info.items():
                block_list = ','.join(map(str, blocks))
                message += f"-{node_id} with blocks {block_list}"
        return message

    @staticmethod
    def create_request_message(file_name, blocks):
        message = f"REQUEST|{file_name}:{blocks}"
        return message
