class FS_TrackProtocol:
    @staticmethod
    def create_register_message(node_info):
        address = node_info['address']
        port = node_info['port']
        files_info = node_info.get('files_info', {})  # Retrieve files info

        # Construct the register message including file info
        message = f"REG|{address}|{port}|"
        for file_name, num_blocks in files_info.items():
            message += f"{file_name}:{num_blocks},"  # Append file info

        return message.rstrip(',')  # Remove ultima virgula

    @staticmethod
    def create_ack_message():
        return "ACK"

    @staticmethod
    def create_get_message(file_name):
        return f"GET|{file_name}"

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
