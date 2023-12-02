class FS_TransferProtocol:
    @staticmethod
    def create_request_message(file_name, blocks):
        # Create a request message for a specific file or file portion
        block_str = ','.join(str(block) for block in blocks)
        return f"REQUEST|{file_name}-{block_str}"

    @staticmethod
    def ack_message(block_id):
        return f"ACK|{block_id}"
