class FS_TransferProtocol:
    #@staticmethod
    #def create_request_message(file_name, blocks):
    #    # Create a request message for a specific file or file portion
    #    return f"REQUEST|{file_name}:{blocks}"
    @staticmethod
    def create_request_message(file_name, blocks):
        # Create a request message for a specific file or file portion
        block_str = ','.join(str(block) for block in blocks)
        return f"REQUEST|{file_name}-{block_str}"

    @staticmethod
    def parse_request_message(message):
        # Parse a request message and extract file name and blocks information
        pass

    @staticmethod
    def create_data_packet(sequence_number, data):
        # Create a data packet with sequence number and data payload
        pass

    @staticmethod
    def parse_data_packet(packet):
        # Parse a received data packet to extract sequence number and data payload
        pass

    @staticmethod
    def create_acknowledgment(sequence_number):
        # Create an acknowledgment message for a received data packet
        return f"ACK|{sequence_number}"

    @staticmethod
    def parse_acknowledgment(message):
        # Parse an acknowledgment message and extract the sequence number
        pass