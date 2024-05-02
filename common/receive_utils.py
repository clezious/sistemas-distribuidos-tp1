import socket

MAX_READ_SIZE = 1024


def receive_exact(s: socket.socket, length: int) -> bytes:
    data = b''
    while len(data) < length:
        bytes_remaining = length - len(data)
        new_data = s.recv(min(MAX_READ_SIZE, bytes_remaining))
        if not new_data:
            raise EOFError("EOF reached while reading data")
        data += new_data
    return data


def receive_line(s: socket.socket, length_bytes: int) -> bytes:
    length_as_bytes = receive_exact(s, length_bytes)
    length = int.from_bytes(length_as_bytes, byteorder='big')
    data = receive_exact(s, length)
    return data
