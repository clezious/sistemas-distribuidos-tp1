import logging
from queue import Queue
import socket
import threading
from concurrent.futures import ThreadPoolExecutor

from .client_state import ClientState
from common.receive_utils import receive_line
from common.packet import PacketType
from common.eof_packet import EOFPacket
from common.middleware import Middleware
from common.book import Book
from common.review import Review
from common.persistence_manager import PersistenceManager

MAX_READ_SIZE = 1024
LENGTH_BYTES = 2
QUEUE_SIZE = 10000
CLIENT_ID_BYTES = 2
MAX_CONCURRENT_CONNECTIONS = 5
TIMEOUT = 5
CLIENT_ID_KEY = "client_id"
CLIENT_STATE_PREFIX = "client_state_"
EOF_STR = "EOF"


class InputBoundary:
    def __init__(self, port: int, backlog: int, books_exchange: str, reviews_exchange: str):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(("", port))
        server_socket.listen(backlog)
        self.socket = server_socket
        self.port = port
        self.should_stop = False
        self.threads = {}
        self.client_sockets = set()
        self.books_packet_queue = Queue(maxsize=QUEUE_SIZE)
        self.reviews_packet_queue = Queue(maxsize=QUEUE_SIZE)
        self.books_middleware_sender_thread = None
        self.reviews_middleware_sender_thread = None
        self.client_id = 0
        self.persistence_manager = PersistenceManager('../storage/input_boundary')
        self.persistence_manager_lock = threading.Lock()
        self.books_exchange = books_exchange
        self.reviews_exchange = reviews_exchange
        self._init_state()
        logging.info("Listening for connections and redirecting to exchanges %s and %s", books_exchange, reviews_exchange)

    def run(self):
        self.books_middleware_sender_thread = threading.Thread(
            target=self.__middleware_sender, args=(self.books_packet_queue, self.books_exchange))
        self.books_middleware_sender_thread.start()
        self.reviews_middleware_sender_thread = threading.Thread(
            target=self.__middleware_sender, args=(self.reviews_packet_queue, self.reviews_exchange, True))
        self.reviews_middleware_sender_thread.start()
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_CONNECTIONS) as executor:
            while self.should_stop is False:
                try:
                    client_socket, address = self.socket.accept()
                    client_id = self.__next_client_id()
                    self.client_sockets.add(client_socket)
                    logging.info(
                        "Connection from %s - Assigning client id: %s", address,
                        client_id)

                    thread = executor.submit(
                        self.__handle_client_connection, client_socket,
                        client_id)

                    self.threads[client_id] = thread
                except OSError:
                    logging.info("Server socket closed")
                    continue

    def __next_client_id(self):
        client_id = self.client_id
        self.client_id += 1
        with self.persistence_manager_lock:
            self.persistence_manager.put(CLIENT_ID_KEY, str(self.client_id))
        return client_id

    def __middleware_sender(self, packet_queue, output_exchange, should_clean_on_eof: bool = False):
        logging.info("Middleware sender started")
        middleware = Middleware(output_exchanges=[output_exchange])
        while self.should_stop is False:
            try:
                packet = packet_queue.get(block=True)
                if packet is None:
                    middleware.shutdown()
                    break
                middleware.send(packet.encode())
                if packet.packet_type == PacketType.EOF:
                    client_id = packet.client_id
                    logging.info(f"Sent EOF packet for client {client_id} to {output_exchange}")
                    if not should_clean_on_eof:
                        self._change_client_state(client_id, ClientState.SENDING_REVIEWS)
                    else:
                        with self.persistence_manager_lock:
                            self.persistence_manager.delete_keys(f"{CLIENT_STATE_PREFIX}{client_id}")
            except OSError:
                logging.error("Middleware closed")
                break
        logging.info("Middleware sender stopped")

    def __handle_client_connection(self, client_socket: socket.socket, client_id: int):
        packet_id = 0

        client_socket.settimeout(TIMEOUT)
        queued_books_eof = False

        with client_socket:
            client_socket.sendall(client_id.to_bytes(CLIENT_ID_BYTES, byteorder='big'))
            self._change_client_state(client_id, ClientState.SENDING_BOOKS)
            while self.should_stop is False:
                try:
                    data = receive_line(client_socket, LENGTH_BYTES).decode().strip()
                    logging.debug("Received line: %s", data)
                    if data == EOF_STR:
                        logging.info(f"EOF reached for {client_id} - queueing EOFPacket {packet_id}")
                        eof_packet = EOFPacket(client_id, packet_id)
                        if not queued_books_eof:
                            self.books_packet_queue.put(eof_packet)
                            queued_books_eof = True
                            packet_id += 1
                        else:
                            self.reviews_packet_queue.put(eof_packet)
                            break
                    elif not queued_books_eof:
                        packet = Book.from_csv_row(data, client_id, packet_id)
                        if packet:
                            self.books_packet_queue.put(packet)
                            packet_id += 1
                    else:
                        packet = Review.from_csv_row(data, client_id, packet_id)
                        if packet:
                            self.reviews_packet_queue.put(packet)
                            packet_id += 1

                except (ConnectionResetError, OSError, EOFError) as e:
                    logging.error(e)
                    logging.error(f"Sending EOF packet for client {client_id}")
                    eof_packet = EOFPacket(client_id, packet_id)
                    if not queued_books_eof:
                        self.books_packet_queue.put(eof_packet)
                    self.reviews_packet_queue.put(eof_packet)
                    break

        self.threads.pop(client_id, None)
        self.client_sockets.discard(client_socket)

    def __clear_queues(self):
        for queue in [self.books_packet_queue, self.reviews_packet_queue]:
            if queue:
                with queue.mutex:
                    queue.queue.clear()
                    queue.put(None)
                logging.info("Cleared packet queues")

    def shutdown(self):
        logging.info("Graceful shutdown")
        self.should_stop = True
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()

        client_sockets = list(self.client_sockets)
        for client_socket in client_sockets:
            logging.info("Closing client socket %s", client_socket)
            client_socket.shutdown(socket.SHUT_RDWR)
            client_socket.close()

        self.__clear_queues()

        logging.info("Waiting for threads to finish")
        threads = list(self.threads.values())
        for thread in threads:
            thread.result()

        logging.info("Waiting for middleware senders to finish")
        for thread in [self.books_middleware_sender_thread, self.reviews_middleware_sender_thread]:
            if thread:
                thread.join()

    def _change_client_state(self, client_id: int, new_state: ClientState):
        with self.persistence_manager_lock:
            self.persistence_manager.put(f"{CLIENT_STATE_PREFIX}{client_id}", str(new_state))

    def _init_state(self):
        for (key, _secondary_key) in self.persistence_manager.get_keys(CLIENT_STATE_PREFIX):
            client_id = int(key.removeprefix(CLIENT_STATE_PREFIX))
            client_state = ClientState.from_str(self.persistence_manager.get(key))
            eof_packet = EOFPacket(client_id, -1)
            if client_state == ClientState.SENDING_BOOKS:
                self.books_packet_queue.put(eof_packet)
            self.reviews_packet_queue.put(eof_packet)
            logging.info(f"Sent EOF packet for client {client_id}")
        self.persistence_manager.delete_keys(CLIENT_STATE_PREFIX)

        self.client_id = int(self.persistence_manager.get(CLIENT_ID_KEY) or "0")
        logging.info(f"Initialized state with latest client id {self.client_id}")
