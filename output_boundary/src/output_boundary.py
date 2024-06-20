import logging
import threading
import queue
import socket
from common.middleware import Middleware
from common.packet import Packet
from common.packet_type import PacketType
from common.receive_utils import receive_exact
from common.result_packet import ResultPacket

CLIENT_ID_BYTES = 2
QUEUE_SIZE = 10000


class OutputBoundary():
    def __init__(self, port: int, backlog: int, result_queues: dict[int, str]):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(('', port))
        server_socket.listen(backlog)
        self.server_socket = server_socket
        self.port = port
        self.backlog = backlog
        self.middleware = None
        self.result_queues = result_queues
        self.lock = threading.Lock()
        self.queues = {}
        self.threads = []
        self.client_sockets = set()
        self.should_stop = False

        logging.info("Listening for connections and replying results")

    def shutdown(self):
        logging.info("Graceful shutdown")
        self.middleware.shutdown()
        for client_socket in self.client_sockets:
            client_socket.close()
        self.client_sockets.clear()

        for queue in self.queues.values():
            queue.queue.clear()
        self.queues.clear()

        for thread in self.threads:
            thread.join()
        self.threads.clear()

        self.server_socket.close()
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(('', self.port))
        self.server_socket.listen(self.backlog)
        self.should_stop = False

    def run(self):
        middleware_receiver_thread = threading.Thread(
            target=self.__middleware_receiver)
        self.threads.append(middleware_receiver_thread)
        middleware_receiver_thread.start()

        while not self.should_stop:
            client_socket, address = self.server_socket.accept()
            logging.info("Connection from %s", address)

            thread = threading.Thread(
                target=self.__handle_client_connection,
                args=(client_socket, )
            )
            self.threads.append(thread)
            thread.start()

    def __middleware_receiver(self):
        self._init_middleware()
        self.middleware.start()

    def _init_middleware(self):
        self.middleware = Middleware()
        for query, queue in self.result_queues.items():
            self.middleware.add_input_queue(
                queue,
                self._handle_query_result(query),
                self._handle_query_eof(query),
                auto_ack=True
            )

    def _handle_query_result(self, query: int):
        def handle_result(result: Packet):
            result_packet = ResultPacket(query, result)
            if result_packet.client_id not in self.queues:
                with self.lock:
                    self.queues[result_packet.client_id] = queue.Queue(maxsize=QUEUE_SIZE)
            self.queues[result_packet.client_id].put((query, result_packet))

        return handle_result

    def _handle_query_eof(self, query: int):
        def handle_eof(eof_packet: Packet):
            logging.info("Query %s finished", query)
            if eof_packet.client_id not in self.queues:
                with self.lock:
                    self.queues[eof_packet.client_id] = queue.Queue(maxsize=QUEUE_SIZE)
            self.queues[eof_packet.client_id].put((query, eof_packet))

        return handle_eof

    # def _reset(self):
    #     # TODO: Implement reset with multiple clients
    #     logging.info("Resetting boundary")
    #     self.middleware.stop()
    #     self.middleware = None

    #     for client_socket in self.client_sockets:
    #         client_socket.close()
    #     self.client_sockets.clear()

    #     for queue in self.queues.values():
    #         queue.queue.clear()
    #     self.queues.clear()

    #     for thread in self.threads:
    #         thread.join()
    #     self.threads.clear()

    #     self.server_socket.close()
    #     self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #     self.server_socket.bind(('', self.port))
    #     self.server_socket.listen(self.backlog)
    #     self.should_stop = False

    def __receive_client_id(self, socket: socket.socket):
        client_id_bytes = receive_exact(socket, CLIENT_ID_BYTES)
        return int.from_bytes(client_id_bytes, byteorder='big')

    def __handle_client_connection(
            self, client_socket: socket.socket):
        try:
            client_id = self.__receive_client_id(client_socket)
            results_queue = self.queues.get(client_id, queue.Queue(maxsize=QUEUE_SIZE))
            with self.lock:
                if client_id not in self.queues:
                    self.queues[client_id] = results_queue
                self.client_sockets.add(client_socket)
        except BrokenPipeError:
            logging.error("Connection closed by client")
            return

        with client_socket:
            eofs = {query: False for query in self.result_queues.keys()}

            while not all(eofs.values()):
                query, packet = results_queue.get(block=True)
                if packet.packet_type == PacketType.EOF:
                    eofs[query] = True
                    continue

                try:
                    client_socket.sendall(packet.encode())
                    logging.debug("Sent result: %s", packet)
                except BrokenPipeError:
                    logging.error("Connection closed by client")
                    break

        with self.lock:
            self.queues.pop(client_id)
            self.client_sockets.remove(client_socket)
        logging.info("Client connection with %s closed", client_id)
