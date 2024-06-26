import errno
import logging
import threading
import queue
import socket
import time
from common.middleware import Middleware
from common.packet import Packet
from common.packet_type import PacketType
from common.receive_utils import receive_exact
from common.result_packet import ResultPacket

CLIENT_ID_BYTES = 2
QUEUE_SIZE = 10000
QUEUE_TIMEOUT = 60 * 60  # 1 hour


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
        self.queues: dict[int, queue.Queue] = {}
        self.access_times: dict[int, float] = {}
        self.connected_clients = set()
        self.threads = []
        self.client_sockets = set()
        self.should_stop = False
        self.condition = threading.Condition()

        logging.info("Listening for connections and replying results")

    def shutdown(self):
        logging.info("Graceful shutdown")
        self.should_stop = True
        self.server_socket.close()
        self.middleware.shutdown()

        with self.condition:
            self.condition.notify_all()

        logging.info("Closing client sockets")
        for client_socket in self.client_sockets:
            client_socket.close()
        self.client_sockets.clear()

        for queue in self.queues.values():
            with queue.mutex:
                queue.queue.clear()
            queue.put((None, None))
        self.queues.clear()

        logging.info("Joining threads")
        for thread in self.threads:
            thread.join()
        self.threads.clear()
        logging.info("Joined all threads")

    def run(self):
        middleware_receiver_thread = threading.Thread(
            target=self.__middleware_receiver)
        cleaner_thread = threading.Thread(target=self.__cleaner)
        self.threads.append(middleware_receiver_thread)
        self.threads.append(cleaner_thread)
        middleware_receiver_thread.start()
        cleaner_thread.start()

        while not self.should_stop:
            try:
                client_socket, address = self.server_socket.accept()
                logging.info("Connection from %s", address)
            except OSError:
                logging.info("Server socket closed")
                break

            thread = threading.Thread(
                target=self.__handle_client_connection,
                args=(client_socket, )
            )
            self.threads.append(thread)
            thread.start()

    def __middleware_receiver(self):
        self._init_middleware()
        self.middleware.start()

    def __cleaner(self):
        while not self.should_stop:
            with self.lock:
                current_time = time.time()
                ids_to_delete = [
                    client_id for client_id,
                    last_access in self.access_times.items()
                    if current_time - last_access > QUEUE_TIMEOUT]
                for client_id in ids_to_delete:
                    logging.info(
                        "[CLEANER] Deleting inactive queue for client %s",
                        client_id)
                    self.queues[client_id].put((None, None))
                    self.queues.pop(client_id, None)
                    self.access_times.pop(client_id, None)
            with self.condition:
                self.condition.wait(QUEUE_TIMEOUT // 10)
        logging.info("[CLEANER] Stopped")

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
            client_id = result_packet.client_id
            with self.lock:
                if client_id not in self.queues:
                    self.queues[client_id] = queue.Queue(
                        maxsize=QUEUE_SIZE)
                if client_id not in self.connected_clients:
                    self.access_times[client_id] = time.time()
            self.queues[client_id].put((query, result_packet))

        return handle_result

    def _handle_query_eof(self, query: int):
        def handle_eof(eof_packet: Packet):
            client_id = eof_packet.client_id
            logging.info("[CLIENT %s] Query %s finished", client_id, query)
            with self.lock:
                if client_id not in self.queues:
                    self.queues[client_id] = queue.Queue(
                        maxsize=QUEUE_SIZE)
                if client_id not in self.connected_clients:
                    self.access_times[client_id] = time.time()
            self.queues[client_id].put((query, eof_packet))

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
        client_id = 'unknown'
        try:
            client_id = self.__receive_client_id(client_socket)
            results_queue = self.queues.get(
                client_id, queue.Queue(maxsize=QUEUE_SIZE))
            with self.lock:
                if client_id not in self.queues:
                    self.queues[client_id] = results_queue
                self.connected_clients.add(client_id)
                self.access_times.pop(client_id, None)
                self.client_sockets.add(client_socket)
        except (BrokenPipeError, EOFError, ConnectionResetError):
            logging.error(
                "[CLIENT %s] Connection closed by client", client_id)
            return
        except OSError as e:
            if e.errno == errno.EBADF:
                logging.info("[CLIENT %s] Connection closed", client_id)
                return
            raise e

        with client_socket:
            eofs = {query: False for query in self.result_queues.keys()}

            while not all(eofs.values()):
                query, packet = results_queue.get(block=True)
                logging.debug("Received result: %s", (query, packet))
                if query is None and packet is None:
                    logging.info("[CLIENT %s] disconnected due to shutdown or cleaner", client_id)
                    break

                if packet.packet_type == PacketType.EOF:
                    eofs[query] = True
                    continue

                try:
                    client_socket.sendall(packet.encode())
                    logging.debug("[CLIENT %s] Sent result: %s",
                                  client_id, packet)
                except (BrokenPipeError, ConnectionResetError):
                    logging.error("[CLIENT %s] Connection closed by client", client_id)
                    break

        with self.lock:
            self.queues.pop(client_id, None)
            self.connected_clients.discard(client_id)
            self.access_times.pop(client_id, None)
            self.client_sockets.discard(client_socket)
        logging.info("[CLIENT %s] Connection closed", client_id)
