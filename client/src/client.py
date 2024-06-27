import csv
from io import TextIOWrapper
import logging
import signal
import socket
import threading
import time

from common.receive_utils import receive_exact, receive_line
from common.result_packet import ResultPacket

LENGTH_BYTES = 2
OUTPUT_DIR = "../output"
CLIENT_ID_BYTES = 2
WAIT_TIMEOUT = 10
EOF_STR = "EOF"


class GracefulShutdown(Exception):
    pass


def send_line(line: str, socket: socket.socket):
    encoded_line = line.encode()
    length_bytes = len(encoded_line).to_bytes(LENGTH_BYTES, byteorder='big')
    encoded_msg = length_bytes + encoded_line
    socket.sendall(encoded_msg)


def process_result(result: ResultPacket):
    if result.query == 1:
        return process_query_1(result.result.payload)
    elif result.query == 2:
        return process_query_2(result.result.payload)
    elif result.query == 3:
        return process_query_3(result.result.payload)
    elif result.query == 4:
        return process_query_4(result.result.payload)
    elif result.query == 5:
        return process_query_5(result.result.payload)
    else:
        raise ValueError(f"Unknown query number {result.query}")


def process_query_1(payload: list[str]):
    title = payload[0]
    authors = payload[2]
    publisher = payload[3]
    year = float(payload[4])
    return title, authors, publisher, year


def process_query_2(payload: list[str]):
    return payload


def process_query_3(payload: list[str]):
    title = payload[0]
    authors = payload[2]
    return title, authors


def process_query_4(payload: list[str]):
    return payload


def process_query_5(payload: list[str]):
    return payload


class Client:
    def __init__(self,
                 books_path: str,
                 reviews_path: str,
                 input_gateway_addr: tuple[str, int],
                 output_gateway_addr: tuple[str, int]):
        self.books_path = books_path
        self.reviews_path = reviews_path
        self.input_gateway_addr = input_gateway_addr
        self.output_gateway_addr = output_gateway_addr
        self.input_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.results_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.results_thread = None
        self.client_id = None
        self.condition = threading.Condition()
        self.results = {query: [] for query in range(1, 6)}
        self.should_stop = False
        self.connected_to_output = False
        signal.signal(signal.SIGTERM, self.__graceful_shutdown)

    def __graceful_shutdown(self, _signum, _frame):
        logging.info("Graceful shutdown starting")
        self.shutdown()

        if self.results_thread:
            self.results_thread.join()
        logging.info("Results thread joined")

    def shutdown(self):
        if self.should_stop:
            return
        self.should_stop = True

        try:
            self.input_socket.shutdown(socket.SHUT_RDWR)
            self.input_socket.close()
        except OSError as e:
            logging.error("Error while closing input gateway socket: %s - Skipping", e)

        try:
            self.results_socket.shutdown(socket.SHUT_RDWR)
            self.results_socket.close()
        except OSError as e:
            logging.error("Error while closing output gateway socket: %s - Skipping", e)

        logging.info("Sockets closed")
        with self.condition:
            self.condition.notify_all()
        logging.info("Notified all threads")

    def run(self):
        start = time.time()

        self.results_thread = threading.Thread(target=self.receive_results)
        self.results_thread.start()
        try:
            self.__connect_to_input_gateway()
            with self.input_socket:
                logging.info("Sending books")
                self.send_books()
                logging.info(f"Sent all books in {time.time() - start} seconds")

                logging.info("Sending books EOF")
                send_line(EOF_STR, self.input_socket)
                logging.info("Sent books EOF")

                logging.info("Sending reviews")
                self.send_reviews()
                logging.info(f"Sent all reviews in {time.time() - start} seconds")

                logging.info("Sending reviews EOF")
                send_line(EOF_STR, self.input_socket)
                logging.info("Sent reviews EOF")
        except ConnectionRefusedError:
            logging.error("Connection refused")
            self.shutdown()
        except (ConnectionResetError, EOFError):
            logging.error("Connection reset by gateway")
            if not self.should_stop:
                self.shutdown()
        except (BrokenPipeError, OSError) as e:
            logging.info("Connection closed: %s", e)
            if not self.should_stop:
                self.shutdown()
        except GracefulShutdown:
            logging.info("Graceful shutdown")

        self.results_thread.join()
        logging.info("Client finished")
        logging.info(f"Total time: {time.time() - start}")

    def __connect_to_input_gateway(self):
        self.input_socket.connect(self.input_gateway_addr)
        logging.info("Connected to input %s:%d", *self.input_gateway_addr)

        client_id_bytes = receive_exact(self.input_socket, CLIENT_ID_BYTES)
        with self.condition:
            self.client_id = int.from_bytes(client_id_bytes, byteorder='big')
            logging.info("Received client id: %d", self.client_id)
            self.condition.notify_all()
            logging.info("Checking for output connection - timeout: %s seconds", WAIT_TIMEOUT)
            if not self.condition.wait_for(lambda: self.connected_to_output or self.should_stop, WAIT_TIMEOUT):
                logging.error("Output connection timeout")
                self.shutdown()
            else:
                logging.info("Output connection established")

        if self.should_stop:
            raise GracefulShutdown

    def send_books(self):
        with open(self.books_path, encoding="utf-8") as csvfile:
            csvfile.readline()  # Skip header
            self.__send_file(csvfile, self.input_socket)

    def send_reviews(self):
        with open(self.reviews_path, encoding="utf-8") as csvfile:
            csvfile.readline()  # Skip header
            self.__send_file(csvfile, self.input_socket)

    def receive_results(self):
        self.results_socket.connect(self.output_gateway_addr)
        with self.condition:
            self.connected_to_output = True
            self.condition.notify_all()

        logging.info("Connected to output %s:%d", *self.output_gateway_addr)
        try:
            with self.results_socket:
                self.__send_client_id(self.results_socket)
                self.__receive_results()
        except GracefulShutdown:
            logging.info("Interrupted results thread for graceful shutdown")
            return
        except (BrokenPipeError, ConnectionResetError, OSError) as e:
            logging.error("Connection closed: %s", e)
            self.shutdown()
            return
        self.__output_results()
        logging.info("Finished receiving results, shutting down")
        self.shutdown()

    def __receive_results(self):
        while not self.should_stop:
            try:
                data = receive_line(self.results_socket, LENGTH_BYTES).decode().strip()
                result_packet = ResultPacket.decode(data)
                result_data = process_result(result_packet)
                self.results[result_packet.query].append(result_data)
                logging.info("Saved result: %s", result_packet)
            except EOFError:
                logging.info("EOF reached")
                break

    def __output_results(self):
        for query in self.results.keys():
            file_name = f"{OUTPUT_DIR}/query_{query}.csv"
            self.results[query] = sorted(
                self.results[query], key=lambda x: x[0])
            self.__save_query_to_csv_file(query, file_name)
            print(
                f"Query {query} finished. Saved to {file_name}. Results count: {len(self.results[query])}.")

    def __save_query_to_csv_file(self, query: int, file_path: str):
        with open(file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=',',
                                quotechar='"',
                                quoting=csv.QUOTE_MINIMAL,
                                lineterminator='\n')
            for row in self.results[query]:
                writer.writerow(row)

    def __send_client_id(self, socket: socket.socket):
        with self.condition:
            logging.info("Waiting for client id")
            if not self.condition.wait_for(lambda: self.client_id is not None, WAIT_TIMEOUT):
                logging.error("Client id not received in time")
                self.shutdown()

        if self.should_stop:
            raise GracefulShutdown

        client_id_encoded = self.client_id.to_bytes(CLIENT_ID_BYTES, byteorder='big')
        socket.sendall(client_id_encoded)

    def __send_file(self, file: TextIOWrapper, socket: socket.socket):
        for line in file:
            if self.should_stop:
                raise GracefulShutdown

            send_line(line.strip(), socket)
            logging.debug("Sent line: %s", line.strip())
