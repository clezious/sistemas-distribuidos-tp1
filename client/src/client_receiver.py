import logging
import signal
import socket
import csv

from common.receive_utils import receive_line
from common.result_packet import ResultPacket

LENGTH_BYTES = 2


class ClientReceiver():
    def __init__(self, ip: str, port: int, output_dir: str):
        self.ip = ip
        self.port = port
        self.socket = None
        self.results = {query: [] for query in range(1, 6)}
        self.output_dir = output_dir

    def run(self):
        signal.signal(signal.SIGTERM, self.__graceful_shutdown)

        self.__connect()
        with self.socket:
            logging.info("Client waiting for results")
            self._handle_server_connection(self.socket)
            self._output_results()

    def _output_results(self):
        for query in self.results.keys():
            file_name = f"{self.output_dir}/query_{query}.csv"
            self.results[query] = sorted(self.results[query], key=lambda x: x[0])
            self.save_query_to_csv_file(query, file_name)
            print(f"Query {query} finished. Saved to {file_name}. Results count: {len(self.results[query])}.")

    def save_query_to_csv_file(self, query: int, file_path: str):
        with open(file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for row in self.results[query]:
                writer.writerow(row)

    def _handle_server_connection(self, s: socket.socket):
        while True:
            try:
                data = receive_line(s, LENGTH_BYTES).decode().strip()
                result_packet = ResultPacket.decode(data)
                self.results[result_packet.query].append(
                    result_packet.result.payload)
                logging.info("Saved result: %s", result_packet)
            except EOFError:
                logging.info("EOF reached")
                break
            except ConnectionResetError:
                logging.info("Connection closed by server")
                break

    def __connect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.ip, self.port))
        logging.info("Connected to %s:%d", self.ip, self.port)

    def __graceful_shutdown(self, signum, frame):
        # TODO: Implement graceful shutdown
        raise NotImplementedError("Graceful shutdown not implemented")
