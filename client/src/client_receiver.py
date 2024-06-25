import errno
import logging
import signal
import socket
import csv

from common.receive_utils import receive_line
from common.result_packet import ResultPacket

LENGTH_BYTES = 2
CLIENT_ID_BYTES = 2


class ClientReceiver():
    def __init__(self, ip: str, port: int, output_dir: str, client_id: int):
        self.ip = ip
        self.port = port
        self.socket = None
        self.results = {query: [] for query in range(1, 6)}
        self.output_dir = output_dir
        self.client_id = client_id
        self.should_stop = False

    def run(self):
        signal.signal(signal.SIGTERM, self.__graceful_shutdown)

        self.__connect()
        with self.socket:
            logging.info("Client waiting for results")
            self.__send_client_id()
            self._handle_server_connection(self.socket)
            if self.should_stop:
                return False
            self._output_results()

        return True

    def __send_client_id(self):
        client_id_encoded = self.client_id.to_bytes(
            CLIENT_ID_BYTES, byteorder='big')
        self.socket.sendall(client_id_encoded)

    def _output_results(self):
        for query in self.results.keys():
            file_name = f"{self.output_dir}/query_{query}.csv"
            self.results[query] = sorted(
                self.results[query], key=lambda x: x[0])
            self.save_query_to_csv_file(query, file_name)
            print(
                f"Query {query} finished. Saved to {file_name}. Results count: {len(self.results[query])}.")

    def save_query_to_csv_file(self, query: int, file_path: str):
        with open(file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=',',
                                quotechar='"',
                                quoting=csv.QUOTE_MINIMAL,
                                lineterminator='\n')
            for row in self.results[query]:
                writer.writerow(row)

    def _handle_server_connection(self, s: socket.socket):
        while not self.should_stop:
            try:
                data = receive_line(s, LENGTH_BYTES).decode().strip()
                result_packet = ResultPacket.decode(data)
                result_data = self._process_result(result_packet)
                self.results[result_packet.query].append(result_data)
                logging.info("Saved result: %s", result_packet)
            except EOFError:
                logging.info("EOF reached")
                break
            except ConnectionResetError:
                logging.info("Connection closed by server")
                break
            except OSError as e:
                if e.errno == errno.EBADF:
                    logging.info("Connection closed")
                else:
                    raise e
                break

    def _process_result(self, result: ResultPacket):
        if result.query == 1:
            return self._process_query_1(result.result.payload)
        elif result.query == 2:
            return self._process_query_2(result.result.payload)
        elif result.query == 3:
            return self._process_query_3(result.result.payload)
        elif result.query == 4:
            return self._process_query_4(result.result.payload)
        elif result.query == 5:
            return self._process_query_5(result.result.payload)
        else:
            raise ValueError(f"Unknown query number {result.query}")

    def _process_query_1(self, payload: list[str]):
        title = payload[0]
        authors = payload[2]
        publisher = payload[3]
        year = float(payload[4])
        return title, authors, publisher, year

    def _process_query_2(self, payload: list[str]):
        return payload

    def _process_query_3(self, payload: list[str]):
        title = payload[0]
        authors = payload[2]
        return title, authors

    def _process_query_4(self, payload: list[str]):
        return payload

    def _process_query_5(self, payload: list[str]):
        return payload

    def __connect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.ip, self.port))
        logging.info("Connected to %s:%d", self.ip, self.port)

    def __graceful_shutdown(self, signum, frame):
        # TODO: Implement graceful shutdown
        self.should_stop = True
        if self.socket:
            self.socket.close()
            self.socket = None
