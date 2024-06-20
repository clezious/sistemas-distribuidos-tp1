from abc import ABC, abstractmethod
import json

from common.packet_type import PacketType


class Packet(ABC):
    def __init__(self, client_id: int, packet_id: int):
        self.client_id = client_id
        self.packet_id = packet_id

    @property
    @abstractmethod
    def packet_type(self) -> PacketType:
        pass

    @property
    @abstractmethod
    def payload(self) -> list:
        pass

    def encode(self) -> str:
        return json.dumps(
            [
                self.client_id,
                self.packet_id,
                self.packet_type.value,
                self.payload
            ]
        )

    @staticmethod
    @abstractmethod
    def decode(fields: list[str]) -> "Packet":
        pass

    @property
    def trace_id(self) -> str:
        return f"{self.client_id}-{self.packet_id}"

    def __str__(self):
        return self.encode()

    def get(self, field: str):
        return getattr(self, field, None)
