from abc import ABC, abstractmethod
import json

from common.packet_type import PacketType


class Packet(ABC):

    @property
    @abstractmethod
    def packet_type(self) -> PacketType:
        pass

    @property
    @abstractmethod
    def payload(self) -> list:
        pass

    def encode(self) -> str:
        return json.dumps([self.packet_type.value, self.payload])

    @staticmethod
    @abstractmethod
    def decode(fields: list[str]) -> "Packet":
        pass

    def __str__(self):
        return self.encode()

    def get(self, field: str):
        return getattr(self, field, None)
