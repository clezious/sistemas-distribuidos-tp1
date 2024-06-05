from abc import ABC, abstractmethod
import json
import uuid

from common.packet_type import PacketType


class Packet(ABC):
    def __init__(self, trace_id: str = None):
        self.trace_id = trace_id if trace_id else str(uuid.uuid4())

    @property
    @abstractmethod
    def packet_type(self) -> PacketType:
        pass

    @property
    @abstractmethod
    def payload(self) -> list:
        pass

    def encode(self) -> str:
        return json.dumps([self.trace_id, self.packet_type.value, self.payload])

    @staticmethod
    @abstractmethod
    def decode(data: str, trace_id: str) -> 'Packet':
        pass

    def __str__(self):
        return self.encode()

    def get(self, field: str):
        return getattr(self, field, None)
