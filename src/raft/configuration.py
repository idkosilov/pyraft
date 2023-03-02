from dataclasses import dataclass
from os import PathLike


@dataclass
class NodeConfiguration:
    node_id: int


@dataclass
class ZmqNodeConfiguration(NodeConfiguration):
    host: str
    port: int

    def url(self) -> str:
        return f"tcp://{self.host}:{self.port}"


@dataclass
class RaftConfiguration:
    node_id: int
    storage_path: str | PathLike
    heartbeat_timeout: int
    election_timeout_lower: int
    election_timeout_upper: int
    cluster: list[NodeConfiguration]
