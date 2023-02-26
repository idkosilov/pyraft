from dataclasses import dataclass


@dataclass
class NodeConfiguration:
    node_id: int
    host: str
    port: int
