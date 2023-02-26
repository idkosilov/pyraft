from os import PathLike
from queue import Queue


class RaftBootstrap:

    def __init__(self, config_path: str | PathLike) -> None:
        ...

    @property
    def incoming_queue(self) -> Queue:
        return 1

    @property
    def outgoing_queue(self) -> Queue:
        return 1

    def start(self) -> None:
        ...

    def stop(self) -> None:
        ...
