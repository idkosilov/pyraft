from abc import ABC
from dataclasses import dataclass

from raftkv.state import Entry


class Message(ABC):
    ...


@dataclass
class VoteRequest(Message):
    term: int
    candidate_id: int
    last_log_index: int
    last_log_term: int


@dataclass
class VoteResponse(Message):
    term: int
    node_id: int
    granted: bool


@dataclass
class AppendEntriesRequest(Message):
    term: int
    leader_id: int
    previous_log_index: int
    previous_log_term: int
    entries: list[Entry]
    leader_commit: int


@dataclass
class AppendEntriesResponse(Message):
    term: int
    node_id: int
    ack: int
    success: bool
