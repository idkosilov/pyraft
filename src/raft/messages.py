from abc import ABC
from dataclasses import dataclass
from typing import Optional, Any

from raft.state import Entry


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
    success: bool
    last_log_index: Optional[int] = None


@dataclass
class ClientRequest(Message):
    message: Any
