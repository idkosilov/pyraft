from abc import ABC
from dataclasses import dataclass


class Message(ABC):
    ...


@dataclass
class VoteRequest(Message):
    term: int
    candidate_id: int
    last_log_index: int
    last_log_term: int
