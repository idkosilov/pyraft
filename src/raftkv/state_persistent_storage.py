from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class Entry:
    term: int
    message: Any


class StatePersistentStorage(ABC):

    @property
    @abstractmethod
    def current_term(self) -> int:
        ...

    @current_term.setter
    @abstractmethod
    def current_term(self, term: int) -> None:
        ...

    @property
    @abstractmethod
    def voted_for(self) -> Optional[int]:
        ...

    @voted_for.setter
    @abstractmethod
    def voted_for(self, leader_id: int) -> None:
        ...

    @property
    @abstractmethod
    def log(self) -> list[Entry]:
        ...

    @log.setter
    @abstractmethod
    def log(self, log: list[Entry]) -> None:
        ...

    @property
    @abstractmethod
    def commit_length(self) -> int:
        ...

    @commit_length.setter
    @abstractmethod
    def commit_length(self, commit_length: int) -> None:
        ...