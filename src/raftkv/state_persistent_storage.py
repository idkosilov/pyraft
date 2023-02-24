from abc import ABC, abstractmethod
from collections import UserList
from dataclasses import dataclass
from functools import partial
from os import PathLike
from typing import Any, Optional, Callable

from raftkv.key_value_storage import KeyValueStorage


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


class TrackedList(UserList):

    def __init__(self, initlist=None):
        super().__init__(initlist)
        self.on_update: Callable[[list], None] = None

    def __setitem__(self, i, item):
        super().__setitem__(i, item)
        self.on_update(self.data)

    def __delitem__(self, i):
        super().__delitem__(i)
        self.on_update(self.data)

    def __add__(self, other):
        result = super().__add__(other)
        self.on_update(self.data)
        return result

    def __radd__(self, other):
        result = super().__radd__(other)
        self.on_update(self.data)
        return result

    def __iadd__(self, other):
        result = super().__iadd__(other)
        self.on_update(self.data)
        return result

    def append(self, item):
        super().append(item)
        self.on_update(self.data)

    def insert(self, i, item):
        super().insert(i, item)
        self.on_update(self.data)

    def pop(self, i=-1):
        super().pop(i)
        self.on_update(self.data)

    def remove(self, item):
        super().remove(item)
        self.on_update(self.data)

    def clear(self):
        super().clear()
        self.on_update(self.data)

    def reverse(self):
        super().clear()
        self.on_update(self.data)

    def extend(self, other):
        super().extend(other)
        self.on_update(self.data)


class StateKeyValueStorage(StatePersistentStorage):
    def __init__(self, path_to_storage: str | PathLike) -> None:
        self._storage = KeyValueStorage(path_to_storage, write_back=True)

    def open(self) -> None:
        self._storage.open()

    def close(self) -> None:
        self._storage.close()

    @property
    def current_term(self) -> int:
        return self._storage.get("current_term", 0)

    @current_term.setter
    def current_term(self, term: int) -> None:
        self._storage["current_term"] = term

    @property
    def voted_for(self) -> Optional[int]:
        return self._storage.get("voted_for", None)

    @voted_for.setter
    def voted_for(self, leader_id: int) -> None:
        self._storage["voted_for"] = leader_id

    @property
    def log(self) -> TrackedList[Entry]:
        log = self._storage.get("log", [])
        tracked_log = TrackedList(log)
        tracked_log.on_update = partial(self._storage.__setitem__, "log")
        return tracked_log

    @log.setter
    def log(self, entries: list[Entry]) -> None:
        self._storage["log"] = entries

    @property
    def commit_length(self) -> int:
        return self._storage.get("commit_length", 0)

    @commit_length.setter
    def commit_length(self, commit_length: int) -> None:
        self._storage["commit_length"] = commit_length
