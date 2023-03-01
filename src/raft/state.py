import enum
from abc import ABC, abstractmethod
from collections import UserList
from dataclasses import dataclass
from functools import partial
from os import PathLike
from typing import Any, Optional, Callable

from key_value.key_value_storage import KeyValueStorage


class Role(enum.Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


@dataclass
class Entry:
    """
    Class representing a log entry with a term and a message.
    """
    term: int
    message: Any


class AbstractState(ABC):
    """
    Abstract class representing the persistent state of a Raft node.

    Subclasses of this class should implement methods to handle read and write
    operations for each persistent field.
    """

    def __init__(self) -> None:
        """
        Initializes an instance of the AbstractState class.
        """
        # The current role of the Raft node.
        self.current_role = Role.FOLLOWER

        # The ID of the current leader Raft node or None if there is no leader.
        self.current_leader: Optional[int] = None

        # The set of IDs of Raft nodes that have voted for the current Raft node.
        self.votes_received: set[int] = set()

        # A dictionary where the keys are the IDs of Raft nodes and the values are the
        # index of the next entry to send to those Raft nodes.
        self.next_index: dict[int, int] = {}

        # A dictionary where the keys are the IDs of Raft nodes and the values are the
        # index of the highest log entry known to be replicated to those Raft nodes.
        self.match_index: dict[int, int] = {}

    @property
    @abstractmethod
    def current_term(self) -> int:
        """
        The latest term the node has seen.

        :return: The latest term seen by the node.
        """
        ...

    @current_term.setter
    @abstractmethod
    def current_term(self, term: int) -> None:
        """
        Set the current term to the given value.

        :param term: The value to set as the current term.
        """
        ...

    @property
    @abstractmethod
    def voted_for(self) -> Optional[int]:
        """
        The ID of the candidate the node voted for in its current term.

        :return: The ID of the candidate voted for in the current term, or None if the node hasn't voted yet.
        """
        ...

    @voted_for.setter
    @abstractmethod
    def voted_for(self, leader_id: int) -> None:
        """
        Set the ID of the candidate the node voted for in its current term.

        :param leader_id: The ID of the candidate to vote for.
        """
        ...

    @property
    @abstractmethod
    def log(self) -> list[Entry]:
        """
        The list of log entries.

        :return: The list of log entries.
        """
        ...

    @log.setter
    @abstractmethod
    def log(self, log: list[Entry]) -> None:
        """
        Set the list of log entries.

        :param log: The new list of log entries.
        """
        ...

    @property
    @abstractmethod
    def commit_index(self) -> int:
        """
        The index of the highest log entry known to be committed.

        :return: The index of the highest committed log entry.
        """
        ...

    @commit_index.setter
    @abstractmethod
    def commit_index(self, commit_index: int) -> None:
        """
        Set the index of the highest log entry known to be committed.

        :param commit_index: The new value for the commit length.
        """
        ...

    @property
    def last_log_term(self) -> int:
        if len(self.log) > 0:
            return self.log[-1].term
        else:
            return 0

    @property
    def last_log_index(self) -> int:
        if len(self.log) > 0:
            return len(self.log) - 1
        else:
            return -1


class TrackedList(UserList):
    """
    A list that tracks updates and notifies a callback function.

    This class is a subclass of UserList that notifies a callback function
    whenever the list is modified.
    """

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


class State(AbstractState):
    """
    A class that implements the StatePersistentStorage interface using a key-value storage.

    :param path_to_storage: The path to the file where the key-value storage is persisted.
    """

    def __init__(self, path_to_storage: str | PathLike) -> None:
        """
        Initializes a new instance of the StateKeyValueStorage class.

        :param path_to_storage: The path to the file where the key-value storage is persisted.
        """
        super().__init__()
        self._storage = KeyValueStorage(path_to_storage, write_back=True)

    def open(self) -> None:
        """
        Opens the key-value storage.
        """
        self._storage.open()

    def close(self) -> None:
        """
        Closes the key-value storage.
        """
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
    def commit_index(self) -> int:
        return self._storage.get("commit_index", 0)

    @commit_index.setter
    def commit_index(self, commit_index: int) -> None:
        self._storage["commit_index"] = commit_index
