import os
from typing import Optional

import pytest

from raftkv.message_bridge import MessageBridge
from raftkv.node import Node
from raftkv.state import State, Entry, AbstractState
from raftkv.timer import ElectionTimer, HeartbeatTimer


@pytest.fixture
def key_value_state_storage_initial():
    storage = State("test")
    storage.open()
    yield storage
    storage.close()
    os.remove("test.db")


@pytest.fixture
def key_value_state_storage_predefined(key_value_state_storage_initial):
    key_value_state_storage_initial._storage["voted_for"] = 4
    key_value_state_storage_initial._storage["log"] = [Entry(term=1, message='set x 12'),
                                                       Entry(term=1, message='delete x')]
    key_value_state_storage_initial._storage["commit_index"] = 2
    key_value_state_storage_initial.close()
    key_value_state_storage_initial.open()
    yield key_value_state_storage_initial


class FakeState(AbstractState):
    def __init__(self):
        super().__init__()
        self._current_term = 0
        self._voted_for = None
        self._log = []
        self._commit_index = 0

    @property
    def current_term(self) -> int:
        return self._current_term

    @current_term.setter
    def current_term(self, term: int) -> None:
        self._current_term = term

    @property
    def voted_for(self) -> Optional[int]:
        return self._voted_for

    @voted_for.setter
    def voted_for(self, node_id: int) -> None:
        self._voted_for = node_id

    @property
    def log(self) -> list[Entry]:
        return self._log

    @log.setter
    def log(self, log) -> None:
        self._log = log

    @property
    def commit_index(self) -> int:
        return self._commit_index

    @commit_index.setter
    def commit_index(self, commit_index: int) -> None:
        self._commit_index = commit_index


@pytest.fixture
def node():
    node_id = 1
    nodes = {1, 2, 3, 4, 5}

    state = FakeState()

    return Node(node_id, nodes, state)


@pytest.fixture
def election_timer():
    election_timeout_lower = 500
    election_timeout_upper = 1000
    return ElectionTimer(election_timeout_lower, election_timeout_upper)


@pytest.fixture
def heartbeat_timer():
    heartbeat_timeout = 100
    return HeartbeatTimer(heartbeat_timeout)


@pytest.fixture
def message_bridge():
    message_bridge = MessageBridge()
    yield message_bridge
    message_bridge.stop()
