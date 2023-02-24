import os
from unittest.mock import MagicMock

import pytest

from raftkv.node import Node
from raftkv.state import State, Entry, AbstractState, Role


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


@pytest.fixture
def node():
    node_id = 1
    nodes = {1, 2, 3, 4, 5}

    state = MagicMock(spec=AbstractState)
    state.current_term = 0
    state.voted_for = None
    state.log = []
    state.commit_length = 0
    state.current_role = Role.FOLLOWER
    state.current_leader = None
    state.votes_received = set()
    state.send_length = {}
    state.acked_length = {}

    return Node(node_id, nodes, state)
