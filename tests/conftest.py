import os

import pytest

from raftkv.state_persistent_storage import StateKeyValueStorage, Entry


@pytest.fixture
def key_value_state_storage_initial():
    storage = StateKeyValueStorage("test")
    storage.open()
    yield storage
    storage.close()
    os.remove("test.db")


@pytest.fixture
def key_value_state_storage_predefined(key_value_state_storage_initial):
    key_value_state_storage_initial._storage["voted_for"] = 4
    key_value_state_storage_initial._storage["log"] = [Entry(term=1, message='set x 12'),
                                                       Entry(term=1, message='delete x')]
    key_value_state_storage_initial._storage["commit_length"] = 2
    key_value_state_storage_initial.close()
    key_value_state_storage_initial.open()
    yield key_value_state_storage_initial

