import os

import pytest

from raftkv.state_persistent_storage import StateKeyValueStorage, Entry


@pytest.fixture
def key_value_state_storage():
    storage = StateKeyValueStorage("test")
    storage.open()
    storage._storage["voted_for"] = 4
    storage._storage["log"] = [Entry(term=1, message='set x 12'), Entry(term=1, message='delete x')]
    storage._storage["commit_length"] = 2
    storage.close()
    storage.open()
    yield storage
    storage.close()
    os.remove("test.db")
