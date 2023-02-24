import pytest

from raftkv.state_persistent_storage import Entry


@pytest.mark.parametrize("state_storage", [
    "key_value_state_storage_initial"
])
def test_initial_values(state_storage, request):
    state_storage = request.getfixturevalue(state_storage)
    assert state_storage.current_term == 0
    assert state_storage.voted_for is None
    assert state_storage.log == []
    assert state_storage.commit_length == 0


@pytest.mark.parametrize("state_storage", [
    "key_value_state_storage_predefined"
])
def test_can_retrieve_current_term(state_storage, request):
    state_storage = request.getfixturevalue(state_storage)
    assert state_storage.current_term == 0


@pytest.mark.parametrize("state_storage", [
    "key_value_state_storage_predefined"
])
def test_set_current_term(state_storage, request):
    state_storage = request.getfixturevalue(state_storage)
    state_storage.current_term = 5
    assert state_storage.current_term == 5


@pytest.mark.parametrize("state_storage", [
    "key_value_state_storage_predefined"
])
def test_can_retrieve_voted_for(state_storage, request):
    state_storage = request.getfixturevalue(state_storage)
    assert state_storage.voted_for == 4


@pytest.mark.parametrize("state_storage", [
    "key_value_state_storage_predefined"
])
def test_can_set_voted_for(state_storage, request):
    state_storage = request.getfixturevalue(state_storage)
    state_storage.voted_for = 6
    assert state_storage.voted_for == 6


@pytest.mark.parametrize("state_storage", [
    "key_value_state_storage_predefined"
])
def test_can_retrieve_log(state_storage, request):
    state_storage = request.getfixturevalue(state_storage)
    assert state_storage.log == [Entry(1, "set x 12"), Entry(1, "delete x")]


@pytest.mark.parametrize("state_storage", [
    "key_value_state_storage_predefined"
])
def test_can_set_log(state_storage, request):
    state_storage = request.getfixturevalue(state_storage)
    state_storage.log = [Entry(1, "set x 33"), Entry(1, "delete y")]

    assert state_storage.log == [Entry(1, "set x 33"), Entry(1, "delete y")]


@pytest.mark.parametrize("state_storage", [
    "key_value_state_storage_predefined"
])
def test_can_update_log(state_storage, request):
    state_storage = request.getfixturevalue(state_storage)
    state_storage.log.extend([Entry(1, "set x 33"), Entry(1, "delete y")])
    assert state_storage.log == [Entry(1, "set x 12"), Entry(1, "delete x"), Entry(1, "set x 33"), Entry(1, "delete y")]
    state_storage.log.remove(Entry(1, "set x 12"))
    assert state_storage.log == [Entry(1, "delete x"), Entry(1, "set x 33"), Entry(1, "delete y")]
    state_storage.log.append(Entry(1, "set x 12"))
    assert state_storage.log == [Entry(1, "delete x"), Entry(1, "set x 33"), Entry(1, "delete y"), Entry(1, "set x 12")]


@pytest.mark.parametrize("state_storage", [
    "key_value_state_storage_predefined"
])
def test_can_retrieve_commit_length(state_storage, request):
    state_storage = request.getfixturevalue(state_storage)
    assert state_storage.commit_length == 2


@pytest.mark.parametrize("state_storage", [
    "key_value_state_storage_predefined"
])
def test_can_set_commit_length(state_storage, request):
    state_storage = request.getfixturevalue(state_storage)
    state_storage.commit_length += 1
    assert state_storage.commit_length == 3
