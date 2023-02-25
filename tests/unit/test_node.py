from typing import Optional
from unittest.mock import MagicMock, call

from raftkv.messages import VoteRequest, VoteResponse, AppendEntriesRequest, AppendEntriesResponse
from raftkv.state import Role, Entry, AbstractState


def test_on_election_timeout_or_leader_fault(node):
    node.send_message_callback = MagicMock()
    node.start_election_timer_callback = MagicMock()
    node.on_election_timeout_or_leader_fault()

    assert node.state.current_term == 1
    assert node.state.current_role == Role.CANDIDATE
    assert node.state.voted_for == node.node_id
    assert node.state.votes_received == {node.node_id, }
    assert node.send_message_callback.call_count == 4
    assert node.start_election_timer_callback.call_count == 1

    expected_vote_request = VoteRequest(
        term=node.state.current_term,
        candidate_id=node.node_id,
        last_log_index=len(node.state.log) - 1,
        last_log_term=node.state.last_log_term
    )

    for call_args in node.send_message_callback.call_args_list:
        recipient_node_id, message = call_args.args
        assert recipient_node_id in node.nodes_ids
        assert message == expected_vote_request


def test_on_vote_request_to_node_granted(node):
    node.send_message_callback = MagicMock()
    node.state.last_log_term = 0
    vote_request = VoteRequest(term=2, candidate_id=2, last_log_index=10, last_log_term=1)
    node.on_vote_request(vote_request)

    assert node.state.current_term == vote_request.term
    assert node.state.current_role == Role.FOLLOWER
    assert node.state.voted_for == vote_request.candidate_id

    expected_vote_response = VoteResponse(term=vote_request.term,
                                          node_id=node.node_id,
                                          granted=True)

    assert node.send_message_callback.call_count == 1
    node.send_message_callback.assert_called_with(2, expected_vote_response)


def test_on_vote_request_to_node_already_voted_for_this_candidate_granted(node):
    node.send_message_callback = MagicMock()
    node.state.last_log_term = 0
    node.state.voted_for = 2
    node.state.current_term = 2
    vote_request = VoteRequest(term=2, candidate_id=2, last_log_index=10, last_log_term=1)
    node.on_vote_request(vote_request)

    assert node.state.current_term == vote_request.term
    assert node.state.current_role == Role.FOLLOWER
    assert node.state.voted_for == vote_request.candidate_id

    expected_vote_response = VoteResponse(term=vote_request.term,
                                          node_id=node.node_id,
                                          granted=True)

    assert node.send_message_callback.call_count == 1
    node.send_message_callback.assert_called_with(2, expected_vote_response)


def test_on_vote_request_to_node_already_voted_for_another_candidate_not_granted(node):
    node.send_message_callback = MagicMock()
    node.state.last_log_term = 0
    node.state.voted_for = 3
    node.state.current_term = 2
    vote_request = VoteRequest(term=2, candidate_id=2, last_log_index=10, last_log_term=1)
    node.on_vote_request(vote_request)

    assert node.state.current_term == vote_request.term
    assert node.state.current_role == Role.FOLLOWER
    assert node.state.voted_for == 3

    expected_vote_response = VoteResponse(term=vote_request.term,
                                          node_id=node.node_id,
                                          granted=False)

    assert node.send_message_callback.call_count == 1
    node.send_message_callback.assert_called_with(2, expected_vote_response)


def test_on_vote_request_to_candidate_node_not_granted(node):
    node.send_message_callback = MagicMock()
    node.state.last_log_term = 0
    node.state.voted_for = node.node_id
    node.state.current_role = Role.CANDIDATE
    node.state.current_term = 2
    vote_request = VoteRequest(term=2, candidate_id=2, last_log_index=10, last_log_term=1)
    node.on_vote_request(vote_request)

    assert node.state.current_term == vote_request.term
    assert node.state.current_role == Role.CANDIDATE
    assert node.state.voted_for == node.node_id

    expected_vote_response = VoteResponse(term=vote_request.term,
                                          node_id=node.node_id,
                                          granted=False)

    assert node.send_message_callback.call_count == 1
    node.send_message_callback.assert_called_with(2, expected_vote_response)


def test_on_vote_request_to_candidate_with_outdated_term_become_follower_and_granted(node):
    node.send_message_callback = MagicMock()
    node.state.last_log_term = 0
    node.state.voted_for = node.node_id
    node.state.current_role = Role.CANDIDATE
    node.state.current_term = 1
    vote_request = VoteRequest(term=2, candidate_id=2, last_log_index=10, last_log_term=1)
    node.on_vote_request(vote_request)

    assert node.state.current_term == vote_request.term
    assert node.state.current_role == Role.FOLLOWER
    assert node.state.voted_for == vote_request.candidate_id

    expected_vote_response = VoteResponse(term=vote_request.term,
                                          node_id=node.node_id,
                                          granted=True)

    assert node.send_message_callback.call_count == 1
    node.send_message_callback.assert_called_with(2, expected_vote_response)


def test_on_vote_request_with_outdated_log_to_node_not_granted(node):
    node.send_message_callback = MagicMock()
    node.state.last_log_term = 2
    node.state.voted_for = 4
    node.state.current_role = Role.FOLLOWER
    node.state.current_term = 2

    vote_request = VoteRequest(term=3, candidate_id=2, last_log_index=10, last_log_term=1)
    node.on_vote_request(vote_request)

    assert node.state.current_term == vote_request.term
    assert node.state.current_role == Role.FOLLOWER
    assert node.state.voted_for is None

    expected_vote_response = VoteResponse(term=vote_request.term,
                                          node_id=node.node_id,
                                          granted=False)

    assert node.send_message_callback.call_count == 1
    node.send_message_callback.assert_called_with(2, expected_vote_response)


def test_on_vote_request_with_outdated_term_to_node_not_granted(node):
    node.send_message_callback = MagicMock()
    node.state.last_log_term = 2
    node.state.voted_for = 4
    node.state.current_role = Role.FOLLOWER
    node.state.current_term = 2

    vote_request = VoteRequest(term=1, candidate_id=2, last_log_index=10, last_log_term=1)
    node.on_vote_request(vote_request)

    assert node.state.current_term == 2
    assert node.state.current_role == Role.FOLLOWER
    assert node.state.voted_for == 4

    expected_vote_response = VoteResponse(term=2,
                                          node_id=node.node_id,
                                          granted=False)

    assert node.send_message_callback.call_count == 1
    node.send_message_callback.assert_called_with(2, expected_vote_response)


def test_on_vote_response_node_become_a_leader_on_quorum_received_all_granted(node):
    node.send_message_callback = MagicMock()
    node.cancel_election_timer_callback = MagicMock()
    node.replicate_log = MagicMock()
    node.state.current_role = Role.CANDIDATE
    node.state.current_term = 2
    node.state.votes_received = {node.node_id, }
    node.state.voted_for = node.node_id

    vote_response_from_2 = VoteResponse(term=node.state.current_term,
                                        node_id=2,
                                        granted=True)
    vote_response_from_3 = VoteResponse(term=node.state.current_term,
                                        node_id=3,
                                        granted=True)
    vote_response_from_4 = VoteResponse(term=node.state.current_term,
                                        node_id=4,
                                        granted=True)
    vote_response_from_5 = VoteResponse(term=node.state.current_term,
                                        node_id=5,
                                        granted=True)

    node.on_vote_response(vote_response_from_2)
    assert node.state.votes_received == {node.node_id, 2}

    node.on_vote_response(vote_response_from_3)
    assert node.state.votes_received == {node.node_id, 2, 3}
    assert node.state.current_role == Role.LEADER
    assert node.state.current_leader == node.node_id
    assert node.cancel_election_timer_callback.call_count == 1
    assert node.replicate_log.call_count == 4

    followers_ids = {2, 3, 4, 5}
    for follower_id in followers_ids:
        assert node.state.next_index[follower_id] == len(node.state.log)
        assert node.state.match_index[follower_id] == 0

    for call_args in node.replicate_log.call_args_list:
        follower_id, = call_args.args
        assert follower_id in followers_ids
        followers_ids.discard(follower_id)

    assert len(followers_ids) == 0

    node.on_vote_response(vote_response_from_4)
    assert node.state.votes_received == {node.node_id, 2, 3}
    assert node.state.current_role == Role.LEADER
    assert node.state.current_leader == node.node_id

    node.on_vote_response(vote_response_from_5)
    assert node.state.votes_received == {node.node_id, 2, 3}
    assert node.state.current_role == Role.LEADER
    assert node.state.current_leader == node.node_id


def test_on_vote_response_on_higher_term_become_follower(node):
    node.send_message_callback = MagicMock()
    node.cancel_election_timer_callback = MagicMock()
    node.replicate_log = MagicMock()
    node.state.current_role = Role.CANDIDATE
    node.state.current_term = 2
    node.state.votes_received = {node.node_id, }
    node.state.voted_for = node.node_id

    vote_response_from_2 = VoteResponse(term=3,
                                        node_id=2,
                                        granted=False)

    node.on_vote_response(vote_response_from_2)

    assert node.state.current_term == vote_response_from_2.term
    assert node.state.current_role == Role.FOLLOWER
    assert node.state.voted_for is None
    assert node.cancel_election_timer_callback.call_count == 1


def test_on_client_request_leader_replicate_log(node):
    node.replicate_log = MagicMock()
    node.state.current_role = Role.LEADER
    node.state.current_term = 2
    node.state.match_index = {node.node_id: 0}

    node.on_client_request("SET X 12")

    expected_log = [Entry(term=2, message="SET X 12")]

    assert node.replicate_log.call_count == 4
    assert node.state.log == expected_log

    followers_ids = {2, 3, 4, 5}
    for call_args in node.replicate_log.call_args_list:
        follower_id, = call_args.args
        assert follower_id in followers_ids
        followers_ids.discard(follower_id)


def test_on_client_request_follower_sent_to_leader(node):
    node.send_message_callback = MagicMock()
    node.state.current_role = Role.FOLLOWER

    node.on_client_request("SET X 12")

    assert node.send_message_callback.call_count == 1
    node.send_message_callback.assert_called_with(node.state.current_leader, "SET X 12")


def test_on_heartbeat_on_leader(node):
    node.replicate_log = MagicMock()
    node.state.current_role = Role.LEADER
    node.state.current_term = 2

    node.on_heartbeat()

    assert node.replicate_log.call_count == 4

    followers_ids = {2, 3, 4, 5}
    for call_args in node.replicate_log.call_args_list:
        follower_id, = call_args.args
        assert follower_id in followers_ids
        followers_ids.discard(follower_id)


def test_on_heartbeat_follower(node):
    node.replicate_log = MagicMock()
    node.state.current_role = Role.FOLLOWER

    node.on_heartbeat()

    assert node.replicate_log.call_count == 0

    node.state.current_role = Role.CANDIDATE

    node.on_heartbeat()

    assert node.replicate_log.call_count == 0


def test_replicate_log_on_leader_empty_log(node):
    node.state.current_term = 1
    node.state.current_role = Role.LEADER
    node.state.current_leader = node.node_id
    node.state.log = []
    node.state.last_log_index = 0
    node.state.last_log_term = 0
    node.state.next_index = {2: 0, 3: 0, 4: 0, 5: 0}
    node.send_message_callback = MagicMock()

    expected_message = AppendEntriesRequest(term=node.state.current_term,
                                            leader_id=node.state.current_leader,
                                            previous_log_index=-1,
                                            previous_log_term=0,
                                            entries=[],
                                            leader_commit=node.state.commit_index)

    node.replicate_log(2)

    node.send_message_callback.assert_called_with(2, expected_message)


def test_replicate_log_on_leader_first_message_log(node):
    node.state.current_term = 1
    node.state.current_role = Role.LEADER
    node.state.current_leader = node.node_id
    node.state.log = [Entry(1, "entry1"), ]
    node.state.last_log_index = 1
    node.state.last_log_term = 1
    node.state.next_index = {2: 0, 3: 0, 4: 0, 5: 0}
    node.send_message_callback = MagicMock()

    expected_message = AppendEntriesRequest(term=node.state.current_term,
                                            leader_id=node.state.current_leader,
                                            previous_log_index=-1,
                                            previous_log_term=0,
                                            entries=[Entry(1, "entry1"), ],
                                            leader_commit=node.state.commit_index)

    node.replicate_log(2)

    node.send_message_callback.assert_called_with(2, expected_message)


def test_replicate_log_on_leader_standard_log(node):
    node.state.current_term = 1
    node.state.current_role = Role.LEADER
    node.state.current_leader = node.node_id
    node.state.log = [Entry(1, "entry1"), Entry(1, "entry2"), Entry(1, "entry3")]
    node.state.last_log_index = 2
    node.state.last_log_term = 1
    node.state.next_index = {2: 1, 3: 2, 4: 3, 5: 0}
    node.send_message_callback = MagicMock()

    expected_message = AppendEntriesRequest(term=node.state.current_term,
                                            leader_id=node.state.current_leader,
                                            previous_log_index=0,
                                            previous_log_term=1,
                                            entries=[Entry(1, "entry2"), Entry(1, "entry3")],
                                            leader_commit=node.state.commit_index)

    node.replicate_log(2)

    node.send_message_callback.assert_called_with(2, expected_message)

    expected_message = AppendEntriesRequest(term=node.state.current_term,
                                            leader_id=node.state.current_leader,
                                            previous_log_index=1,
                                            previous_log_term=1,
                                            entries=[Entry(1, "entry3")],
                                            leader_commit=node.state.commit_index)

    node.replicate_log(3)

    node.send_message_callback.assert_called_with(3, expected_message)

    expected_message = AppendEntriesRequest(term=node.state.current_term,
                                            leader_id=node.state.current_leader,
                                            previous_log_index=2,
                                            previous_log_term=1,
                                            entries=[],
                                            leader_commit=node.state.commit_index)

    node.replicate_log(4)

    node.send_message_callback.assert_called_with(4, expected_message)

    expected_message = AppendEntriesRequest(term=node.state.current_term,
                                            leader_id=node.state.current_leader,
                                            previous_log_index=-1,
                                            previous_log_term=0,
                                            entries=[Entry(1, "entry1"), Entry(1, "entry2"), Entry(1, "entry3")],
                                            leader_commit=node.state.commit_index)

    node.replicate_log(5)

    node.send_message_callback.assert_called_with(5, expected_message)


def test_on_append_entries_request_returns_false_if_term_less_than_current_term(node):
    node.state.current_term = 2
    node.state.log = [Entry(term=1, message="entry1"), Entry(term=1, message="entry1"), Entry(term=2, message="entry1")]
    node.state.last_log_index = 2
    node.send_message_callback = MagicMock()
    append_entries_request = AppendEntriesRequest(term=1,
                                                  leader_id=1,
                                                  previous_log_index=0,
                                                  previous_log_term=1,
                                                  entries=[],
                                                  leader_commit=0)
    node.on_append_entries_request(append_entries_request)

    node.send_message_callback.assert_called_once_with(1, AppendEntriesResponse(term=2,
                                                                                node_id=1,
                                                                                success=False))


def test_on_append_entries_request_returns_false_if_term_more_then_previous_term_index(node):
    node.state.current_term = 2
    node.state.log = [Entry(term=1, message="entry1"), Entry(term=1, message="entry1"), Entry(term=2, message="entry1")]
    node.state.last_log_index = 2
    node.send_message_callback = MagicMock()
    append_entries_request = AppendEntriesRequest(term=1,
                                                  leader_id=1,
                                                  previous_log_index=2,
                                                  previous_log_term=1,
                                                  entries=[],
                                                  leader_commit=0)

    node.on_append_entries_request(append_entries_request)

    node.send_message_callback.assert_called_once_with(1, AppendEntriesResponse(term=2,
                                                                                node_id=1,
                                                                                success=False))


def test_on_append_entries_request_on_candidate_with_less_term_become_follower_and_replicate_entries(node):
    node.state.current_term = 1
    node.state.current_role = Role.CANDIDATE
    node.state.voted_for = 1
    node.state.log = [Entry(term=1, message="entry1"), Entry(term=1, message="entry1"), Entry(term=1, message="entry1")]
    node.state.last_log_index = 2
    node.cancel_election_timer_callback = MagicMock()
    node.append_entries = MagicMock()
    node.send_message_callback = MagicMock()

    append_entries_request = AppendEntriesRequest(term=2,
                                                  leader_id=2,
                                                  previous_log_index=2,
                                                  previous_log_term=1,
                                                  entries=[],
                                                  leader_commit=2)

    node.on_append_entries_request(append_entries_request)

    assert node.state.current_term == 2
    assert node.state.current_role == Role.FOLLOWER
    node.cancel_election_timer_callback.assert_called_once()
    assert node.state.current_leader == 2
    assert node.state.voted_for is None
    node.append_entries.assert_called_once_with(append_entries_request.previous_log_index,
                                                append_entries_request.leader_commit,
                                                append_entries_request.entries)

    node.send_message_callback.assert_called_once_with(2, AppendEntriesResponse(term=2,
                                                                                node_id=1,
                                                                                last_log_index=2,
                                                                                success=True))


def test_on_append_entries_request_appends_entries_if_previous_log_index_and_term_match(node):
    node.state.log = [Entry(term=1, message="entry1"), Entry(term=1, message="entry2"), Entry(term=1, message="entry3")]
    node.state.last_log_index = 2
    node.cancel_election_timer_callback = MagicMock()
    node.append_entries = MagicMock()
    node.send_message_callback = MagicMock()

    append_entries_request = AppendEntriesRequest(term=1,
                                                  leader_id=2,
                                                  previous_log_index=2,
                                                  previous_log_term=1,
                                                  entries=[Entry(term=1, message="entry4")],
                                                  leader_commit=3)

    node.on_append_entries_request(append_entries_request)

    assert node.state.current_role == Role.FOLLOWER
    assert node.state.current_leader == 2

    node.append_entries.assert_called_once_with(append_entries_request.previous_log_index,
                                                append_entries_request.leader_commit,
                                                append_entries_request.entries)

    node.send_message_callback.assert_called_once_with(2,
                                                       AppendEntriesResponse(term=1,
                                                                             node_id=1,
                                                                             last_log_index=node.state.last_log_index,
                                                                             success=True))


def test_on_append_entries_response_on_follower(node):
    node.state.current_role = Role.FOLLOWER
    node.state.current_term = 1
    node.cancel_election_timer_callback = MagicMock()
    node.commit_log_entries = MagicMock()
    node.replicate_log = MagicMock()
    append_entries_response = AppendEntriesResponse(term=1, node_id=1, success=True, last_log_index=0)
    node.on_append_entries_response(append_entries_response)

    assert node.cancel_election_timer_callback.call_count == 0
    assert node.commit_log_entries.call_count == 0
    assert node.replicate_log.call_count == 0


def test_on_append_entries_response_on_leader_with_lower_term(node):
    node.state.current_role = Role.LEADER
    node.state.current_term = 1
    node.cancel_election_timer_callback = MagicMock()
    node.commit_log_entries = MagicMock()
    node.replicate_log = MagicMock()
    append_entries_response = AppendEntriesResponse(term=2, node_id=1, success=False)
    node.on_append_entries_response(append_entries_response)

    assert node.cancel_election_timer_callback.call_count == 1
    assert node.state.current_term == 2
    assert node.state.current_role == Role.FOLLOWER
    assert node.state.voted_for is None


def test_on_append_entries_response_on_leader_with_term_ok_and_success(node):
    node.state.current_role = Role.LEADER
    node.state.current_term = 1
    node.state.match_index = {2: 0}
    node.state.next_index = {2: 0}
    node.cancel_election_timer_callback = MagicMock()
    node.commit_log_entries = MagicMock()
    node.replicate_log = MagicMock()
    append_entries_response = AppendEntriesResponse(term=1, node_id=2, success=True, last_log_index=3)
    node.on_append_entries_response(append_entries_response)

    assert node.state.current_term == 1
    assert node.state.current_role == Role.LEADER
    assert node.state.match_index[2] == append_entries_response.last_log_index
    assert node.state.next_index[2] == append_entries_response.last_log_index + 1
    assert node.commit_log_entries.call_count == 1


def test_on_append_entries_response_on_leader_with_term_ok_and_not_success(node):
    node.state.current_role = Role.LEADER
    node.state.current_term = 1
    node.state.match_index = {2: 0}
    node.state.next_index = {2: 2}
    node.cancel_election_timer_callback = MagicMock()
    node.commit_log_entries = MagicMock()
    node.replicate_log = MagicMock()
    append_entries_response = AppendEntriesResponse(term=1, node_id=2, success=False, last_log_index=3)
    node.on_append_entries_response(append_entries_response)

    assert node.state.current_term == 1
    assert node.state.current_role == Role.LEADER
    assert node.state.next_index[2] == 1
    node.replicate_log.assert_called_once_with(2)


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


def test_append_entries_truncates_the_log_if_last_log_index_more_then_previous_log_index_and_terms_not_match(node):
    node.state = FakeState()
    node.state.log = [Entry(term=1, message="entry1"), Entry(term=1, message="entry1"), Entry(term=2, message="entry1")]
    node.state.commit_index = 1

    node.deliver_changes_callback = MagicMock()

    node.append_entries(1, 3, [Entry(term=3, message="entry3")])

    assert node.state.log == [Entry(term=1, message="entry1"), Entry(term=1, message="entry1"),
                              Entry(term=3, message="entry3")]

    assert node.state.commit_index == 3
    node.deliver_changes_callback.assert_has_calls([call("entry1"), call("entry3")])


def test_append_entries_truncates_the_log_if_last_log_index_much_more_then_previous_log_index_and_terms_not_match(node):
    node.state = FakeState()
    node.state.log = [Entry(term=1, message="entry1"), Entry(term=1, message="entry1"), Entry(term=2, message="entry2"),
                      Entry(term=2, message="entry1"), Entry(term=2, message="entry1"), Entry(term=2, message="entry1"),
                      Entry(term=2, message="entry1"), Entry(term=2, message="entry1")]
    node.state.commit_index = 1

    node.deliver_changes_callback = MagicMock()

    node.append_entries(2, 2, [Entry(term=3, message="entry1")])

    assert node.state.log == [Entry(term=1, message="entry1"), Entry(term=1, message="entry1"),
                              Entry(term=2, message="entry2"), Entry(term=3, message="entry1")]

    assert node.state.commit_index == 2
    node.deliver_changes_callback.assert_called_once_with("entry1")
