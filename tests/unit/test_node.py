from unittest.mock import MagicMock

from raftkv.messages import VoteRequest, VoteResponse
from raftkv.state import Role


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
        last_log_index=len(node.state.log),
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
