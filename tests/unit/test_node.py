from unittest.mock import MagicMock

from raftkv.messages import VoteRequest
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
