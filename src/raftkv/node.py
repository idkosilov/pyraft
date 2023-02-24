from typing import Callable, Optional, Any

from raftkv.messages import Message, VoteRequest
from raftkv.state import AbstractState, Role


class Node:
    """
    A class representing a node in a Raft cluster.
    """

    def __init__(self, node_id: int, nodes_ids: set[int], state: AbstractState) -> None:
        """
        Initializes a new instance of the Node class.

        :param node_id: the unique ID of the node.
        :param nodes_ids: a set of IDs of all nodes in the cluster.
        :param state: the state object containing the node's current state.
        """
        self.node_id = node_id
        self.nodes_ids = nodes_ids
        self.state = state

        self.send_message_callback: Optional[Callable[[int, Message], None]] = None
        self.deliver_changes_callback: Optional[Callable[[Any], None]] = None
        self.cancel_election_timer_callback: Optional[Callable[[], None]] = None
        self.start_election_timer_callback: Optional[Callable[[], None]] = None

    def on_election_timeout_or_leader_fault(self) -> None:
        """
        The function to be called when an election timeout or leader fault occurs.
        Initiates a new election and sends VoteRequest messages to all other nodes in the cluster.
        """
        self.state.current_term += 1
        self.state.current_role = Role.CANDIDATE
        self.state.voted_for = self.node_id
        self.state.votes_received = {self.node_id, }

        vote_request = VoteRequest(term=self.state.current_term,
                                   candidate_id=self.node_id,
                                   last_log_index=len(self.state.log),
                                   last_log_term=self.state.last_log_term)

        recipients_vote_request = self.nodes_ids.difference((self.node_id, ))

        for node_id in recipients_vote_request:
            self.send_message_callback(node_id, vote_request)

        self.start_election_timer_callback()
