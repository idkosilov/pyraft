from math import ceil
from typing import Callable, Optional, Any

from raftkv.messages import Message, VoteRequest, VoteResponse
from raftkv.state import AbstractState, Role, Entry


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
                                   last_log_index=len(self.state.log) - 1,
                                   last_log_term=self.state.last_log_term)

        recipients_vote_request = self.nodes_ids.difference((self.node_id,))

        for node_id in recipients_vote_request:
            self.send_message_callback(node_id, vote_request)

        self.start_election_timer_callback()

    def on_vote_request(self, vote_request: VoteRequest) -> None:
        """
        Handles a received vote request from another node.

        :param vote_request: the vote request received from another node.
        """
        if vote_request.term > self.state.current_term:
            self.state.current_term = vote_request.term
            self.state.current_role = Role.FOLLOWER
            self.state.voted_for = None

        is_term_ok = vote_request.term == self.state.current_term
        is_log_up_to_date = vote_request.last_log_term > self.state.last_log_term or (
                vote_request.last_log_term == self.state.last_log_term and
                vote_request.last_log_index >= self.state.last_log_index)
        already_voted_for_this_candidate_or_did_not_voted = self.state.voted_for in (vote_request.candidate_id, None)

        if is_term_ok and is_log_up_to_date and already_voted_for_this_candidate_or_did_not_voted:
            self.state.voted_for = vote_request.candidate_id
            message = VoteResponse(term=self.state.current_term,
                                   node_id=self.node_id,
                                   granted=True)
        else:
            message = VoteResponse(term=self.state.current_term,
                                   node_id=self.node_id,
                                   granted=False)

        self.send_message_callback(vote_request.candidate_id, message)

    def on_vote_response(self, vote_response: VoteResponse) -> None:
        """
        Handles a received vote response from another node.

        :param vote_response: the vote response received from another node.
        """
        if self.state.current_role == Role.CANDIDATE and \
                self.state.current_term == vote_response.term and vote_response.granted:
            self.state.votes_received.add(vote_response.node_id)
            if len(self.state.votes_received) >= ceil((len(self.nodes_ids) + 1) / 2):
                self.state.current_role = Role.LEADER
                self.state.current_leader = self.node_id
                self.cancel_election_timer_callback()
                followers_ids = self.nodes_ids.difference((self.node_id,))
                for follower_id in followers_ids:
                    self.state.next_index[follower_id] = len(self.state.log)
                    self.state.match_index[follower_id] = 0
                    self.replicate_log(follower_id)
        elif vote_response.term > self.state.current_term:
            self.state.current_term = vote_response.term
            self.state.current_role = Role.FOLLOWER
            self.state.voted_for = None
            self.cancel_election_timer_callback()

    def on_client_request(self, message: Any) -> None:
        if self.state.current_role == Role.LEADER:
            entry = Entry(message=message, term=self.state.current_term)
            self.state.log.append(entry)
            self.state.match_index[self.node_id] = self.state.last_log_index
            followers_ids = self.nodes_ids.difference((self.node_id,))
            for follower_id in followers_ids:
                self.replicate_log(follower_id)
        else:
            self.send_message_callback(self.state.current_leader, message)

    def on_heartbeat(self) -> None:
        if self.state.current_role == Role.LEADER:
            followers_ids = self.nodes_ids.difference((self.node_id,))
            for follower_id in followers_ids:
                self.replicate_log(follower_id)

    def replicate_log(self, follower_id: int) -> None:
        ...
        # TODO
