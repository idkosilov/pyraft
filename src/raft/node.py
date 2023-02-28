from math import ceil
from typing import Callable, Optional, Any

from raft.messages import Message, VoteRequest, VoteResponse, AppendEntriesRequest, AppendEntriesResponse
from raft.state import AbstractState, Role, Entry
from raft.timer import HeartbeatTimer, ElectionTimer


class Node:
    """
    A class representing a node in a Raft cluster.
    """

    def __init__(self,
                 node_id: int,
                 nodes_ids: set[int],
                 state: AbstractState,
                 election_timer: ElectionTimer,
                 heartbeat_timer: HeartbeatTimer) -> None:
        """
        Initializes a new instance of the Node class.

        :param node_id: the unique ID of the node.
        :param nodes_ids: a set of IDs of all nodes in the cluster.
        :param state: the state object containing the node's current state.
        """
        self.node_id = node_id
        self.nodes_ids = nodes_ids
        self.state = state

        self.election_timer = election_timer
        self.election_timer.set_on_election_timeout(self.on_election_timeout_or_leader_fault)
        self.election_timer.start()

        self.heartbeat_timer = heartbeat_timer
        self.heartbeat_timer.set_on_heartbeat_callback(self.on_heartbeat)
        self.heartbeat_timer.start()

        self.send_message_callback: Optional[Callable[[int, Message], None]] = None
        self.deliver_changes_callback: Optional[Callable[[Any], None]] = None

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

        self.election_timer.cancel()

        for node_id in recipients_vote_request:
            self.send_message_callback(node_id, vote_request)

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
                self.election_timer.cancel()
                followers_ids = self.nodes_ids.difference((self.node_id,))
                for follower_id in followers_ids:
                    self.state.next_index[follower_id] = len(self.state.log)
                    self.state.match_index[follower_id] = 0
                    self.replicate_log(follower_id)
        elif vote_response.term > self.state.current_term:
            self.state.current_term = vote_response.term
            self.state.current_role = Role.FOLLOWER
            self.state.voted_for = None
            self.election_timer.cancel()

    def on_client_request(self, message: Any) -> None:
        """
        Receives a client request message and if the node is the current leader, appends the message to its log
        and replicates the log to all other nodes in the cluster.

        :param message: the message received from the client.
        """
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
        """
        Sends AppendEntriesRequests to all followers in the cluster to replicate log entries.
        """
        if self.state.current_role == Role.LEADER:
            followers_ids = self.nodes_ids.difference((self.node_id,))
            for follower_id in followers_ids:
                self.replicate_log(follower_id)

    def on_append_entries_request(self, append_entries_request: AppendEntriesRequest) -> None:
        """
        Handles a received append entries request from another node.

        :param append_entries_request: the append entries request received from another node.
        """

        if append_entries_request.term > self.state.current_term:
            self.state.current_term = append_entries_request.term
            self.state.voted_for = None
            self.election_timer.cancel()

        if append_entries_request.term == self.state.current_term:
            self.state.current_role = Role.FOLLOWER
            self.state.current_leader = append_entries_request.leader_id

        previous_log_index = append_entries_request.previous_log_index
        previous_log_term = append_entries_request.previous_log_term
        is_log_ok = self.state.last_log_index >= previous_log_index and (
                previous_log_index == -1 or self.state.log[previous_log_term].term == previous_log_term)

        if append_entries_request.term == self.state.current_term and is_log_ok:
            self.append_entries(append_entries_request.previous_log_index,
                                append_entries_request.leader_commit,
                                append_entries_request.entries)
            message = AppendEntriesResponse(term=self.state.current_term,
                                            node_id=self.node_id,
                                            last_log_index=self.state.last_log_index,
                                            success=True)
        else:
            message = AppendEntriesResponse(term=self.state.current_term,
                                            node_id=self.node_id,
                                            success=False)

        self.send_message_callback(append_entries_request.leader_id, message)

    def on_append_entries_response(self, append_entries_response: AppendEntriesResponse) -> None:
        """
        Handles a received AppendEntriesResponse from a follower.

        If the response is successful, updates the match index and next index of the follower.
        If the response is unsuccessful, decrements the next index of the follower and retries the AppendEntriesRequest.

        :param append_entries_response: the AppendEntriesResponse received from the follower.
        """
        if append_entries_response.term == self.state.current_term and self.state.current_role == Role.LEADER:
            if append_entries_response.success and \
                    append_entries_response.last_log_index >= self.state.match_index[append_entries_response.node_id]:
                self.state.next_index[append_entries_response.node_id] = append_entries_response.last_log_index + 1
                self.state.match_index[append_entries_response.node_id] = append_entries_response.last_log_index
                self.commit_log_entries()
            elif self.state.next_index[append_entries_response.node_id] > 0:
                self.state.next_index[append_entries_response.node_id] -= 1
                self.replicate_log(append_entries_response.node_id)
        elif append_entries_response.term > self.state.current_term:
            self.state.current_term = append_entries_response.term
            self.state.current_role = Role.FOLLOWER
            self.state.voted_for = None
            self.election_timer.cancel()

    def replicate_log(self, follower_id: int) -> None:
        """
        Sends AppendEntriesRequest messages to the follower to replicate log entries.

        :param follower_id: the ID of the follower node to which AppendEntriesRequest messages are sent.
        """
        previous_log_index = self.state.next_index[follower_id] - 1
        entries = self.state.log[previous_log_index + 1:]

        previous_log_term = 0
        if previous_log_index > -1:
            previous_log_term = self.state.log[previous_log_index].term

        message = AppendEntriesRequest(term=self.state.current_term,
                                       leader_id=self.node_id,
                                       previous_log_index=previous_log_index,
                                       previous_log_term=previous_log_term,
                                       entries=entries,
                                       leader_commit=self.state.commit_index)

        self.send_message_callback(follower_id, message)

    def append_entries(self, previous_log_index: int, leader_commit: int, entries: list[Entry]) -> None:
        if len(entries) > 0 and self.state.last_log_index > previous_log_index:
            index = min(self.state.last_log_index, previous_log_index + len(entries))
            if self.state.log[index].term != entries[index - previous_log_index - 1]:
                self.state.log = self.state.log[:previous_log_index + 1]

        if previous_log_index + len(entries) > self.state.last_log_index:
            self.state.log.extend(entries)

        if leader_commit > self.state.commit_index:
            for entry in self.state.log[self.state.commit_index: leader_commit]:
                self.deliver_changes_callback(entry.message)
            self.state.commit_index = leader_commit

    def commit_log_entries(self) -> None:
        while self.state.commit_index < self.state.last_log_index:
            acks = 0
            for node_id in self.nodes_ids:
                if self.state.match_index[node_id] > self.state.commit_index:
                    acks += 1
            if acks >= ceil((len(self.nodes_ids) + 1) / 2):
                entry = self.state.log[self.state.commit_index]
                self.deliver_changes_callback(entry.message)
                self.state.commit_index += 1
            else:
                break
