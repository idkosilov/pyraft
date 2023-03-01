from math import ceil
from typing import Callable, Optional, Any

from raft.messages import Message, VoteRequest, VoteResponse, AppendEntriesRequest, AppendEntriesResponse, ClientRequest
from raft.state import AbstractState, Role, Entry
from raft.timer import HeartbeatTimer, ElectionTimer


class Node:
    def __init__(self,
                 node_id: int,
                 nodes_ids: set[int],
                 state: AbstractState,
                 election_timer: ElectionTimer,
                 heartbeat_timer: HeartbeatTimer) -> None:
        """
        Initialize a Raft node with a given ID, set of nodes IDs, and raft state.

        :param node_id: ID of the node.
        :param nodes_ids: IDs of all nodes in the Raft cluster.
        :param state: AbstractState implementation object that holds the Raft node state.
        :param election_timer: election timer object.
        :param heartbeat_timer: heartbeat timer object.
        """
        self.node_id = node_id
        self.nodes_ids = nodes_ids
        self.state = state
        self.election_timer = election_timer
        self.election_timer.set_on_election_timeout(self.on_election_timeout_or_leader_fault)
        self.heartbeat_timer = heartbeat_timer
        self.heartbeat_timer.set_on_heartbeat_callback(self.on_heartbeat)
        self.send_message_callback: Optional[Callable[[int, Message], None]] = None
        self.deliver_changes_callback: Optional[Callable[[Any], None]] = None

    def set_send_message_callback(self, send_message_callback: Optional[Callable[[int, Message], None]]) -> None:
        """
        Set the send_message_callback.

        :param send_message_callback: Callback function for sending messages to other Raft nodes.
        """
        self.send_message_callback = send_message_callback

    def set_deliver_changes_callback(self, deliver_changes_callback: Optional[Callable[[Any], None]]) -> None:
        """
        Set the deliver_changes_callback.

        :param deliver_changes_callback: Callback function for delivering the committed changes to the application.
        """
        self.deliver_changes_callback = deliver_changes_callback

    def on_election_timeout_or_leader_fault(self) -> None:
        """
        Called when the election timer or heartbeat timer times out.

        If the node is not a leader, become a candidate, start a new election term,
        vote for itself, and send vote requests to all other nodes.
        """
        if self.state.current_role != Role.LEADER:
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

    def on_vote_request(self, vote_request: VoteRequest) -> None:
        """
        Called when the node receives a vote request message from a candidate. The method sends a vote response message
        to the candidate, indicating whether the node grants the vote.

        If the vote is granted, the node updates its state to indicate that it has voted for the candidate.

        If the vote is not granted, the node does not change its state.

        If the candidate's term is greater than the node's current term, the node updates its
        term and changes its role to follower.

        If the candidate's term is equal to the node's current term and the candidate's log is up-to-date with the
        node's log, and the node has not already voted for another candidate, the node grants the vote.

        If the candidate's term is equal to the node's current term and the candidate's log is not up-to-date with
        the node's log, or the node has already voted for another candidate, the node does not grant the vote.

        param: vote_request: the vote request message.
        """
        if vote_request.term > self.state.current_term:
            self.state.current_term = vote_request.term
            self.state.current_role = Role.FOLLOWER
            self.state.voted_for = None

        is_term_ok = vote_request.term == self.state.current_term

        is_log_up_to_date = vote_request.last_log_term > self.state.last_log_term or \
            (vote_request.last_log_term == self.state.last_log_term and
             vote_request.last_log_index >= self.state.last_log_index)

        already_voted_for_this_candidate_or_did_not_voted = self.state.voted_for in (vote_request.candidate_id, None)

        if is_term_ok and is_log_up_to_date and already_voted_for_this_candidate_or_did_not_voted:
            self.state.voted_for = vote_request.candidate_id
            message = VoteResponse(term=self.state.current_term, node_id=self.node_id, granted=True)
        else:
            message = VoteResponse(term=self.state.current_term, node_id=self.node_id, granted=False)
        self.send_message_callback(vote_request.candidate_id, message)

    def on_vote_response(self, vote_response: VoteResponse) -> None:
        """
        Handle a vote response received from another node in the network.

        Updates the node's state based on the contents of the vote response message.
        Cancels the election timer if the node becomes a follower.
        Replicates the node's log to all followers if the node becomes a leader.

        :param vote_response: a VoteResponse object representing the vote response received from another node.
        """
        if self.state.current_role == Role.CANDIDATE and \
                self.state.current_term == vote_response.term and vote_response.granted:
            self.state.votes_received.add(vote_response.node_id)
            # If the node is a candidate, the received vote response is for the same term as the node's current term,
            # and the vote was granted, update the node's vote count and change role to leader
            # if a majority of nodes have voted for it.
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
            # If the received vote response is for a higher term than the node's current term,
            # update the node's current term and become a follower.
            self.state.current_term = vote_response.term
            self.state.current_role = Role.FOLLOWER
            self.state.voted_for = None
            self.election_timer.cancel()

    def on_client_request(self, message: ClientRequest) -> None:
        """
        Handle incoming requests from clients.

        :param message: a ClientRequest object.
        """
        if self.state.current_role == Role.LEADER:
            entry = Entry(message=message.message, term=self.state.current_term)
            self.state.log.append(entry)
            self.state.match_index[self.node_id] = self.state.last_log_index
            followers_ids = self.nodes_ids.difference((self.node_id,))
            for follower_id in followers_ids:
                self.replicate_log(follower_id)
        else:
            self.send_message_callback(self.state.current_leader, message)

    def on_heartbeat(self) -> None:
        """
        Called when the node receives a heartbeat timeout. The method replicates logs for every follower in cluster.
        """
        if self.state.current_role == Role.LEADER:
            followers_ids = self.nodes_ids.difference((self.node_id,))
            for follower_id in followers_ids:
                self.replicate_log(follower_id)

    def on_append_entries_request(self, append_entries_request: AppendEntriesRequest) -> None:
        """
        Called when a node receives an AppendEntriesRequest from the leader.

        :param append_entries_request: an instance of AppendEntriesRequest,
        containing the information sent by the leader.
        """
        # Update the current term and clear the voted_for value if the incoming term is greater than the node's
        # current term.
        if append_entries_request.term > self.state.current_term:
            self.state.current_term = append_entries_request.term
            self.state.voted_for = None
        # If the incoming term matches the current term, update the current_role, current_leader and continue processing
        if append_entries_request.term == self.state.current_term:
            self.state.current_role = Role.FOLLOWER
            self.state.current_leader = append_entries_request.leader_id

        # Verify if the log entries in the incoming AppendEntriesRequest are correct and if the term is correct
        previous_log_index = append_entries_request.previous_log_index
        previous_log_term = append_entries_request.previous_log_term
        is_log_ok = self.state.last_log_index >= previous_log_index and (
                previous_log_index == -1 or self.state.log[previous_log_term].term == previous_log_term)

        # If the term is correct and the log entries are valid,
        # append the entries and send a success message to the leader
        if append_entries_request.term == self.state.current_term and is_log_ok:
            self.append_entries(append_entries_request.previous_log_index, append_entries_request.leader_commit,
                                append_entries_request.entries)
            message = AppendEntriesResponse(term=self.state.current_term, node_id=self.node_id,
                                            last_log_index=self.state.last_log_index, success=True)
            self.election_timer.cancel()
        # If the term is incorrect or the log entries are invalid, send a failure message to the leader
        else:
            message = AppendEntriesResponse(term=self.state.current_term, node_id=self.node_id, success=False)

        # Send the response to the leader
        self.send_message_callback(append_entries_request.leader_id, message)

    def on_append_entries_response(self, append_entries_response: AppendEntriesResponse) -> None:
        """
        Handles the response to an Append Entries.

        If the response is from the current term and the current role is LEADER, the node updates its state.
        If the response is successful and the match index is greater than or equal to the next index of the node,
        the next index is updated and the leader tries to commit the log entries.
        If the response is unsuccessful, the next index is decremented and log replication is retried.
        If the response has a higher term than the node's current term,
        the node steps down as follower and updates its term.

        :param append_entries_response: the response to an Append Entries.
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
        Replicates log entries to the given follower.
        Sends an Append Entries request to the given follower with the entries to be replicated.

        :param follower_id: the id of the follower node to which the entries are to be replicated.
        """
        previous_log_index = self.state.next_index[follower_id] - 1
        entries = self.state.log[previous_log_index + 1:]
        previous_log_term = 0
        if previous_log_index > -1:
            previous_log_term = self.state.log[previous_log_index].term
        message = AppendEntriesRequest(term=self.state.current_term, leader_id=self.node_id,
                                       previous_log_index=previous_log_index, previous_log_term=previous_log_term,
                                       entries=entries, leader_commit=self.state.commit_index)
        self.send_message_callback(follower_id, message)

    def append_entries(self, previous_log_index: int, leader_commit: int, entries: list[Entry]) -> None:
        """
        Appends new entries to the log and updates the commit index if necessary.

        If the new entries are not duplicates of existing entries, they are added to the log.
        If the leader's commit index is greater than the follower's commit index, the follower updates its commit index.

        :param previous_log_index: the index of the previous log entry on the leader.
        :param leader_commit: the index of the last entry known to be committed on the leader.
        :param entries: the log entries to be appended.
        """
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
        """
        Commits log entries that have been replicated on a majority of nodes in the cluster.
        """
        # Iterate over the logs to check which logs can be committed
        while self.state.commit_index < self.state.last_log_index:
            acks = 0
            # Count the number of nodes that have replicated the log entry at commit index or higher
            for node_id in self.nodes_ids:
                if self.state.match_index[node_id] > self.state.commit_index:
                    acks += 1
            # If a majority of nodes have replicated the log entry, commit it and apply to the state machine
            if acks >= ceil((len(self.nodes_ids) + 1) / 2):
                entry = self.state.log[self.state.commit_index]
                self.deliver_changes_callback(entry.message)
                self.state.commit_index += 1
            else:
                break
