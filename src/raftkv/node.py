from typing import Callable, Optional, Any

from raftkv.messages import Message
from raftkv.state import AbstractState


class Node:
    """
    A class representing a node in a Raft cluster.
    """

    def __init__(self, node_id: int, nodes: set[int], state: AbstractState) -> None:
        """
        Initializes a new instance of the Node class.

        :param node_id: the unique ID of the node.
        :param nodes: a set of IDs of all nodes in the cluster.
        :param state: the state object containing the node's current state.
        """
        self.node_id = node_id
        self.nodes_ids = nodes
        self.state = state

        # a callback function that sends a message to another node.
        self.send_message_callback: Optional[Callable[[int, Message], None]] = None

        # a callback function that deliver changes to the app.
        self.deliver_changes_callback: Optional[Callable[[Any], None]] = None

        # a callback function that cancels the node's election timer.
        self.cancel_election_timer_callback: Optional[Callable[[], None]] = None

        # a callback function that starts the node's election timer.
        self.start_election_timer_callback: Optional[Callable[[], None]] = None


