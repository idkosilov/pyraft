from threading import Thread

import zmq

from raft.configuration import ZmqNodeConfiguration
from raft.messages import Message, VoteRequest, VoteResponse, AppendEntriesRequest, AppendEntriesResponse, ClientRequest
from raft.node import Node


class Server:

    def __init__(self, node: Node, cluster: list[ZmqNodeConfiguration]) -> None:
        self.worker = None
        self.node = node
        self.node.set_send_message_callback(self.publish)
        self.cluster = cluster
        self.context = zmq.Context.instance()

        self.pub_socket = self.context.socket(zmq.PUB)

        node_config = next(node for node in self.cluster if node.node_id == self.node.node_id)

        self.pub_socket.bind(node_config.url())

        self.sub_socket = self.context.socket(zmq.SUB)

        for node in self.cluster:
            if node.node_id != self.node.node_id:
                self.sub_socket.connect(node.url())

        self.sub_socket.subscribe(b"")

        self.is_running = False

    def publish(self, node_id: int, message: Message) -> None:
        self.pub_socket.send_pyobj({"to": node_id, "message": message})

    def listen(self) -> None:
        while self.is_running:
            message: dict = self.sub_socket.recv_pyobj()

            if message.get("to") == self.node.node_id:
                message = message["message"]
                if isinstance(message, VoteRequest):
                    self.node.on_vote_request(message)
                elif isinstance(message, VoteResponse):
                    self.node.on_vote_response(message)
                elif isinstance(message, AppendEntriesRequest):
                    self.node.on_append_entries_request(message)
                elif isinstance(message, AppendEntriesResponse):
                    self.node.on_append_entries_response(message)
                elif isinstance(message, ClientRequest):
                    self.node.on_client_request(message)

    def start(self) -> None:
        self.is_running = True
        self.worker = Thread(target=self.listen, daemon=True)
        self.worker.start()

    def stop(self) -> None:
        self.is_running = False
        self.sub_socket.close()
        self.pub_socket.close()

