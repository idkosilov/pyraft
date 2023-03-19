from queue import Queue
from threading import Thread, Event
from typing import Optional

import zmq

from raft.configuration import ZmqNodeConfiguration
from raft.messages import Message, VoteRequest, VoteResponse, AppendEntriesRequest, AppendEntriesResponse, ClientRequest
from raft.node import Node


class Server:

    def __init__(self, node: Node, cluster: list[ZmqNodeConfiguration]) -> None:
        self.node = node
        self.cluster = cluster
        self.context = zmq.Context.instance()
        self.recv_socket = self.context.socket(zmq.REP)
        self.send_sockets: dict[int, zmq.Socket] = {node.node_id: self.context.socket(zmq.REQ) for node in cluster
                                                    if node.node_id != self.node.node_id}
        self.send_queues: dict[int, Queue] = {node.node_id: Queue() for node in cluster
                                              if node.node_id != self.node.node_id}

        self.workers: list[Thread] = []
        self.stopped = Event()

        self.node.set_send_message_callback(self.send_message_to)

    def handle_recv(self) -> None:
        node_config = next(node for node in self.cluster if node.node_id == self.node.node_id)

        self.recv_socket.bind(node_config.url())

        while not self.stopped.is_set():
            try:
                message: Message = self.recv_socket.recv_pyobj()
                self.recv_socket.send_string("ok")
            except (zmq.ContextTerminated, zmq.ZMQError):
                break

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

    def handle_send(self, node: ZmqNodeConfiguration) -> None:
        send_socket = self.send_sockets[node.node_id]
        send_queue = self.send_queues[node.node_id]

        send_socket.connect(node.url())

        while not self.stopped.is_set():
            message: Optional[Message] = send_queue.get()
            if not isinstance(message, Message):
                break

            try:
                send_socket.send_pyobj(message)
                send_socket.recv_string()
            except (zmq.ContextTerminated, zmq.ZMQError):
                break

    def send_message_to(self, node_id: int, message: Message) -> None:
        self.send_queues[node_id].put(message)

    def start(self) -> None:
        self.workers = [Thread(target=self.handle_send, args=(node,), daemon=True) for node in self.cluster
                        if node.node_id != self.node.node_id]
        self.workers.append(Thread(target=self.handle_recv, daemon=True))

        for worker in self.workers:
            worker.start()

    def stop(self) -> None:
        self.stopped.set()

        for send_queue in self.send_queues.values():
            send_queue.put(None)

        for send_socket in self.send_sockets.values():
            send_socket.close()

        self.recv_socket.close()
