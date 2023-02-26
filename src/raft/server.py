from queue import Queue
from threading import Thread, Event
from typing import Callable, Optional, Any

import zmq

from raft.configuration import NodeConfiguration


class Server:

    def __init__(self, node_id: int, cluster: dict[int, NodeConfiguration]) -> None:
        self.node_id = node_id
        self.cluster = cluster
        self.outgoing_messages_queue = Queue()

        self.is_running_event = Event()

        self.context = zmq.Context()
        self.incoming_socket = self.context.socket(zmq.REP)
        self.outgoing_sockets = {
            node_id: self.context.socket(zmq.REQ)
            for node_id in self.cluster if node_id != self.node_id
        }

        self.outgoing_queues = {
            node_id: Queue() for node_id in self.cluster if node_id != self.node_id
        }

        self.incoming_messages_worker: Optional[Thread] = None
        self.messages_dispatcher_worker: Optional[Thread] = None
        self.outgoing_messages_workers: Optional[list[Thread]] = None

        self.deliver_to_node_callback: Optional[Callable[[Any], None]] = None

    def handle_incoming_messages(self) -> None:
        while self.is_running_event.is_set():
            message = self.incoming_socket.recv_pyobj()
            self.incoming_socket.send_string("ok")
            self.deliver_to_node_callback(message)

    def handle_outgoing_messages(self) -> None:
        while self.is_running_event.is_set():
            node_id, message = self.outgoing_messages_queue.get()
            outgoing_queue = self.outgoing_queues[node_id]
            outgoing_queue.put(message)

    def send_outgoing_messages_to(self, node_id: int) -> None:
        outgoing_socket = self.outgoing_sockets[node_id]
        outgoing_socket.setsockopt(zmq.RCVTIMEO, 1000)
        outgoing_queue = self.outgoing_queues[node_id]
        outgoing_socket.connect(f"tcp://{self.cluster[node_id].host}:{self.cluster[node_id].port}")

        retries = 3
        while self.is_running_event.is_set():
            message = outgoing_queue.get()
            for _ in range(retries):
                try:
                    outgoing_socket.send_pyobj(message)
                    status = outgoing_socket.recv_string()
                    break
                except zmq.error.Again as e:
                    print(f"No message received within 1 second: {e}")
            else:
                print(f"Failed to send message after {retries} retries")
                continue

    def start(self) -> None:
        if self.is_running_event.is_set():
            return
        self.is_running_event.set()

        self.incoming_messages_worker = Thread(target=self.handle_incoming_messages, daemon=True)
        self.messages_dispatcher_worker = Thread(target=self.handle_incoming_messages, daemon=True)
        self.outgoing_messages_workers = [
            Thread(target=self.send_outgoing_messages_to, args=(node_id,), daemon=True)
            for node_id in self.cluster.keys() if node_id != self.node_id
        ]
        self.incoming_messages_worker.start()
        self.messages_dispatcher_worker.start()
        for outgoing_messages_worker in self.outgoing_messages_workers:
            outgoing_messages_worker.start()

    def stop(self) -> None:
        if not self.is_running_event.is_set():
            return
        self.is_running_event.clear()

        self.incoming_socket.close()
        for outgoing_socket in self.outgoing_sockets.values():
            outgoing_socket.close()

        self.messages_dispatcher_worker.join()

        if self.incoming_messages_worker is not None:
            self.outgoing_messages_queue.put((None, None), block=False)
            self.incoming_messages_worker.join()

        if self.outgoing_messages_workers is not None:
            for node_id, _ in self.outgoing_queues.items():
                self.outgoing_queues[node_id].put(None, block=False)
            for outgoing_messages_worker in self.outgoing_messages_workers:
                outgoing_messages_worker.join()
            self.outgoing_messages_workers = None

        self.incoming_messages_worker = None
        self.messages_dispatcher_worker = None
