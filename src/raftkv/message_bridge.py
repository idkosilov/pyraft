import logging
from queue import Queue
from threading import Thread
from typing import Any, Optional, Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar('T')


class StopCommand:
    """A stop command to indicate to stop delivering messages."""
    ...


class MessageBridge:
    """
    The MessageBridge's role in connecting node and app and
    enabling them to communicate through message passing. This is done using
    a message queue, and each message is delivered in a separate thread.
    """
    def __init__(self) -> None:
        super().__init__()
        self.messages_queue_to_app: Queue[T] = Queue()
        self.messages_queue_to_node: Queue[T] = Queue()

        self.is_running = False

        self.worker_deliver_to_app: Optional[Thread] = None
        self.worker_deliver_to_node: Optional[Thread] = None

        self.deliver_to_app_callback: Optional[Callable[[T], None]] = None
        self.deliver_to_node_callback: Optional[Callable[[T], None]] = None

    def register_deliver_to_app_callback(self, deliver_to_app_callback: Callable[[T], None]) -> None:
        """
        Register a callback function for delivering messages to the app.

        :param deliver_to_app_callback: the callback function to register.
        :return: None
        """
        self.deliver_to_app_callback = deliver_to_app_callback

    def register_deliver_to_node_callback(self, deliver_to_node_callback: Callable[[T], None]) -> None:
        """
        Register a callback function for delivering messages to the node.

        :param deliver_to_node_callback: the callback function to register.
        """
        self.deliver_to_node_callback = deliver_to_node_callback

    def deliver_changes_to_app(self) -> None:
        """
        Deliver messages from the node to the app.
        """
        while self.is_running:
            message = self.messages_queue_to_app.get()
            if isinstance(message, StopCommand):
                break
            try:
                self.deliver_to_app_callback(message)
            except Exception as err:
                logger.exception("Exception occurred on deliver to app:", err)

    def deliver_changes_to_node(self) -> None:
        """
        Deliver messages from the app to the node.
        """
        while self.is_running:
            message = self.messages_queue_to_node.get()
            if isinstance(message, StopCommand):
                break
            try:
                self.deliver_to_node_callback(message)
            except Exception as err:
                logger.exception("Exception occurred on deliver to node:", err)

    def start(self) -> None:
        """
        Start delivering messages in separate threads.
        """
        self.is_running = True
        self.worker_deliver_to_app = Thread(target=self.deliver_changes_to_app)
        self.worker_deliver_to_app.start()

        self.worker_deliver_to_node = Thread(target=self.deliver_changes_to_node)
        self.worker_deliver_to_node.start()

    def stop(self) -> None:
        """
        Stop delivering messages and wait for the threads to finish.
        """
        self.is_running = False
        self.messages_queue_to_app.put(StopCommand())
        self.messages_queue_to_node.put(StopCommand())
        self.worker_deliver_to_node.join()
        self.worker_deliver_to_app.join()
        self.messages_queue_to_node.queue.clear()
        self.messages_queue_to_app.queue.clear()

    def handle_message_from_node(self, message: Any) -> None:
        """
        Add a message received from the node to the message queue for the app.

        :param message: The message received from the node.
        """
        self.messages_queue_to_app.put(message)

    def handle_message_from_app(self, message: Any) -> None:
        """
        Add a message received from the app to the message queue for the node.

        :param message: The message received from the app.
        """
        self.messages_queue_to_node.put(message)
