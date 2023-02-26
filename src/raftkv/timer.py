from random import randint
from threading import Thread, Event
from typing import Callable, Optional


class ElectionTimer:
    """
    Timer for managing elections in a distributed system.

    :param election_timeout_lower: The lower bound for the election timeout.
    :type election_timeout_lower: int
    :param election_timeout_upper: The upper bound for the election timeout.
    :type election_timeout_upper: int
    """
    def __init__(self, election_timeout_lower: int, election_timeout_upper: int) -> None:
        super().__init__()
        self.election_timeout_lower: int = election_timeout_lower
        self.election_timeout_upper: int = election_timeout_upper
        self.on_election_timeout: Optional[Callable[[], None]] = None
        self.daemon: bool = True
        self.cancelled: Event = Event()
        self.is_running: bool = False

    def election_timeout(self) -> int:
        """
        Returns a random integer between the lower and upper bounds
        for the election timeout.

        :return: A random integer.
        :rtype: int
        """
        return randint(self.election_timeout_lower, self.election_timeout_upper)

    def set_on_election_timeout(self, on_election_timeout: Callable[[], None]) -> None:
        """
        Sets the function to be called when an election timeout occurs.

        :param on_election_timeout: The function to call.
        :type on_election_timeout: Callable[[], None]
        """
        self.on_election_timeout = on_election_timeout

    def run(self) -> None:
        """
        Starts the timer, and calls the function set using
        set_on_election_timeout when an election timeout occurs.
        """
        self.is_running = True

        while self.is_running:
            self.cancelled.wait(self.election_timeout())
            if not self.cancelled.is_set() and self.on_election_timeout is not None:
                self.on_election_timeout()
            self.cancelled.clear()

    def stop(self) -> None:
        """
        Stops the timer.
        """
        self.is_running = False
        self.cancelled.set()

    def cancel(self) -> None:
        """
        Cancels the timer.
        """
        self.cancelled.set()


class HeartbeatTimer(Thread):
    """
    Timer for managing heartbeats in a distributed system.

    :param heartbeat_timeout: The timeout for the heartbeat.
    :type heartbeat_timeout: int
    """
    def __init__(self, heartbeat_timeout: int) -> None:
        super().__init__()
        self.heartbeat_timeout: int = heartbeat_timeout
        self.on_heartbeat_callback: Optional[Callable[[], None]] = None
        self.daemon: bool = True
        self.cancelled: Event = Event()
        self.is_running: bool = False

    def set_on_heartbeat_callback(self, on_heartbeat_callback: Callable[[], None]) -> None:
        """
        Sets the function to be called when a heartbeat is sent.

        :param on_heartbeat_callback: The function to call.
        :type on_heartbeat_callback: Callable[[], None]
        """
        self.on_heartbeat_callback = on_heartbeat_callback

    def run(self) -> None:
        """
        Starts the timer, and calls the function set using
        set_on_heartbeat_callback when a heartbeat is sent.
        """
        self.is_running = True

        while self.is_running:
            self.cancelled.wait(self.heartbeat_timeout)
            if not self.cancelled.is_set() and self.on_heartbeat_callback is not None:
                self.on_heartbeat_callback()
            self.cancelled.clear()

    def stop(self) -> None:
        """
        Stops the timer.
        """
        self.is_running = False
        self.cancelled.set()
