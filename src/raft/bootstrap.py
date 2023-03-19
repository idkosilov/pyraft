from time import sleep

from raft.configuration import RaftConfiguration, ZmqNodeConfiguration
from raft.node import Node
from raft.server import Server
from raft.state import State
from raft.timer import ElectionTimer, HeartbeatTimer


class RaftBootstrap:

    def __init__(self, raft_configuration: RaftConfiguration) -> None:
        self.configuration = raft_configuration

        self.state = State(self.configuration.storage_path)

        self.election_timer = ElectionTimer(self.configuration.election_timeout_lower,
                                            self.configuration.election_timeout_upper)
        self.heartbeat_timer = HeartbeatTimer(self.configuration.heartbeat_timeout)

        self.node = Node(self.configuration.node_id,
                         {node.node_id for node in self.configuration.cluster},
                         self.state,
                         self.election_timer,
                         self.heartbeat_timer)

        self.server = Server(self.node, self.configuration.cluster)

    def start(self) -> None:
        self.state.open()
        self.server.start()
        self.election_timer.start()
        self.heartbeat_timer.start()

    def stop(self) -> None:
        self.heartbeat_timer.stop()
        self.election_timer.stop()
        self.server.stop()
        self.state.close()


if __name__ == "__main__":
    bootstrap = RaftBootstrap(
        raft_configuration=RaftConfiguration(
            node_id=1,
            storage_path=f"state",
            heartbeat_timeout=100,
            election_timeout_lower=300,
            election_timeout_upper=600,
            cluster=[
                ZmqNodeConfiguration(
                    node_id=1,
                    host="127.0.0.1",
                    port=9999
                ),
                ZmqNodeConfiguration(
                    node_id=2,
                    host="127.0.0.1",
                    port=9998
                ),
                ZmqNodeConfiguration(
                    node_id=3,
                    host="127.0.0.1",
                    port=9997
                ),
            ]
        )
    )

    bootstrap.start()
    sleep(10)
    bootstrap.stop()
