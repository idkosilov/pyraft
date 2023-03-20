from time import sleep

from raft.messages import ClientRequest
from raft.state import Role


def test_replication_process(cluster_bootstraps):
    leader = None
    while not leader:
        for cluster_bootstrap in cluster_bootstraps:
            if cluster_bootstrap.node.state.current_role == Role.LEADER:
                leader = cluster_bootstrap
                break

    messages = ["ADD 2", "ADD 3", "DELETE 2"]

    for message in messages:
        leader.node.on_client_request(ClientRequest(message))

    sleep(leader.election_timer.election_timeout_upper / 1000 * 2)

    for cluster_bootstrap in cluster_bootstraps:
        assert cluster_bootstrap.state.last_log_index == 2
        assert cluster_bootstrap.state.commit_index == 2
        assert [entry.message for entry in cluster_bootstrap.state.log] == messages

    for cluster_bootstrap in cluster_bootstraps:
        if cluster_bootstrap.state.current_role != Role.LEADER:
            for message in messages:
                cluster_bootstrap.node.on_client_request(ClientRequest(message))
            break

    sleep(leader.election_timer.election_timeout_upper / 1000 * 2)

    for cluster_bootstrap in cluster_bootstraps:
        assert cluster_bootstrap.state.last_log_index == 5
        assert cluster_bootstrap.state.commit_index == 5
        assert [entry.message for entry in cluster_bootstrap.state.log] == messages + messages
