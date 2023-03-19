from raft.state import Role


def test_election_process(cluster_bootstraps):
    leader = None
    while not leader:
        for cluster_bootstrap in cluster_bootstraps:
            if cluster_bootstrap.node.state.current_role == Role.LEADER:
                leader = cluster_bootstrap
                break

    for cluster_bootstrap in cluster_bootstraps:
        if cluster_bootstrap is not leader:
            assert cluster_bootstrap.node.state.current_role != Role.LEADER

    leader.stop()
    leader.node.state.current_role = Role.FOLLOWER

    leader = None

    while not leader:
        for cluster_bootstrap in cluster_bootstraps:
            if cluster_bootstrap.node.state.current_role == Role.LEADER:
                leader = cluster_bootstrap
                break

    for cluster_bootstrap in cluster_bootstraps:
        if cluster_bootstrap is not leader:
            assert cluster_bootstrap.node.state.current_role != Role.LEADER
