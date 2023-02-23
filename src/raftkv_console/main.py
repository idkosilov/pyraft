import argparse

from raftkv_console.key_value_console import RaftKeyValueStorageShell


def main():
    parser = argparse.ArgumentParser(description='RAFT KV store')

    parser.add_argument('--db', dest='db_file', required=True,
                        help='Path to the database file')

    parser.add_argument('--address', dest='address', required=True,
                        help='Address to bind to')

    parser.add_argument('--cluster', dest='cluster', required=True, nargs='+',
                        help='List of addresses of nodes in the cluster')

    args = parser.parse_args()

    shell = RaftKeyValueStorageShell(db_file=args.db_file, address=args.address, cluster=args.cluster)
    shell.cmdloop()


if __name__ == "__main__":
    main()
