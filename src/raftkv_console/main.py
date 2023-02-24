import argparse

from raftkv_console.key_value_console import RaftKeyValueStorageShell


def main():
    parser = argparse.ArgumentParser(description='RAFT KV store')

    parser.add_argument('--db', dest='db_file', required=True,
                        help='Path to the database file')

    args = parser.parse_args()

    shell = RaftKeyValueStorageShell(db_file=args.db_file)

    try:
        shell.cmdloop()
    except Exception as err:
        print(err)
        shell.do_quit()


if __name__ == "__main__":
    main()
