import cmd
from os import PathLike

from raftkv.key_value_storage import KeyValueStorage


class RaftKeyValueStorageShell(cmd.Cmd):
    intro = "Welcome to the raft key-value storage shell. Type help or ? to list commands.\n"
    prompt = "rkvs> "

    def __init__(self, db_file: str | PathLike) -> None:
        super().__init__()
        self.db = KeyValueStorage(db_file)

    def do_set(self, arg: str) -> None:
        """
        Set a key-value pair in the store.

        Usage: set <key> <value>
        """
        key, value = arg.split()

        with self.db:
            self.db[key] = value

    def do_get(self, arg):
        """
        Get the value for a given key from the store.

        Usage: get <key>
        """
        with self.db:
            value = self.db.get(arg)

        if value is not None:
            print(value)
        else:
            print("Key not found")

    def do_delete(self, arg):
        """
        Delete a key-value pair from the store.

        Usage: delete <key>
        """
        with self.db:
            if arg in self.db:
                del self.db[arg]
            else:
                print("Key not found")

    def do_list(self, _):
        """
        List all keys in the store.

        Usage: list
        """
        with self.db:
            for key in self.db:
                print(key)

    @staticmethod
    def do_quit(_):
        """
        Quit the shell.

        Usage: quit
        """
        return True
