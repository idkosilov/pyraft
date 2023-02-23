from raftkv import commands
from raftkv.key_value_storage import KeyValueStorage


class Node:

    def __init__(self, database: KeyValueStorage, address: str, cluster: list[str]) -> None:
        self._database = database
        self._address = address
        self._cluster = cluster

    def handle_command(self, command: commands.Command) -> None:
        if isinstance(command, commands.Set):
            with self._database:
                self._database[command.key] = command.value
        elif isinstance(command, commands.Delete):
            with self._database:
                del self._database[command.key]