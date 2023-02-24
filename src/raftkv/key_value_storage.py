from __future__ import annotations

import pickle
from os import PathLike
from collections.abc import MutableMapping
from typing import Iterator, Any


class KeyValueStorage(MutableMapping[str, Any]):
    """
    A key-value storage that uses the dbm module to persist data.

    This class provides a simple key-value storage API. It uses the dbm module to store data
    persistently on disk. The storage can be used as a context manager to ensure that the dbm
    database is properly closed when the storage object is no longer needed.
    """
    def __init__(self, filename: str | PathLike, write_back: bool = False) -> None:
        """
        Initialize the key-value storage.

        :param filename: the filename of the dbm database.
        :param write_back: whether to cache data in memory and write it back to the database when
                           the storage object is closed or when the `sync()` method is called.
        """
        self._data = None
        self._filename = filename
        self._write_back = write_back
        self._cache = {}

    def __enter__(self, mode: str = "c") -> KeyValueStorage:
        """
        Enter a context to use the key-value storage.

        :return: the key-value storage object.
        """
        self.open(mode)
        return self

    def __exit__(self, exc_type: type[Exception], exc_val: Exception, exc_tb) -> None:
        """
        Exit the context used to use the key-value storage.

        """
        self.close()

    def open(self, mode: str = "c") -> None:
        """
        This method opens the dbm database and returns the storage object.
        """
        import dbm

        self._data = dbm.open(self._filename, mode)

    def close(self) -> None:
        """
        This method closes the dbm database.
        """

        self.sync()
        self._data.close()

    def __delitem__(self, key: str) -> None:
        """
        Delete an item from the key-value storage.

        This method deletes an item from the dbm database and the cache.

        :param key: the key of the item to delete.
        """
        del self._data[key.encode()]

        if key in self._cache:
            del self._cache[key]

    def __getitem__(self, key: str) -> Any:
        """
        Get an item from the key-value storage.

        This method retrieves an item from the dbm database. If the `write_back` flag is set, the
        item is also cached in memory for later write-back to the database.

        :param key: the key of the item to retrieve.
        :return: the value of the item.
        """
        if key in self._cache:
            value = self._cache[key]
        else:
            value = pickle.loads(self._data[key.encode()])

            if self._write_back:
                self._cache[key] = value

        return value

    def __len__(self) -> int:
        """
        Returns the number of items in KeyValueStorage.

        :return: the number of items in KeyValueStorage.
        """
        return len(self._data)

    def __iter__(self) -> Iterator[str]:
        """
        Returns an iterator over the keys in KeyValueStorage.

        :return: an iterator over the keys in KeyValueStorage.
        """
        for key in self._data.keys():
            yield key.decode()

    def __setitem__(self, key: str, value: Any) -> None:
        """
        Sets a key-value pair in KeyValueStorage.

        :param key: the key to set.
        :param value: the value to set for the key.
        """
        if self._write_back:
            self._cache[key] = value

        self._data[key.encode()] = pickle.dumps(value)

    def sync(self) -> None:
        """
        Writes changes to disk if write_back is True and cache is not empty.
        """
        if self._write_back and self._cache:
            self._write_back = False
            for key, entry in self._cache.items():
                self[key] = entry
            self._write_back = True
            self._cache = {}
        if hasattr(self._data, 'sync'):
            self._data.sync()
