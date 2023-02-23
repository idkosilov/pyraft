import abc
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Command(abc.ABC):
    ...


@dataclass(frozen=True)
class Set(Command):
    key: str
    value: Any


@dataclass(frozen=True)
class Delete(Command):
    key: str
