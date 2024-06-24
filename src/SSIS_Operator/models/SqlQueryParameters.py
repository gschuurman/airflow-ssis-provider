from enum import Enum
from dataclasses import dataclass


class LoggingLevel(Enum):
    none: int = 0
    basic: int = 1
    performance: int = 2
    verbose: int = 3
    runtime_lineage: int = 4
    custom: int = 100


class ParameterType(Enum):
    PROJECT: int = 20
    PACKAGE: int = 30
    LOGGING: int = 50


@dataclass
class QueryParameters:
    name: str
    value: str
    type: ParameterType
