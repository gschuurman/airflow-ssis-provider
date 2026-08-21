"""Value types shared by the SSIS operator for building parameterized executions."""

from enum import Enum
from dataclasses import dataclass


class LoggingLevel(Enum):
    """SSIS catalog logging levels, matching the `LOGGING_LEVEL` execution parameter values."""

    none: int = 0
    basic: int = 1
    performance: int = 2
    verbose: int = 3
    runtime_lineage: int = 4
    custom: int = 100


class ParameterType(Enum):
    """SSIS catalog `object_type` values accepted by `set_execution_parameter_value`."""

    PROJECT: int = 20
    PACKAGE: int = 30
    LOGGING: int = 50


@dataclass
class QueryParameters:
    """A single SSIS execution parameter to set before starting a package run.

    Example::

        QueryParameters(name="SourcePath", value="/mnt/data", type=ParameterType.PACKAGE)
    """

    name: str
    value: str
    type: ParameterType
