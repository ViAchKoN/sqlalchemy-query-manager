import dataclasses
import enum
import typing

from sqlalchemy.orm import DeclarativeMeta


class JoinType(enum.Enum):
    """Enumeration of supported join types."""

    INNER = "inner"
    LEFT = "left"
    FULL = "full"


@dataclasses.dataclass
class JoinConfig:
    model: DeclarativeMeta
    join_type: JoinType = JoinType.INNER
    relationship_attr: typing.Any = None  # for contains_eager
