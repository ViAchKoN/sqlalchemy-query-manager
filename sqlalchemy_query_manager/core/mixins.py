import typing

from sqlalchemy_query_manager.consts import classproperty
from sqlalchemy_query_manager.core.async_query_manager import AsyncQueryManager
from sqlalchemy_query_manager.core.sync_query_manager import QueryManager


class BaseModelQueryManagerMixin:
    class QueryManagerConfig:
        session = None

    def as_dict(self) -> typing.Dict[str, str]:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}  # type: ignore


class ModelQueryManagerMixin(BaseModelQueryManagerMixin):
    @classproperty
    def query_manager(cls):
        return QueryManager(
            model=cls,
            session=getattr(cls.QueryManagerConfig, "session", None),
        )


class AsyncModelQueryManagerMixin(BaseModelQueryManagerMixin):
    @classproperty
    def query_manager(cls):
        return AsyncQueryManager(
            model=cls,
            session=getattr(cls.QueryManagerConfig, "session", None),
        )
