import pytest

from sqlalchemy_query_manager.core.base import (
    AsyncModelQueryManagerMixin,
    ModelQueryManagerMixin,
)
from tests.models import Item


@pytest.fixture
def item_sql_query_manager(sync_db_sessionmaker):
    class InjectedItem(Item, ModelQueryManagerMixin):
        class QueryManagerConfig:
            session = sync_db_sessionmaker

    return InjectedItem


@pytest.fixture
def async_item_sql_query_manager(async_db_sessionmaker):
    class InjectedItem(Item, AsyncModelQueryManagerMixin):
        class QueryManagerConfig:
            session = async_db_sessionmaker

    return InjectedItem
