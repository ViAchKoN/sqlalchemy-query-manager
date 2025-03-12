from contextlib import asynccontextmanager, contextmanager

import pytest
from sqlalchemy.orm import Session

from sqlalchemy_query_manager.core.base import (
    AsyncModelQueryManagerMixin,
    ModelQueryManagerMixin,
)
from tests import models_factory
from tests.models import Item


def test_sync_context_manager_session(
    create_tables,
    sync_db_sessionmaker,
):
    @contextmanager
    def session_scope() -> Session:
        session = sync_db_sessionmaker()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    class InjectedItem(Item, ModelQueryManagerMixin):
        class QueryManagerConfig:
            session = session_scope()

    item = models_factory.ItemFactory.create()

    returned_obj = InjectedItem.query_manager.get(id=item.id)

    assert returned_obj.id == item.id


@pytest.mark.asyncio
async def test_async_context_manager_session(
    create_tables,
    async_db_sessionmaker,
):
    @asynccontextmanager
    async def async_session_scope() -> Session:
        session = async_db_sessionmaker()
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

    class InjectedItem(Item, AsyncModelQueryManagerMixin):
        class QueryManagerConfig:
            session = async_session_scope()

    item = models_factory.ItemFactory.create()

    returned_obj = await InjectedItem.query_manager.get(id=item.id)

    assert returned_obj.id == item.id
