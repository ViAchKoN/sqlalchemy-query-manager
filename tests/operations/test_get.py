import pytest
from sqlalchemy.engine import Row

from sqlalchemy_query_manager.core.base import AsyncModelQueryManager, ModelQueryManager
from tests import models_factory
from tests.models import Item


@pytest.fixture
def item_sql_query_manager(sync_db_sessionmaker):
    class InjectedItem(Item, ModelQueryManager):
        sessionmaker = sync_db_sessionmaker

    return InjectedItem


@pytest.fixture
def async_item_sql_query_manager(async_db_sessionmaker):
    class InjectedItem(Item, AsyncModelQueryManager):
        sessionmaker = async_db_sessionmaker

    return InjectedItem


def test_get_object(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    item = models_factory.ItemFactory.create()

    models_factory.ItemFactory.create()

    returned_obj = item_sql_query_manager.query_manager.get(id=item.id)

    assert returned_obj.id == item.id


def test_where_object_only(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    item = models_factory.ItemFactory.create()

    models_factory.ItemFactory.create()

    returned_obj_record = item_sql_query_manager.query_manager.where(id=item.id).only(Item.id).first()

    assert isinstance(returned_obj_record, Row)

    assert returned_obj_record.id == 1


@pytest.mark.asyncio
async def test_async_get_object(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    item = models_factory.ItemFactory.create()

    models_factory.ItemFactory.create()

    returned_obj = await async_item_sql_query_manager.query_manager.get(id=item.id)

    assert returned_obj.id == item.id


@pytest.mark.asyncio
async def test_async_where_object_only(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    item = models_factory.ItemFactory.create()

    models_factory.ItemFactory.create()

    returned_obj_record = await async_item_sql_query_manager.query_manager.where(id=item.id).only(Item.id).first()

    assert isinstance(returned_obj_record, Row)

    assert returned_obj_record.id == 1


@pytest.mark.asyncio
async def test_async_where_object_only_field_as_str(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    item = models_factory.ItemFactory.create()

    models_factory.ItemFactory.create()

    returned_obj_record = await async_item_sql_query_manager.query_manager.where(id=item.id).only('id').first()

    assert isinstance(returned_obj_record, Row)

    assert returned_obj_record.id == 1
