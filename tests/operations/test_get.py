import pytest

from tests import models_factory


def test_get_object__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    item = models_factory.ItemFactory.create()

    models_factory.ItemFactory.create()

    returned_obj = item_sql_query_manager.query_manager.get(id=item.id)

    assert returned_obj.id == item.id


def test_get_object__multiple_filters__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    item = models_factory.ItemFactory.create()

    models_factory.ItemFactory.create()

    returned_obj = item_sql_query_manager.query_manager.get(id=item.id, name=item.name)

    assert returned_obj.id == item.id


@pytest.mark.asyncio
async def test_async_get_object__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    item = models_factory.ItemFactory.create()

    models_factory.ItemFactory.create()

    returned_obj = await async_item_sql_query_manager.query_manager.get(id=item.id)

    assert returned_obj.id == item.id


@pytest.mark.asyncio
async def test_async_get_object__multiple_filters__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    item = models_factory.ItemFactory.create()

    models_factory.ItemFactory.create()

    returned_obj = await async_item_sql_query_manager.query_manager.get(id=item.id, name=item.name)

    assert returned_obj.id == item.id
