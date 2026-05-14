import pytest

from sqlalchemy_query_manager.core.exceptions import (
    DoesNotExist,
    MultipleObjectsReturned,
)
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

    returned_obj = await async_item_sql_query_manager.query_manager.get(
        id=item.id, name=item.name
    )

    assert returned_obj.id == item.id


def test_get__does_not_exist__raises__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):

    with pytest.raises(DoesNotExist):
        item_sql_query_manager.query_manager.get(id=999999)


def test_get__multiple_objects_returned__raises__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):

    models_factory.ItemFactory.create_batch(size=2, is_valid=True)

    with pytest.raises(MultipleObjectsReturned):
        item_sql_query_manager.query_manager.get(is_valid=True)


def test_get__with_where__preserves_filters__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    """get() must respect prior where() filters — previously _filters were ignored."""
    expected = models_factory.ItemFactory.create(is_valid=True, number=42)
    models_factory.ItemFactory.create(is_valid=False, number=42)

    result = item_sql_query_manager.query_manager.where(is_valid=True).get(number=42)

    assert result.id == expected.id


@pytest.mark.asyncio
async def test_async_get__does_not_exist__raises__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):

    with pytest.raises(DoesNotExist):
        await async_item_sql_query_manager.query_manager.get(id=999999)


@pytest.mark.asyncio
async def test_async_get__multiple_objects_returned__raises__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):

    models_factory.ItemFactory.create_batch(size=2, is_valid=True)

    with pytest.raises(MultipleObjectsReturned):
        await async_item_sql_query_manager.query_manager.get(is_valid=True)


@pytest.mark.asyncio
async def test_async_get__with_where__preserves_filters__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    expected = models_factory.ItemFactory.create(is_valid=True, number=42)
    models_factory.ItemFactory.create(is_valid=False, number=42)

    result = await async_item_sql_query_manager.query_manager.where(is_valid=True).get(
        number=42
    )

    assert result.id == expected.id
