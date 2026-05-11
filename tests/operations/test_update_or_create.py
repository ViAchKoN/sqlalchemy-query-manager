import pytest

from tests import models_factory


def test_update_or_create__updates_existing__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    item = models_factory.ItemFactory.create()

    obj, created = item_sql_query_manager.query_manager.update_or_create(
        id=item.id,
        defaults={"name": "updated_name", "number": 999, "is_valid": False},
    )

    assert created is False
    assert obj.id == item.id
    assert obj.name == "updated_name"
    assert obj.number == 999
    assert obj.is_valid is False


def test_update_or_create__creates_new__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    obj, created = item_sql_query_manager.query_manager.update_or_create(
        name="new_name",
        number=999,
        is_valid=False,
    )

    assert created is True
    assert obj.name == "new_name"
    assert obj.number == 999
    assert obj.is_valid is False


def test_update_or_create__creates_new_with_defaults__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    obj, created = item_sql_query_manager.query_manager.update_or_create(
        name="new_name",
        defaults={"number": 999, "is_valid": False},
    )

    assert created is True
    assert obj.name == "new_name"
    assert obj.number == 999
    assert obj.is_valid is False


@pytest.mark.asyncio
async def test_async_update_or_create__updates_existing__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    item = models_factory.ItemFactory.create()

    obj, created = await async_item_sql_query_manager.query_manager.update_or_create(
        id=item.id,
        defaults={"name": "updated_name", "number": 999, "is_valid": False},
    )

    assert created is False
    assert obj.id == item.id
    assert obj.name == "updated_name"
    assert obj.number == 999
    assert obj.is_valid is False


@pytest.mark.asyncio
async def test_async_update_or_create__creates_new__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    obj, created = await async_item_sql_query_manager.query_manager.update_or_create(
        name="new_name",
        number=999,
        is_valid=False,
    )

    assert created is True
    assert obj.name == "new_name"
    assert obj.number == 999
    assert obj.is_valid is False


@pytest.mark.asyncio
async def test_async_update_or_create__creates_new_with_defaults__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    obj, created = await async_item_sql_query_manager.query_manager.update_or_create(
        name="new_name",
        defaults={"number": 999, "is_valid": False},
    )

    assert created is True
    assert obj.name == "new_name"
    assert obj.number == 999
    assert obj.is_valid is False
