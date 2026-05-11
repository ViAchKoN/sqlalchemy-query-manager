import pytest

from tests import models_factory


def test_bulk_update__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    items = models_factory.ItemFactory.create_batch(size=3)

    data = [
        {"id": item.id, "name": "new_name", "number": 999, "is_valid": False}
        for item in items
    ]

    updated_objs = item_sql_query_manager.query_manager.bulk_update(data=data)

    assert len(updated_objs) == 3

    for updated_obj in updated_objs:
        updated_obj_data = updated_obj.as_dict()

        assert updated_obj_data["name"] == "new_name"
        assert updated_obj_data["number"] == 999
        assert updated_obj_data["is_valid"] is False


def test_bulk_update__empty_data__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    result = item_sql_query_manager.query_manager.bulk_update(data=[])

    assert result == []


@pytest.mark.asyncio
async def test_async_bulk_update__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    items = models_factory.ItemFactory.create_batch(size=3)

    data = [
        {"id": item.id, "name": "new_name", "number": 999, "is_valid": False}
        for item in items
    ]

    updated_objs = await async_item_sql_query_manager.query_manager.bulk_update(
        data=data
    )

    assert len(updated_objs) == 3

    for updated_obj in updated_objs:
        updated_obj_data = updated_obj.as_dict()

        assert updated_obj_data["name"] == "new_name"
        assert updated_obj_data["number"] == 999
        assert updated_obj_data["is_valid"] is False


@pytest.mark.asyncio
async def test_async_bulk_update__empty_data__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    result = await async_item_sql_query_manager.query_manager.bulk_update(data=[])

    assert result == []
