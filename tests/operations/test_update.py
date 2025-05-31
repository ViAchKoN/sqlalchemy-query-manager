import pytest

from tests import models_factory


@pytest.fixture()
def update_kwargs():
    return {
        "name": "new_name",
        "number": 999,
        "is_valid": False,
    }


def test_update_object__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
    update_kwargs,
):
    item = models_factory.ItemFactory.create()

    updated_obj = item_sql_query_manager.query_manager.where(id=item.id).update(
        **update_kwargs
    )

    updated_obj_data = updated_obj.as_dict()

    assert updated_obj_data["name"] == "new_name"
    assert updated_obj_data["number"] == 999
    assert updated_obj_data["is_valid"] is False


def test_update_objects__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
    update_kwargs,
):
    items = models_factory.ItemFactory.create_batch(size=3)
    item_ids = [item.id for item in items]

    updated_objs = item_sql_query_manager.query_manager.where(id__in=item_ids).update(
        **update_kwargs
    )

    for updated_obj in updated_objs:
        updated_obj_data = updated_obj.as_dict()

        assert updated_obj_data["name"] == "new_name"
        assert updated_obj_data["number"] == 999
        assert updated_obj_data["is_valid"] is False


@pytest.mark.asyncio
async def test_async_update_object__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
    update_kwargs,
):
    item = models_factory.ItemFactory.create()

    updated_obj = await async_item_sql_query_manager.query_manager.where(
        id=item.id
    ).update(**update_kwargs)

    updated_obj_data = updated_obj.as_dict()

    assert updated_obj_data["name"] == "new_name"
    assert updated_obj_data["number"] == 999
    assert updated_obj_data["is_valid"] is False


@pytest.mark.asyncio
async def test_async_update_objects__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
    update_kwargs,
):
    items = models_factory.ItemFactory.create_batch(size=3)
    item_ids = [item.id for item in items]

    updated_objs = await async_item_sql_query_manager.query_manager.where(
        id__in=item_ids
    ).update(**update_kwargs)

    for updated_obj in updated_objs:
        updated_obj_data = updated_obj.as_dict()

        assert updated_obj_data["name"] == "new_name"
        assert updated_obj_data["number"] == 999
        assert updated_obj_data["is_valid"] is False
