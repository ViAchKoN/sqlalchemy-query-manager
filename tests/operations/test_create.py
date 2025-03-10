import pytest


@pytest.fixture()
def create_kwargs():
    return {
        "name": "new_name",
        "number": 999,
        "is_valid": False,
    }


def test_create_new_object__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
    create_kwargs,
):
    created_obj = item_sql_query_manager.query_manager.create(**create_kwargs)

    created_obj_data = created_obj.as_dict()

    assert created_obj_data["name"] == "new_name"
    assert created_obj_data["number"] == 999
    assert created_obj_data["is_valid"] is False


@pytest.mark.asyncio
async def test_async_create_new_object__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
    create_kwargs,
):
    created_obj = await async_item_sql_query_manager.query_manager.create(
        **create_kwargs
    )

    created_obj_data = created_obj.as_dict()

    assert created_obj_data["name"] == "new_name"
    assert created_obj_data["number"] == 999
    assert created_obj_data["is_valid"] is False
