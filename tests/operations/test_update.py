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


def test_update__with_q_filter__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
    update_kwargs,
):
    from sqlalchemy_query_manager.core.helpers import Q

    expected_1 = models_factory.ItemFactory.create(is_valid=True)
    expected_2 = models_factory.ItemFactory.create(number=999)
    models_factory.ItemFactory.create(is_valid=False, number=1)

    updated = item_sql_query_manager.query_manager.where(
        Q(is_valid=True) | Q(number=999)
    ).update(**update_kwargs)

    updated_list = updated if isinstance(updated, list) else [updated]
    assert len(updated_list) == 2
    assert {obj.id for obj in updated_list} == {expected_1.id, expected_2.id}
    for obj in updated_list:
        data = obj.as_dict()
        assert data["name"] == "new_name"
        assert data["number"] == 999
        assert data["is_valid"] is False


@pytest.mark.asyncio
async def test_async_update__with_q_filter__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
    update_kwargs,
):
    from sqlalchemy_query_manager.core.helpers import Q

    expected_1 = models_factory.ItemFactory.create(is_valid=True)
    expected_2 = models_factory.ItemFactory.create(number=999)
    models_factory.ItemFactory.create(is_valid=False, number=1)

    updated = await async_item_sql_query_manager.query_manager.where(
        Q(is_valid=True) | Q(number=999)
    ).update(**update_kwargs)

    updated_list = updated if isinstance(updated, list) else [updated]
    assert len(updated_list) == 2
    assert {obj.id for obj in updated_list} == {expected_1.id, expected_2.id}
    for obj in updated_list:
        data = obj.as_dict()
        assert data["name"] == "new_name"
        assert data["number"] == 999
        assert data["is_valid"] is False
