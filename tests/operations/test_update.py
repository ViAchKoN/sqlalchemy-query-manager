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


# ---------------------------------------------------------------------------
# Regression tests for `update()` follow-up-SELECT bug.
#
# Previously `update()` ran the UPDATE, then issued a *second* SELECT with the
# same WHERE clause to fetch the updated rows. When the WHERE clause referenced
# a column that the UPDATE itself changed (or when UPDATE matched zero rows),
# the second SELECT returned no rows and `updated_objects[0]` raised
# IndexError. The fix is to use UPDATE ... RETURNING in a single statement.
# ---------------------------------------------------------------------------


def test_update__filter_targets_changed_column__returns_rows(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    """WHERE references is_valid=True; UPDATE sets is_valid=False.
    After UPDATE no row matches `is_valid=True` anymore - the follow-up
    SELECT used to return [], crashing on updated_objects[0]. Now the rows
    must be returned via RETURNING.
    """
    items = models_factory.ItemFactory.create_batch(size=3, is_valid=True)
    item_ids = {it.id for it in items}

    updated = item_sql_query_manager.query_manager.where(is_valid=True).update(
        is_valid=False, name="rotated"
    )

    updated_list = updated if isinstance(updated, list) else [updated]
    assert {obj.id for obj in updated_list} == item_ids
    for obj in updated_list:
        data = obj.as_dict()
        assert data["is_valid"] is False
        assert data["name"] == "rotated"


@pytest.mark.asyncio
async def test_async_update__filter_targets_changed_column__returns_rows(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    items = models_factory.ItemFactory.create_batch(size=3, is_valid=True)
    item_ids = {it.id for it in items}

    updated = await async_item_sql_query_manager.query_manager.where(
        is_valid=True
    ).update(is_valid=False, name="rotated")

    updated_list = updated if isinstance(updated, list) else [updated]
    assert {obj.id for obj in updated_list} == item_ids
    for obj in updated_list:
        data = obj.as_dict()
        assert data["is_valid"] is False
        assert data["name"] == "rotated"


def test_update__no_rows_matched__returns_empty_list(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    """UPDATE where no row matches the filter. Previously crashed with
    IndexError on `updated_objects[0]`. Must now return [] instead.
    """
    updated = item_sql_query_manager.query_manager.where(id=999_999).update(
        name="never"
    )
    assert updated == []


@pytest.mark.asyncio
async def test_async_update__no_rows_matched__returns_empty_list(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    updated = await async_item_sql_query_manager.query_manager.where(id=999_999).update(
        name="never"
    )
    assert updated == []


def test_update__changes_primary_key__returns_renamed(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    """UPDATE that changes the primary key referenced in WHERE. Previously
    the follow-up SELECT used the old id and returned []. Must now return
    the row with the new id.
    """
    item = models_factory.ItemFactory.create()
    old_id = item.id
    new_id = old_id + 1_000_000

    updated = item_sql_query_manager.query_manager.where(id=old_id).update(
        id=new_id, name="renamed"
    )

    assert not isinstance(updated, list)
    data = updated.as_dict()
    assert data["id"] == new_id
    assert data["name"] == "renamed"


@pytest.mark.asyncio
async def test_async_update__changes_primary_key__returns_renamed(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    item = models_factory.ItemFactory.create()
    old_id = item.id
    new_id = old_id + 1_000_000

    updated = await async_item_sql_query_manager.query_manager.where(id=old_id).update(
        id=new_id, name="renamed"
    )

    assert not isinstance(updated, list)
    data = updated.as_dict()
    assert data["id"] == new_id
    assert data["name"] == "renamed"
