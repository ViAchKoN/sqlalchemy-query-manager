import pytest

from sqlalchemy_query_manager.core.helpers import Q
from tests import models_factory


def test_update_raw__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    item = models_factory.ItemFactory.create(name="old")

    rowcount = item_sql_query_manager.query_manager.where(id=item.id).update_raw(
        name="new_name", number=999, is_valid=False
    )

    assert rowcount == 1


def test_update_raw__multiple__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    items = models_factory.ItemFactory.create_batch(size=3)
    models_factory.ItemFactory.create()

    ids = [item.id for item in items]
    rowcount = item_sql_query_manager.query_manager.where(id__in=ids).update_raw(
        name="bulk_updated"
    )

    assert rowcount == 3


def test_update_raw__with_q_filter__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    models_factory.ItemFactory.create(is_valid=True)
    models_factory.ItemFactory.create(number=999)
    models_factory.ItemFactory.create(is_valid=False, number=1)

    rowcount = item_sql_query_manager.query_manager.where(
        Q(is_valid=True) | Q(number=999)
    ).update_raw(name="updated_via_q")

    assert rowcount == 2


def test_update_raw__no_filters__raises__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    import pytest as pt

    with pt.raises(ValueError, match="Cannot update without filters"):
        item_sql_query_manager.query_manager.update_raw(name="boom")


@pytest.mark.asyncio
async def test_async_update_raw__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    item = models_factory.ItemFactory.create(name="old")

    rowcount = await async_item_sql_query_manager.query_manager.where(
        id=item.id
    ).update_raw(name="new_name", number=999, is_valid=False)

    assert rowcount == 1


@pytest.mark.asyncio
async def test_async_update_raw__with_q_filter__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    models_factory.ItemFactory.create(is_valid=True)
    models_factory.ItemFactory.create(number=999)
    models_factory.ItemFactory.create(is_valid=False, number=1)

    rowcount = await async_item_sql_query_manager.query_manager.where(
        Q(is_valid=True) | Q(number=999)
    ).update_raw(name="updated_via_q")

    assert rowcount == 2
