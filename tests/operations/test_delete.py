import pytest

from sqlalchemy_query_manager.core.helpers import Q
from tests import models_factory


def test_delete__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    item = models_factory.ItemFactory.create()
    models_factory.ItemFactory.create()

    deleted = item_sql_query_manager.query_manager.where(id=item.id).delete()

    assert deleted == 1
    remaining = item_sql_query_manager.query_manager.all()
    assert len(remaining) == 1
    assert remaining[0].id != item.id


def test_delete__multiple__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    items = models_factory.ItemFactory.create_batch(size=3)
    models_factory.ItemFactory.create()

    ids = [item.id for item in items]
    deleted = item_sql_query_manager.query_manager.where(id__in=ids).delete()

    assert deleted == 3
    remaining = item_sql_query_manager.query_manager.all()
    assert len(remaining) == 1


def test_delete__with_q_filter__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    models_factory.ItemFactory.create(is_valid=True)
    models_factory.ItemFactory.create(number=999)
    survivor = models_factory.ItemFactory.create(is_valid=False, number=1)

    deleted = item_sql_query_manager.query_manager.where(
        Q(is_valid=True) | Q(number=999)
    ).delete()

    assert deleted == 2
    remaining = item_sql_query_manager.query_manager.all()
    assert len(remaining) == 1
    assert remaining[0].id == survivor.id


def test_delete__no_filters__raises__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    import pytest as pt

    with pt.raises(ValueError, match="Cannot delete without filters"):
        item_sql_query_manager.query_manager.delete()


@pytest.mark.asyncio
async def test_async_delete__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
    item_sql_query_manager,
):
    item = models_factory.ItemFactory.create()
    models_factory.ItemFactory.create()

    deleted = await async_item_sql_query_manager.query_manager.where(
        id=item.id
    ).delete()

    assert deleted == 1
    remaining = item_sql_query_manager.query_manager.all()
    assert len(remaining) == 1


@pytest.mark.asyncio
async def test_async_delete__with_q_filter__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
    item_sql_query_manager,
):
    models_factory.ItemFactory.create(is_valid=True)
    models_factory.ItemFactory.create(number=999)
    survivor = models_factory.ItemFactory.create(is_valid=False, number=1)

    deleted = await async_item_sql_query_manager.query_manager.where(
        Q(is_valid=True) | Q(number=999)
    ).delete()

    assert deleted == 2
    remaining = item_sql_query_manager.query_manager.all()
    assert len(remaining) == 1
    assert remaining[0].id == survivor.id
