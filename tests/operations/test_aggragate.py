import pytest

from sqlalchemy_query_manager.core.helpers import Avg, Count, Max, Min, Q, Sum
from tests import models, models_factory


def test_aggregate__all_functions__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    models_factory.ItemFactory.create(number=10)
    models_factory.ItemFactory.create(number=20)
    models_factory.ItemFactory.create(number=30)

    result = item_sql_query_manager.query_manager.aggregate(
        total=Sum("number"),
        avg=Avg("number"),
        minimum=Min("number"),
        maximum=Max("number"),
        count=Count("id"),
    )

    assert result["total"] == 60
    assert result["avg"] == 20
    assert result["minimum"] == 10
    assert result["maximum"] == 30
    assert result["count"] == 3


def test_aggregate__with_where__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    models_factory.ItemFactory.create(number=10, is_valid=True)
    models_factory.ItemFactory.create(number=20, is_valid=True)
    models_factory.ItemFactory.create(number=30, is_valid=False)

    result = item_sql_query_manager.query_manager.where(is_valid=True).aggregate(
        total=Sum("number"),
        count=Count("id"),
    )

    assert result["total"] == 30
    assert result["count"] == 2


def test_aggregate__with_fk_where__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    active_group = models_factory.GroupFactory.create(is_active=True, with_item=True)
    active_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == active_group.id)
        .first()
    )
    active_item.number = 15
    db_session.commit()

    inactive_group = models_factory.GroupFactory.create(is_active=False, with_item=True)
    inactive_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == inactive_group.id)
        .first()
    )
    inactive_item.number = 85
    db_session.commit()

    result = item_sql_query_manager.query_manager.where(
        group__is_active=True
    ).aggregate(
        count=Count("id"),
        total=Sum("number"),
    )

    assert result["count"] == 1
    assert result["total"] == 15


def test_aggregate__with_q_filter__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    models_factory.ItemFactory.create(number=5, is_valid=True)
    models_factory.ItemFactory.create(number=50, is_valid=False)
    models_factory.ItemFactory.create(number=25, is_valid=False)  # unexpected

    result = item_sql_query_manager.query_manager.where(
        Q(is_valid=True) | Q(number__gt=40)
    ).aggregate(
        count=Count("id"),
        total=Sum("number"),
    )

    assert result["count"] == 2
    assert result["total"] == 55


def test_aggregate__empty_result__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    result = item_sql_query_manager.query_manager.where(is_valid=True).aggregate(
        count=Count("id"),
        total=Sum("number"),
    )

    assert result["count"] == 0
    assert result["total"] is None


@pytest.mark.asyncio
async def test_async_aggregate__all_functions__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    models_factory.ItemFactory.create(number=10)
    models_factory.ItemFactory.create(number=20)
    models_factory.ItemFactory.create(number=30)

    result = await async_item_sql_query_manager.query_manager.aggregate(
        total=Sum("number"),
        avg=Avg("number"),
        minimum=Min("number"),
        maximum=Max("number"),
        count=Count("id"),
    )

    assert result["total"] == 60
    assert result["avg"] == 20
    assert result["minimum"] == 10
    assert result["maximum"] == 30
    assert result["count"] == 3


@pytest.mark.asyncio
async def test_async_aggregate__with_fk_where__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    active_group = models_factory.GroupFactory.create(is_active=True, with_item=True)
    active_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == active_group.id)
        .first()
    )
    active_item.number = 15
    db_session.commit()

    inactive_group = models_factory.GroupFactory.create(is_active=False, with_item=True)
    inactive_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == inactive_group.id)
        .first()
    )
    inactive_item.number = 85
    db_session.commit()

    result = await async_item_sql_query_manager.query_manager.where(
        group__is_active=True
    ).aggregate(
        count=Count("id"),
        total=Sum("number"),
    )

    assert result["count"] == 1
    assert result["total"] == 15


@pytest.mark.asyncio
async def test_async_aggregate__with_q_filter__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    models_factory.ItemFactory.create(number=5, is_valid=True)
    models_factory.ItemFactory.create(number=50, is_valid=False)
    models_factory.ItemFactory.create(number=25, is_valid=False)  # unexpected

    result = await async_item_sql_query_manager.query_manager.where(
        Q(is_valid=True) | Q(number__gt=40)
    ).aggregate(
        count=Count("id"),
        total=Sum("number"),
    )

    assert result["count"] == 2
    assert result["total"] == 55


@pytest.mark.asyncio
async def test_async_aggregate__empty_result__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    result = await async_item_sql_query_manager.query_manager.where(
        is_valid=True
    ).aggregate(
        count=Count("id"),
        total=Sum("number"),
    )

    assert result["count"] == 0
    assert result["total"] is None
