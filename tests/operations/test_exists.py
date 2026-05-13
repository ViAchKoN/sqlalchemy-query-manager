import pytest

from sqlalchemy_query_manager.core.helpers import Q
from tests import models_factory


def test_exists__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    models_factory.ItemFactory.create(name="target")

    assert item_sql_query_manager.query_manager.exists(name="target") is True
    assert item_sql_query_manager.query_manager.exists(name="missing") is False


def test_exists__with_where__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    models_factory.ItemFactory.create(name="target", is_valid=True)
    models_factory.ItemFactory.create(name="target", is_valid=False)

    assert (
        item_sql_query_manager.query_manager.where(is_valid=True).exists(name="target")
        is True
    )
    assert (
        item_sql_query_manager.query_manager.where(is_valid=True).exists(name="missing")
        is False
    )


def test_exists__with_q_filter__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    models_factory.ItemFactory.create(is_valid=True, number=1)
    models_factory.ItemFactory.create(is_valid=False, number=999)

    assert (
        item_sql_query_manager.query_manager.where(
            Q(is_valid=True) | Q(number=999)
        ).exists()
    ) is True

    assert (
        item_sql_query_manager.query_manager.where(
            Q(is_valid=True) | Q(number=999)
        ).exists(name="nonexistent_name_xyz")
    ) is False


def test_exists__q_filter_not_lost_with_kwargs__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    """
    Regression: .where(Q(...)).exists(kwarg=val) must preserve Q-filters.
    Previously _q_filters were dropped when kwargs were passed to exists().
    """
    models_factory.ItemFactory.create(is_valid=True, number=42)
    # This item matches Q filter but not extra kwarg
    models_factory.ItemFactory.create(is_valid=True, number=1)

    result = item_sql_query_manager.query_manager.where(
        Q(is_valid=True) | Q(number=999)
    ).exists(number=42)

    assert result is True


@pytest.mark.asyncio
async def test_async_exists__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    models_factory.ItemFactory.create(name="async_target")

    assert (
        await async_item_sql_query_manager.query_manager.exists(name="async_target")
        is True
    )
    assert (
        await async_item_sql_query_manager.query_manager.exists(name="missing") is False
    )


@pytest.mark.asyncio
async def test_async_exists__with_q_filter__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    models_factory.ItemFactory.create(is_valid=True, number=1)
    models_factory.ItemFactory.create(is_valid=False, number=999)

    assert (
        await async_item_sql_query_manager.query_manager.where(
            Q(is_valid=True) | Q(number=999)
        ).exists()
    ) is True
