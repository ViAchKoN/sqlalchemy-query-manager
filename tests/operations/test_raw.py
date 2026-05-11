import pytest

from tests import models, models_factory


def test_raw__simple_select_with_param__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    item = models_factory.ItemFactory.create(name="target")
    models_factory.ItemFactory.create_batch(size=3)

    results = item_sql_query_manager.query_manager.raw(
        "SELECT * FROM item WHERE id = :id",
        id=item.id,
    )

    assert len(results) == 1
    assert results[0]["id"] == item.id
    assert results[0]["name"] == "target"


def test_raw__multiple_params__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    expected = models_factory.ItemFactory.create(
        name="target", number=42, is_valid=True
    )
    models_factory.ItemFactory.create(name="target", number=42, is_valid=False)
    models_factory.ItemFactory.create(name="other", number=42, is_valid=True)

    results = item_sql_query_manager.query_manager.raw(
        "SELECT * FROM item WHERE name = :name AND number = :number AND is_valid = :is_valid",
        name="target",
        number=42,
        is_valid=True,
    )

    assert len(results) == 1
    assert results[0]["id"] == expected.id


def test_raw__join__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    active_group = models_factory.GroupFactory.create(
        name="active_group", is_active=True, with_item=True
    )
    active_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == active_group.id)
        .first()
    )

    models_factory.GroupFactory.create(is_active=False, with_item=True)

    results = item_sql_query_manager.query_manager.raw(
        """
        SELECT i.id, i.name, g.name as group_name
        FROM item i
        JOIN "group" g ON i.group_id = g.id
        WHERE g.is_active = :is_active
        """,
        is_active=True,
    )

    assert len(results) == 1
    assert results[0]["id"] == active_item.id
    assert results[0]["group_name"] == "active_group"


def test_raw__aggregation_with_group_by_having__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    group_a = models_factory.GroupFactory.create(name="group_a")
    group_b = models_factory.GroupFactory.create(name="group_b")

    # group_a gets 3 items — should appear
    models_factory.ItemFactory.create(number=10, group=group_a)
    models_factory.ItemFactory.create(number=20, group=group_a)
    models_factory.ItemFactory.create(number=30, group=group_a)

    # group_b gets 1 item — should be filtered by HAVING
    models_factory.ItemFactory.create(number=5, group=group_b)

    results = item_sql_query_manager.query_manager.raw(
        """
        SELECT group_id, COUNT(*) as cnt, SUM(number) as total
        FROM item
        GROUP BY group_id
        HAVING COUNT(*) > :min_count
        """,
        min_count=2,
    )

    assert len(results) == 1
    assert results[0]["group_id"] == group_a.id
    assert results[0]["cnt"] == 3
    assert results[0]["total"] == 60


def test_raw__cte__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    group = models_factory.GroupFactory.create()
    first_item = models_factory.ItemFactory.create(number=100, group=group)
    models_factory.ItemFactory.create(number=50, group=group)
    models_factory.ItemFactory.create(number=75, group=group)

    results = item_sql_query_manager.query_manager.raw(
        """
        WITH ranked AS (
            SELECT id, number,
                   ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY number DESC) as rn
            FROM item
            WHERE group_id = :group_id
        )
        SELECT id, number FROM ranked WHERE rn = 1
        """,
        group_id=group.id,
    )

    assert len(results) == 1
    assert results[0]["id"] == first_item.id
    assert results[0]["number"] == 100


def test_raw__empty_result__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    results = item_sql_query_manager.query_manager.raw(
        "SELECT * FROM item WHERE id = :id",
        id=999999,
    )

    assert results == []


@pytest.mark.asyncio
async def test_async_raw__simple_select_with_param__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    item = models_factory.ItemFactory.create(name="target")
    models_factory.ItemFactory.create_batch(size=3)

    results = await async_item_sql_query_manager.query_manager.raw(
        "SELECT * FROM item WHERE id = :id",
        id=item.id,
    )

    assert len(results) == 1
    assert results[0]["id"] == item.id
    assert results[0]["name"] == "target"


@pytest.mark.asyncio
async def test_async_raw__join__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    active_group = models_factory.GroupFactory.create(
        name="active_group", is_active=True, with_item=True
    )
    active_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == active_group.id)
        .first()
    )

    models_factory.GroupFactory.create(is_active=False, with_item=True)

    results = await async_item_sql_query_manager.query_manager.raw(
        """
        SELECT i.id, i.name, g.name as group_name
        FROM item i
        JOIN "group" g ON i.group_id = g.id
        WHERE g.is_active = :is_active
        """,
        is_active=True,
    )

    assert len(results) == 1
    assert results[0]["id"] == active_item.id
    assert results[0]["group_name"] == "active_group"


@pytest.mark.asyncio
async def test_async_raw__aggregation_with_group_by_having__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    group_a = models_factory.GroupFactory.create(name="group_a")
    group_b = models_factory.GroupFactory.create(name="group_b")

    models_factory.ItemFactory.create(number=10, group=group_a)
    models_factory.ItemFactory.create(number=20, group=group_a)
    models_factory.ItemFactory.create(number=30, group=group_a)

    models_factory.ItemFactory.create(number=5, group=group_b)

    results = await async_item_sql_query_manager.query_manager.raw(
        """
        SELECT group_id, COUNT(*) as cnt, SUM(number) as total
        FROM item
        GROUP BY group_id
        HAVING COUNT(*) > :min_count
        """,
        min_count=2,
    )

    assert len(results) == 1
    assert results[0]["group_id"] == group_a.id
    assert results[0]["cnt"] == 3
    assert results[0]["total"] == 60


@pytest.mark.asyncio
async def test_async_raw__empty_result__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    results = await async_item_sql_query_manager.query_manager.raw(
        "SELECT * FROM item WHERE id = :id",
        id=999999,
    )

    assert results == []
