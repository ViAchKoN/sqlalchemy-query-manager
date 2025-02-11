import datetime as dt

import pytest

from tests import models, models_factory


@pytest.mark.asyncio
async def test_async_all__filter__eq__ok(
    db_session,
    async_item_sql_query_manager,
):
    # Create expected group and item
    expected_group_name = "expected_group_name"

    expected_group = models_factory.GroupFactory.create(
        name=expected_group_name, with_item=True
    )
    expected_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == expected_group.id)
        .first()
    )

    # Create unexpected groups and items
    models_factory.GroupFactory.create_batch(with_item=True, size=4)

    assert db_session.query(models.Item).count() == 5

    results = await async_item_sql_query_manager.query_manager.where(
        group__name=expected_group_name
    ).all()

    assert len(results) == 1

    result = results[0]

    assert result.as_dict() == expected_item.as_dict()


@pytest.mark.asyncio
async def test_async_all__filter__in_not_in__ok(
    db_session,
    async_item_sql_query_manager,
):
    first_group_name = "first_group_name"

    first_group = models_factory.GroupFactory.create(
        name=first_group_name, with_item=True
    )
    first_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == first_group.id)
        .first()
    )

    second_group_name = "second_group_name"

    second_group = models_factory.GroupFactory.create(
        name=second_group_name, with_item=True
    )
    second_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == second_group.id)
        .first()
    )

    for field, filter_values, expected_items in (
        (
            "group__name__in",
            [first_group_name, second_group_name],
            [first_item, second_item],
        ),
        ("group__name__not_in", [first_group_name, second_group_name], []),
        (
            "group__name__in",
            [
                first_group_name,
            ],
            [
                first_item,
            ],
        ),
        (
            "group__name__not_in",
            [
                first_group_name,
            ],
            [
                second_item,
            ],
        ),
    ):
        results = await async_item_sql_query_manager.query_manager.where(
            **{field: filter_values}
        ).all()

        assert len(results) == len(expected_items)

        for expected_item, result in zip(expected_items, results):
            assert result.as_dict() == expected_item.as_dict()


@pytest.mark.asyncio
async def test_async_all__filter__in_not_in__list_as_string__ok(
    db_session,
    async_item_sql_query_manager,
):
    first_group_name = "first_group_name"

    first_group = models_factory.GroupFactory.create(
        name=first_group_name, with_item=True
    )
    first_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == first_group.id)
        .first()
    )

    second_group_name = "second_group_name"

    second_group = models_factory.GroupFactory.create(
        name=second_group_name, with_item=True
    )
    second_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == second_group.id)
        .first()
    )

    for field, filter_values, expected_items in (
        (
            "group__name__in",
            [first_group_name, second_group_name],
            [first_item, second_item],
        ),
        ("group__name__not_in", [first_group_name, second_group_name], []),
        (
            "group__name__in",
            [
                first_group_name,
            ],
            [
                first_item,
            ],
        ),
        (
            "group__name__not_in",
            [
                first_group_name,
            ],
            [
                second_item,
            ],
        ),
    ):
        results = await async_item_sql_query_manager.query_manager.where(
            **{field: filter_values}
        ).all()

        assert len(results) == len(expected_items)

        for expected_item, result in zip(expected_items, results):
            assert result.as_dict() == expected_item.as_dict()


@pytest.mark.asyncio
async def test_async_all__filter__in_not_in__dates__ok(
    db_session,
    async_item_sql_query_manager,
):
    now = dt.datetime.now()

    first_date = now + dt.timedelta(days=1)

    first_group = models_factory.GroupFactory.create(
        created_at=first_date, with_item=True
    )
    first_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == first_group.id)
        .first()
    )

    second_date = now + dt.timedelta(days=2)

    second_group = models_factory.GroupFactory.create(
        created_at=second_date, with_item=True
    )
    second_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == second_group.id)
        .first()
    )

    for field, filter_values, expected_items in (
        ("group__created_at__in", [first_date, second_date], [first_item, second_item]),
        ("group__created_at__not_in", [first_date, second_date], []),
        (
            "group__created_at__in",
            [
                first_date,
            ],
            [
                first_item,
            ],
        ),
        (
            "group__created_at__not_in",
            [
                first_date,
            ],
            [
                second_item,
            ],
        ),
    ):
        results = await async_item_sql_query_manager.query_manager.where(
            **{field: filter_values}
        ).all()

        assert len(results) == len(expected_items)

        for expected_item, result in zip(expected_items, results):
            assert result.as_dict() == expected_item.as_dict()


@pytest.mark.asyncio
async def test_async_all__filter__gt_lt_gte_lte__date__ok(
    db_session, async_item_sql_query_manager
):
    now = dt.datetime.now()

    first_date = now + dt.timedelta(days=1)

    first_group = models_factory.GroupFactory.create(
        created_at=first_date, with_item=True
    )
    first_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == first_group.id)
        .first()
    )

    second_date = now + dt.timedelta(days=2)

    second_group = models_factory.GroupFactory.create(
        created_at=second_date, with_item=True
    )
    second_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == second_group.id)
        .first()
    )

    third_date = now + dt.timedelta(days=3)

    for field, filter_values, expected_items in (
        ("group__created_at__gt", now, [first_item, second_item]),
        ("group__created_at__lt", third_date, [first_item, second_item]),
        (
            "group__created_at__gte",
            second_date,
            [
                second_item,
            ],
        ),
        (
            "group__created_at__lte",
            first_date,
            [
                first_item,
            ],
        ),
    ):
        results = await async_item_sql_query_manager.query_manager.where(
            **{field: filter_values}
        ).all()

        assert len(results) == len(expected_items)

        for expected_item, result in zip(expected_items, results):
            assert result.as_dict() == expected_item.as_dict()


@pytest.mark.asyncio
async def test_async_all__filter__not__ok(db_session, async_item_sql_query_manager):
    first_group_name = "first_group_name"

    first_group = models_factory.GroupFactory.create(
        name=first_group_name, with_item=True
    )
    first_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == first_group.id)
        .first()
    )

    second_group_name = "second_group_name"

    second_group = models_factory.GroupFactory.create(
        name=second_group_name, with_item=True
    )
    second_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == second_group.id)
        .first()
    )

    fake_name = "fake_name"

    for field, filter_values, expected_items in (
        ("group__name__not", fake_name, [first_item, second_item]),
        (
            "group__name__not",
            second_group_name,
            [
                first_item,
            ],
        ),
        (
            "group__name__not",
            first_group_name,
            [
                second_item,
            ],
        ),
    ):
        results = await async_item_sql_query_manager.query_manager.where(
            **{field: filter_values}
        ).all()

        assert len(results) == len(expected_items)

        for expected_item, result in zip(expected_items, results):
            assert result.as_dict() == expected_item.as_dict()


@pytest.mark.asyncio
async def test_async_all__filter__is_is_not__ok(
    db_session, async_item_sql_query_manager
):
    active_group = models_factory.GroupFactory.create(is_active=True, with_item=True)
    first_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == active_group.id)
        .first()
    )

    inactive_group = models_factory.GroupFactory.create(is_active=False, with_item=True)
    second_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == inactive_group.id)
        .first()
    )

    for field, filter_values, expected_items in (
        ("group__is_active__is", True, [first_item]),
        (
            "group__is_active__is",
            False,
            [
                second_item,
            ],
        ),
    ):
        results = await async_item_sql_query_manager.query_manager.where(
            **{field: filter_values}
        ).all()

        assert len(results) == len(expected_items)

        for expected_item, result in zip(expected_items, results):
            assert result.as_dict() == expected_item.as_dict()


@pytest.mark.asyncio
async def test_async_all__filter__like_ilike__ok(
    db_session, async_item_sql_query_manager
):
    first_group = models_factory.GroupFactory.create(name="first name", with_item=True)
    first_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == first_group.id)
        .first()
    )

    second_group = models_factory.GroupFactory.create(
        name="some oThEr NaMe", with_item=True
    )
    second_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == second_group.id)
        .first()
    )

    assert db_session.query(models.Item).count() == 2

    for field, filter_value, expected_items in (
        (
            "group__name__like",
            "%name%",
            [
                first_item,
            ],
        ),
        (
            "group__name__ilike",
            "%other name%",
            [
                second_item,
            ],
        ),
    ):
        results = await async_item_sql_query_manager.query_manager.where(
            **{field: filter_value}
        ).all()

        assert len(results) == len(expected_items)

        for expected_item, result in zip(expected_items, results):
            assert result.as_dict() == expected_item.as_dict()
