import datetime as dt
import typing as tp

import pytest
from sqlalchemy import select

from tests import models, models_factory


@pytest.mark.asyncio
async def test_async_all_filter__eq__ok(
    db_session,
    async_item_sql_query_manager,
):
    # Create expected group and item
    expected_owner_first_name = "expected_owner_first_name"

    expected_owner = models_factory.OwnerFactory.create(
        first_name=expected_owner_first_name,
    )
    expected_group = models_factory.GroupFactory.create(
        with_item=True,
        owner=expected_owner,
    )
    expected_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == expected_group.id)
        .first()
    )

    # Create unexpected groups and items
    models_factory.GroupFactory.create_batch(with_item=True, size=4)

    assert db_session.query(models.Item).count() == 5

    results = await async_item_sql_query_manager.query_manager.where(group__owner__first_name=expected_owner_first_name).all()

    assert len(results) == 1

    result = results[0]

    assert result.as_dict() == expected_item.as_dict()


@pytest.mark.asyncio
async def test_async_all_filter__in_not_in__ok(
    db_session,
    async_item_sql_query_manager,
):
    first_owner_last_name = "first_owner_last_name"

    first_owner = models_factory.OwnerFactory.create(
        last_name=first_owner_last_name,
    )
    first_group = models_factory.GroupFactory.create(
        with_item=True,
        owner=first_owner,
    )
    first_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == first_group.id)
        .first()
    )

    second_owner_last_name = "second_owner_last_name"

    second_owner = models_factory.OwnerFactory.create(
        last_name=second_owner_last_name,
    )
    second_group = models_factory.GroupFactory.create(
        with_item=True,
        owner=second_owner,
    )
    second_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == second_group.id)
        .first()
    )

    for field, filter_values, expected_items in (
        (
            "group__owner__last_name__in",
            [first_owner_last_name, second_owner_last_name],
            [first_item, second_item],
        ),
        (
            "group__owner__last_name__not_in",
            [first_owner_last_name, second_owner_last_name],
            [],
        ),
        (
            "group__owner__last_name__in",
            [
                first_owner_last_name,
            ],
            [
                first_item,
            ],
        ),
        (
            "group__owner__last_name__not_in",
            [
                first_owner_last_name,
            ],
            [
                second_item,
            ],
        ),
    ):
        results = await async_item_sql_query_manager.query_manager.where(**{field: filter_values}).all()

        assert len(results) == len(expected_items)

        for expected_item, result in zip(expected_items, results):
            assert result.as_dict() == expected_item.as_dict()


@pytest.mark.asyncio
async def test_async_all_filter__in_not_in__dates__ok(
    db_session,
    async_item_sql_query_manager,
):
    now = dt.datetime.now()

    first_date = now + dt.timedelta(days=1)

    first_owner = models_factory.OwnerFactory.create(
        created_at=first_date,
    )
    first_group = models_factory.GroupFactory.create(
        with_item=True,
        owner=first_owner,
    )
    first_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == first_group.id)
        .first()
    )

    second_date = now + dt.timedelta(days=2)

    second_owner = models_factory.OwnerFactory.create(
        created_at=second_date,
    )
    second_group = models_factory.GroupFactory.create(
        with_item=True,
        owner=second_owner,
    )
    second_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == second_group.id)
        .first()
    )

    for field, filter_values, expected_items in (
        (
            "group__owner__created_at__in",
            [first_date, second_date],
            [first_item, second_item],
        ),
        ("group__owner__created_at__not_in", [first_date, second_date], []),
        (
            "group__owner__created_at__in",
            [
                first_date,
            ],
            [
                first_item,
            ],
        ),
        (
            "group__owner__created_at__not_in",
            [
                first_date,
            ],
            [
                second_item,
            ],
        ),
    ):
        results = await async_item_sql_query_manager.query_manager.where(**{field: filter_values}).all()

        assert len(results) == len(expected_items)

        for expected_item, result in zip(expected_items, results):
            assert result.as_dict() == expected_item.as_dict()


@pytest.mark.asyncio
async def test_async_all_filter__gt_lt_gte_lte__date__ok(
    db_session,
    async_item_sql_query_manager,
):
    now = dt.datetime.now()

    first_date = now + dt.timedelta(days=1)

    first_owner = models_factory.OwnerFactory.create(
        created_at=first_date,
    )
    first_group = models_factory.GroupFactory.create(
        with_item=True,
        owner=first_owner,
    )
    first_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == first_group.id)
        .first()
    )

    second_date = now + dt.timedelta(days=2)

    second_owner = models_factory.OwnerFactory.create(
        created_at=second_date,
    )
    second_group = models_factory.GroupFactory.create(
        with_item=True,
        owner=second_owner,
    )
    second_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == second_group.id)
        .first()
    )

    third_date = now + dt.timedelta(days=3)

    for field, filter_values, expected_items in (
        ("group__owner__created_at__gt", now, [first_item, second_item]),
        ("group__owner__created_at__lt", third_date, [first_item, second_item]),
        (
            "group__owner__created_at__gte",
            second_date,
            [
                second_item,
            ],
        ),
        (
            "group__owner__created_at__lte",
            first_date,
            [
                first_item,
            ],
        ),
    ):
        results = await async_item_sql_query_manager.query_manager.where(**{field: filter_values}).all()

        assert len(results) == len(expected_items)

        for expected_item, result in zip(expected_items, results):
            assert result.as_dict() == expected_item.as_dict()


@pytest.mark.asyncio
async def test_async_all_filter__not__ok(
    db_session,
    async_item_sql_query_manager,
):
    first_owner_last_name = "first_owner_last_name"

    first_owner = models_factory.OwnerFactory.create(
        last_name=first_owner_last_name,
    )
    first_group = models_factory.GroupFactory.create(
        with_item=True,
        owner=first_owner,
    )
    first_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == first_group.id)
        .first()
    )

    second_owner_last_name = "second_owner_last_name"

    second_owner = models_factory.OwnerFactory.create(
        last_name=second_owner_last_name,
    )
    second_group = models_factory.GroupFactory.create(
        with_item=True,
        owner=second_owner,
    )
    second_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == second_group.id)
        .first()
    )

    fake_name = "fake_name"

    for field, filter_values, expected_items in (
        ("group__owner__last_name__not", fake_name, [first_item, second_item]),
        (
            "group__owner__last_name__not",
            second_owner_last_name,
            [
                first_item,
            ],
        ),
        (
            "group__owner__last_name__not",
            first_owner_last_name,
            [
                second_item,
            ],
        ),
    ):
        results = await async_item_sql_query_manager.query_manager.where(**{field: filter_values}).all()

        assert len(results) == len(expected_items)

        for expected_item, result in zip(expected_items, results):
            assert result.as_dict() == expected_item.as_dict()


@pytest.mark.asyncio
async def test_async_all_filter__is_null_is_not_null__ok(
    db_session,
    async_item_sql_query_manager,
):
    owner_without_email = models_factory.OwnerFactory.create(
        email=None,
    )
    first_group = models_factory.GroupFactory.create(
        with_item=True,
        owner=owner_without_email,
    )
    first_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == first_group.id)
        .first()
    )

    owner_with_email = models_factory.OwnerFactory.create()
    second_group = models_factory.GroupFactory.create(
        with_item=True,
        owner=owner_with_email,
    )
    second_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == second_group.id)
        .first()
    )

    for field, filter_values, expected_items in (
        ("group__owner__email__isnull", True, [first_item]),
        (
            "group__owner__email__isnull",
            False,
            [
                second_item,
            ],
        ),
    ):
        results = await async_item_sql_query_manager.query_manager.where(**{field: filter_values}).all()

        assert len(results) == len(expected_items)

        for expected_item, result in zip(expected_items, results):
            assert result.as_dict() == expected_item.as_dict()


@pytest.mark.asyncio
async def test_async_all_filter__like_ilike__ok(
    db_session,
    async_item_sql_query_manager,
):
    first_owner_last_name = "first name"

    first_owner = models_factory.OwnerFactory.create(
        last_name=first_owner_last_name,
    )
    first_group = models_factory.GroupFactory.create(
        with_item=True,
        owner=first_owner,
    )
    first_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == first_group.id)
        .first()
    )

    second_owner_last_name = "some oThEr NaMe"

    second_owner = models_factory.OwnerFactory.create(
        last_name=second_owner_last_name,
    )
    second_group = models_factory.GroupFactory.create(
        with_item=True,
        owner=second_owner,
    )
    second_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == second_group.id)
        .first()
    )

    for field, filter_value, expected_items in (
        (
            "group__owner__last_name__like",
            "%name%",
            [
                first_item,
            ],
        ),
        (
            "group__owner__last_name__ilike",
            "%other name%",
            [
                second_item,
            ],
        ),
    ):
        results = await async_item_sql_query_manager.query_manager.where(**{field: filter_value}).all()

        assert len(results) == len(expected_items)

        for expected_item, result in zip(expected_items, results):
            assert result.as_dict() == expected_item.as_dict()
