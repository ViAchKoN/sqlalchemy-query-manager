import datetime as dt

import pytest
from sqlalchemy import nulls_first, nulls_last

from sqlalchemy_query_manager.core.helpers import E
from tests import models_factory


@pytest.mark.asyncio
async def test_async_all__order_by__id__ok(db_session, async_item_sql_query_manager):
    items = models_factory.ItemFactory.create_batch(size=5)

    for order_by in [
        "id",
        "-id",
    ]:
        expected_items = items
        if order_by in ["-id"]:
            expected_items = list(reversed(items))

        results = await async_item_sql_query_manager.query_manager.order_by(
            order_by
        ).all()

        for expected_item, result in zip(expected_items, results):
            assert result.as_dict() == expected_item.as_dict()


@pytest.mark.asyncio
async def test_async_all__order_by__dates__ok(
    db_session,
    async_item_sql_query_manager,
):
    now = dt.datetime.now()

    first_date = now + dt.timedelta(days=1)
    first_item = models_factory.ItemFactory.create(
        created_at=first_date,
    )

    second_date = now + dt.timedelta(days=2)
    second_item = models_factory.ItemFactory.create(
        created_at=second_date,
    )

    third_date = now + dt.timedelta(days=3)
    third_item = models_factory.ItemFactory.create(
        created_at=third_date,
    )

    fourth_date = now + dt.timedelta(days=4)
    fourth_item = models_factory.ItemFactory.create(
        created_at=fourth_date,
    )

    fifth_date = now + dt.timedelta(days=5)
    fifth_item = models_factory.ItemFactory.create(
        created_at=fifth_date,
    )

    items = [first_item, second_item, third_item, fourth_item, fifth_item]

    for order_by in [
        "created_at",
        "-created_at",
    ]:
        expected_items = items
        if order_by in [
            "-created_at",
        ]:
            expected_items = list(reversed(items))

        results = await async_item_sql_query_manager.query_manager.order_by(
            order_by
        ).all()

        for expected_item, result in zip(expected_items, results):
            assert result.as_dict() == expected_item.as_dict()


@pytest.mark.asyncio
async def test_async_all__order_by__name__ok(
    db_session,
    async_item_sql_query_manager,
):
    items = []

    for name in [
        "aar",
        "abc",
        "cat",
        "wow",
    ]:
        items.append(models_factory.ItemFactory.create(name=name))

    for order_by in [
        "name",
        "-name",
    ]:
        expected_items = items
        if order_by in [
            "-name",
        ]:
            expected_items = list(reversed(items))

        results = await async_item_sql_query_manager.query_manager.order_by(
            order_by
        ).all()

        for expected_item, result in zip(expected_items, results):
            assert result.as_dict() == expected_item.as_dict()


@pytest.mark.asyncio
async def test_async_all__order_by__name__nulls_last__ok(
    db_session,
    async_item_sql_query_manager,
):
    items = []

    for name in [
        "aar",
        "abc",
        "cat",
        "wow",
    ]:
        items.append(models_factory.ItemFactory.create(name=name))

    null_name_item = models_factory.ItemFactory.create(name=None)

    for order_by in [
        "name",
        "-name",
    ]:
        expected_items = items.copy()
        if order_by in [
            "-name",
        ]:
            expected_items = list(reversed(items))

        expected_items.append(null_name_item)

        results = await async_item_sql_query_manager.query_manager.order_by(
            E(order_by, nulls_last)
        ).all()

        for expected_item, result in zip(expected_items, results):
            assert result.as_dict() == expected_item.as_dict()


@pytest.mark.asyncio
async def test_async_all__order_by__name__nulls_first__ok(
    db_session,
    async_item_sql_query_manager,
):
    items = []

    for name in [
        "aar",
        "abc",
        "cat",
        "wow",
    ]:
        items.append(models_factory.ItemFactory.create(name=name))

    null_name_item = models_factory.ItemFactory.create(name=None)

    for order_by in [
        "name",
        "-name",
    ]:
        expected_items = items.copy()
        if order_by in [
            "-name",
        ]:
            expected_items = list(reversed(items))

        expected_items.insert(0, null_name_item)

        results = await async_item_sql_query_manager.query_manager.order_by(
            E(order_by, nulls_first)
        ).all()

        for expected_item, result in zip(expected_items, results):
            assert result.as_dict() == expected_item.as_dict()
