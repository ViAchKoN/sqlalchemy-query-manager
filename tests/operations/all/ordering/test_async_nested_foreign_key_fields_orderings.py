import pytest

from tests import models_factory


@pytest.mark.asyncio
async def test_all__order_by__id__ok(db_session, async_item_sql_query_manager):
    items = []
    for i in range(5):
        items.append(
            models_factory.ItemFactory.create(
                group=models_factory.GroupFactory.create(
                    owner=models_factory.OwnerFactory.create()
                )
            )
        )

    for order_by in [
        "group__owner__id",
        "-group__owner__id",
    ]:
        expected_items = items
        if order_by in [
            "-group__owner__id",
        ]:
            expected_items = list(reversed(items))

        results = await async_item_sql_query_manager.query_manager.order_by(
            order_by
        ).all()

        for expected_item, result in zip(expected_items, results):
            assert result.as_dict() == expected_item.as_dict()


@pytest.mark.asyncio
async def test_all__order_by__dates__ok(db_session, async_item_sql_query_manager):
    items = []
    for i in range(5):
        owner = models_factory.OwnerFactory.create()
        items.append(
            models_factory.ItemFactory.create(
                group=models_factory.GroupFactory.create(owner=owner)
            )
        )

    for order_by in [
        "group__owner__created_at",
        "-group__owner__created_at",
    ]:
        expected_items = items
        if order_by in [
            "-group__owner__created_at",
            [
                "-group__owner__created_at",
            ],
        ]:
            expected_items = list(reversed(items))

        results = await async_item_sql_query_manager.query_manager.order_by(
            order_by
        ).all()

        for expected_item, result in zip(expected_items, results):
            assert result.as_dict() == expected_item.as_dict()


@pytest.mark.asyncio
async def test_all__order_by__name__ok(db_session, async_item_sql_query_manager):
    items = []
    for name in [
        "aar",
        "abc",
        "cat",
        "wow",
    ]:
        items.append(
            models_factory.ItemFactory.create(
                group=models_factory.GroupFactory.create(
                    owner=models_factory.OwnerFactory.create(first_name=name)
                )
            )
        )

    for order_by in [
        "group__owner__first_name",
        "-group__owner__first_name",
    ]:
        expected_items = items
        if order_by in [
            "-group__owner__first_name",
            [
                "-group__owner__first_name",
            ],
        ]:
            expected_items = list(reversed(items))

        results = await async_item_sql_query_manager.query_manager.order_by(
            order_by
        ).all()

        for expected_item, result in zip(expected_items, results):
            assert result.as_dict() == expected_item.as_dict()
