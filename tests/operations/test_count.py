import pytest

from tests import models_factory


def test_count__ok(
    db_session,
    item_sql_query_manager,
):
    expected_count = 5

    models_factory.ItemFactory.create_batch(size=expected_count)

    count = item_sql_query_manager.query_manager.count()

    assert count == expected_count


@pytest.mark.asyncio
async def test_async_count__ok(
    db_session,
    async_item_sql_query_manager,
):
    expected_count = 5

    models_factory.ItemFactory.create_batch(size=expected_count)

    count = await async_item_sql_query_manager.query_manager.count()

    assert count == expected_count
