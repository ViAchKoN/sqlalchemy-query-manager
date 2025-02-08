import pytest

from tests import models_factory


def test_where_object_last__ok(
    db_session,
    item_sql_query_manager,
):
    models_factory.ItemFactory.create()
    last_item = models_factory.ItemFactory.create()

    returned_obj_record = item_sql_query_manager.query_manager.last()

    assert returned_obj_record.as_dict() == last_item.as_dict()


@pytest.mark.asyncio
async def test_async_where_object_last__ok(
    db_session,
    async_item_sql_query_manager,
):
    models_factory.ItemFactory.create()
    last_item = models_factory.ItemFactory.create()

    returned_obj_record = await async_item_sql_query_manager.query_manager.last()

    assert returned_obj_record.as_dict() == last_item.as_dict()
