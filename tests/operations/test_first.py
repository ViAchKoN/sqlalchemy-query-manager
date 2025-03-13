import pytest
from sqlalchemy.engine import Row

from tests import models_factory
from tests.models import Item


def test_where_object_first__ok(
    db_session,
    item_sql_query_manager,
):
    item = models_factory.ItemFactory.create()

    models_factory.ItemFactory.create()

    returned_obj = item_sql_query_manager.query_manager.where(id=item.id).first()

    assert returned_obj.as_dict() == item.as_dict()


def test_where_object_only__ok(
    db_session,
    item_sql_query_manager,
):
    item = models_factory.ItemFactory.create()

    models_factory.ItemFactory.create()

    returned_obj_record = (
        item_sql_query_manager.query_manager.where(id=item.id).only(Item.id).first()
    )

    assert isinstance(returned_obj_record, Row)

    assert returned_obj_record.id == 1


def test_where_object_only__as_str__ok(
    db_session,
    item_sql_query_manager,
):
    item = models_factory.ItemFactory.create()

    models_factory.ItemFactory.create()

    returned_obj_record = (
        item_sql_query_manager.query_manager.where(id=item.id).only("id").first()
    )

    assert isinstance(returned_obj_record, Row)

    assert returned_obj_record.id == 1


def test_where_object_only__star__ok(
    db_session,
    item_sql_query_manager,
):
    item = models_factory.ItemFactory.create(name=None)

    models_factory.ItemFactory.create()

    returned_obj_record = (
        item_sql_query_manager.query_manager.where(id=item.id).only("*").first()
    )

    assert isinstance(returned_obj_record, Row)

    assert returned_obj_record.id == item.id
    assert returned_obj_record.created_at == item.created_at
    assert returned_obj_record.number == item.number
    assert returned_obj_record.is_valid == item.is_valid
    assert returned_obj_record.has_name is False


@pytest.mark.asyncio
async def test_async_where_object_first__ok(
    db_session,
    async_item_sql_query_manager,
):
    item = models_factory.ItemFactory.create()

    models_factory.ItemFactory.create()

    returned_obj = await async_item_sql_query_manager.query_manager.where(
        id=item.id
    ).first()

    assert returned_obj.as_dict() == item.as_dict()


@pytest.mark.asyncio
async def test_async_where_object_only__ok(
    db_session,
    async_item_sql_query_manager,
):
    item = models_factory.ItemFactory.create()

    models_factory.ItemFactory.create()

    returned_obj_record = (
        await async_item_sql_query_manager.query_manager.where(id=item.id)
        .only(Item.id)
        .first()
    )

    assert isinstance(returned_obj_record, Row)

    assert returned_obj_record.id == 1


@pytest.mark.asyncio
async def test_async_where_object_only__as_str__ok(
    db_session,
    async_item_sql_query_manager,
):
    item = models_factory.ItemFactory.create()

    models_factory.ItemFactory.create()

    returned_obj_record = (
        await async_item_sql_query_manager.query_manager.where(id=item.id)
        .only("id")
        .first()
    )

    assert isinstance(returned_obj_record, Row)

    assert returned_obj_record.id == 1


@pytest.mark.asyncio
async def test_async_where_object_only__star__ok(
    db_session,
    async_item_sql_query_manager,
):
    item = models_factory.ItemFactory.create(name=None)

    models_factory.ItemFactory.create()

    returned_obj_record = (
        await async_item_sql_query_manager.query_manager.where(id=item.id)
        .only("*")
        .first()
    )

    assert isinstance(returned_obj_record, Row)

    assert returned_obj_record.id == item.id
    assert returned_obj_record.created_at == item.created_at
    assert returned_obj_record.number == item.number
    assert returned_obj_record.is_valid == item.is_valid
    assert returned_obj_record.has_name is False
