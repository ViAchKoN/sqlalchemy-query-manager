import pytest

from tests import models_factory
from tests.models import Item


def test_all__ok(
    db_session,
    item_sql_query_manager,
):
    items = models_factory.ItemFactory.create_batch(size=2)

    returned_objs = item_sql_query_manager.query_manager.all()

    assert [obj.id for obj in returned_objs] == [item.id for item in items]


@pytest.mark.parametrize(
    'fields', (
        Item.id, 'id',
    )
)
def test_all__only__ok(
    db_session,
    item_sql_query_manager,
    fields,
):
    items = models_factory.ItemFactory.create_batch(size=2)

    returned_objs = item_sql_query_manager.query_manager.only(fields).all()

    assert [obj.id for obj in returned_objs] == [item.id for item in items]



@pytest.mark.parametrize(
    'fields', (
        (Item.name, Item.number),
        ('name', 'number'),
    )
)
def test_all__only__several_fields__ok(
    db_session,
    item_sql_query_manager,
    fields,
):
    items = models_factory.ItemFactory.create_batch(size=2)

    returned_objs = item_sql_query_manager.query_manager.only(*fields).all()

    assert [(obj.name, obj.number) for obj in returned_objs] == [(item.name, item.number) for item in items]


@pytest.mark.parametrize(
    'fields', (
        (Item.name, Item.number),
        ('name', 'number'),
    )
)
def test_all__only_several_fields__ok(
    db_session,
    item_sql_query_manager,
    fields,
):
    items = models_factory.ItemFactory.create_batch(size=2)

    returned_objs = item_sql_query_manager.query_manager.only(*fields).all()

    assert [(obj.name, obj.number) for obj in returned_objs] == [(item.name, item.number) for item in items]


def test_all__limit__ok(
    db_session,
    item_sql_query_manager,
):
    models_factory.ItemFactory.create_batch(size=10)

    returned_objs = item_sql_query_manager.query_manager.limit(5).all()

    assert len(returned_objs) == 5


def test_all__offset__ok(
    db_session,
    item_sql_query_manager,
):
    items = models_factory.ItemFactory.create_batch(size=10)

    returned_objs = item_sql_query_manager.query_manager.order_by('-id').offset(5).all()

    assert [obj.id for obj in returned_objs] == [item.id for item in items][:5][::-1]


@pytest.mark.asyncio
async def test_async_all__ok(
    db_session,
    async_item_sql_query_manager,
):
    items = models_factory.ItemFactory.create_batch(size=2)

    returned_objs = await async_item_sql_query_manager.query_manager.all()

    assert [obj.id for obj in returned_objs] == [item.id for item in items]


@pytest.mark.parametrize(
    'fields', (
        Item.id, 'id',
    )
)
@pytest.mark.asyncio
async def test_async_all__only__ok(
    db_session,
    async_item_sql_query_manager,
    fields,
):
    items = models_factory.ItemFactory.create_batch(size=2)

    returned_objs = await async_item_sql_query_manager.query_manager.only(fields).all()

    assert [obj.id for obj in returned_objs] == [item.id for item in items]


@pytest.mark.parametrize(
    'fields', (
        (Item.name, Item.number),
        ('name', 'number'),
    )
)
@pytest.mark.asyncio
async def test_async_all__only_several_fields__ok(
    db_session,
    async_item_sql_query_manager,
    fields,
):
    items = models_factory.ItemFactory.create_batch(size=2)

    returned_objs = await async_item_sql_query_manager.query_manager.only(*fields).all()

    assert [(obj.name, obj.number) for obj in returned_objs] == [(item.name, item.number) for item in items]


@pytest.mark.asyncio
async def test_async_all__limit__ok(
    db_session,
    async_item_sql_query_manager,
):
    models_factory.ItemFactory.create_batch(size=10)

    returned_objs = await async_item_sql_query_manager.query_manager.limit(5).all()

    assert len(returned_objs) == 5


@pytest.mark.asyncio
async def test_async_all__offset__ok(
    db_session,
    async_item_sql_query_manager,
):
    items = models_factory.ItemFactory.create_batch(size=10)

    returned_objs = await async_item_sql_query_manager.query_manager.order_by('-id').offset(5).all()

    assert [obj.id for obj in returned_objs] == [item.id for item in items][:5][::-1]
