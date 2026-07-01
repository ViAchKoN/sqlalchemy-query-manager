import re

import pytest
from sqlalchemy import select
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import DBAPIError

from tests import models_factory
from tests.models import Item


def normalize(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip()


def _build_select_item() -> str:
    sql = str(select(Item).compile(dialect=postgresql.dialect()))
    return normalize(sql)


SELECT_ITEM = _build_select_item()


def test_select_for_update__get_sql_query__ok(item_sql_query_manager):
    sql = (
        item_sql_query_manager.query_manager.where(id=1)
        .select_for_update()
        .get_sql_query(dialect=postgresql.dialect())
    )

    assert normalize(sql) == normalize(f"{SELECT_ITEM} WHERE item.id = 1 FOR UPDATE")


def test_select_for_update__nowait__get_sql_query__ok(item_sql_query_manager):
    sql = (
        item_sql_query_manager.query_manager.where(id=1)
        .select_for_update(nowait=True)
        .get_sql_query(dialect=postgresql.dialect())
    )

    assert normalize(sql) == normalize(
        f"{SELECT_ITEM} WHERE item.id = 1 FOR UPDATE NOWAIT"
    )


def test_select_for_update__skip_locked__get_sql_query__ok(item_sql_query_manager):
    sql = (
        item_sql_query_manager.query_manager.where(id=1)
        .select_for_update(skip_locked=True)
        .get_sql_query(dialect=postgresql.dialect())
    )

    assert normalize(sql) == normalize(
        f"{SELECT_ITEM} WHERE item.id = 1 FOR UPDATE SKIP LOCKED"
    )


def test_select_for_update__before_where__get_sql_query__ok(item_sql_query_manager):
    sql = (
        item_sql_query_manager.query_manager.select_for_update(skip_locked=True)
        .where(id=1)
        .get_sql_query(dialect=postgresql.dialect())
    )

    assert normalize(sql) == normalize(
        f"{SELECT_ITEM} WHERE item.id = 1 FOR UPDATE SKIP LOCKED"
    )


def test_select_for_update__no_key__get_sql_query__ok(item_sql_query_manager):
    sql = (
        item_sql_query_manager.query_manager.where(id=1)
        .select_for_update(no_key=True)
        .get_sql_query(dialect=postgresql.dialect())
    )

    assert normalize(sql) == normalize(
        f"{SELECT_ITEM} WHERE item.id = 1 FOR NO KEY UPDATE"
    )


def test_select_for_update__nowait_and_skip_locked__raises(item_sql_query_manager):
    with pytest.raises(ValueError, match="nowait and skip_locked"):
        item_sql_query_manager.query_manager.select_for_update(
            nowait=True,
            skip_locked=True,
        )


def test_select_for_update__without_explicit_session__raises(item_sql_query_manager):
    with pytest.raises(ValueError, match="requires an explicit SQLAlchemy Session"):
        item_sql_query_manager.query_manager.select_for_update().first()


def test_select_for_update__with_method_session__ok(
    create_tables,
    sync_db_sessionmaker,
    item_sql_query_manager,
):
    item = models_factory.ItemFactory.create()

    with sync_db_sessionmaker() as session:
        returned_obj = (
            item_sql_query_manager.query_manager.where(id=item.id)
            .select_for_update()
            .get(session=session)
        )

        assert returned_obj.id == item.id


def test_select_for_update__with_session__ok(
    create_tables,
    sync_db_sessionmaker,
    item_sql_query_manager,
):
    item = models_factory.ItemFactory.create()

    with sync_db_sessionmaker() as session:
        returned_obj = (
            item_sql_query_manager.query_manager.with_session(session)
            .where(id=item.id)
            .select_for_update()
            .get()
        )

        assert returned_obj.id == item.id


def test_select_for_update__nowait__locked_row_raises(
    create_tables,
    sync_db_sessionmaker,
    item_sql_query_manager,
):
    item = models_factory.ItemFactory.create()

    with sync_db_sessionmaker() as locking_session:
        locked_obj = (
            item_sql_query_manager.query_manager.where(id=item.id)
            .select_for_update()
            .get(session=locking_session)
        )

        assert locked_obj.id == item.id

        with sync_db_sessionmaker() as competing_session:
            with pytest.raises(DBAPIError):
                (
                    item_sql_query_manager.query_manager.where(id=item.id)
                    .select_for_update(nowait=True)
                    .get(session=competing_session)
                )


def test_select_for_update__skip_locked__locked_row_is_skipped(
    create_tables,
    sync_db_sessionmaker,
    item_sql_query_manager,
):
    item = models_factory.ItemFactory.create()

    with sync_db_sessionmaker() as locking_session:
        locked_obj = (
            item_sql_query_manager.query_manager.where(id=item.id)
            .select_for_update()
            .get(session=locking_session)
        )

        assert locked_obj.id == item.id

        with sync_db_sessionmaker() as competing_session:
            skipped_obj = (
                item_sql_query_manager.query_manager.where(id=item.id)
                .select_for_update(skip_locked=True)
                .first(session=competing_session)
            )

        assert skipped_obj is None


def test_async_select_for_update__get_sql_query__ok(async_item_sql_query_manager):
    sql = (
        async_item_sql_query_manager.query_manager.where(id=1)
        .select_for_update(skip_locked=True)
        .get_sql_query(dialect=postgresql.dialect())
    )

    assert normalize(sql) == normalize(
        f"{SELECT_ITEM} WHERE item.id = 1 FOR UPDATE SKIP LOCKED"
    )


@pytest.mark.asyncio
async def test_async_select_for_update__without_explicit_session__raises(
    async_item_sql_query_manager,
):
    with pytest.raises(
        ValueError, match="requires an explicit SQLAlchemy AsyncSession"
    ):
        await async_item_sql_query_manager.query_manager.select_for_update().first()


@pytest.mark.asyncio
async def test_async_select_for_update__with_method_session__ok(
    create_tables,
    async_db_sessionmaker,
    async_item_sql_query_manager,
):
    item = models_factory.ItemFactory.create()

    async with async_db_sessionmaker() as session:
        returned_obj = await (
            async_item_sql_query_manager.query_manager.where(id=item.id)
            .select_for_update()
            .get(session=session)
        )

        assert returned_obj.id == item.id


@pytest.mark.asyncio
async def test_async_select_for_update__nowait__locked_row_raises(
    create_tables,
    async_db_sessionmaker,
    async_item_sql_query_manager,
):
    item = models_factory.ItemFactory.create()

    async with async_db_sessionmaker() as locking_session:
        locked_obj = await (
            async_item_sql_query_manager.query_manager.where(id=item.id)
            .select_for_update()
            .get(session=locking_session)
        )

        assert locked_obj.id == item.id

        async with async_db_sessionmaker() as competing_session:
            with pytest.raises(DBAPIError):
                await (
                    async_item_sql_query_manager.query_manager.where(id=item.id)
                    .select_for_update(nowait=True)
                    .get(session=competing_session)
                )


@pytest.mark.asyncio
async def test_async_select_for_update__skip_locked__locked_row_is_skipped(
    create_tables,
    async_db_sessionmaker,
    async_item_sql_query_manager,
):
    item = models_factory.ItemFactory.create()

    async with async_db_sessionmaker() as locking_session:
        locked_obj = await (
            async_item_sql_query_manager.query_manager.where(id=item.id)
            .select_for_update()
            .get(session=locking_session)
        )

        assert locked_obj.id == item.id

        async with async_db_sessionmaker() as competing_session:
            skipped_obj = await (
                async_item_sql_query_manager.query_manager.where(id=item.id)
                .select_for_update(skip_locked=True)
                .first(session=competing_session)
            )

        assert skipped_obj is None
