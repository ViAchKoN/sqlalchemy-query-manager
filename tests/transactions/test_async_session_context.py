from contextlib import asynccontextmanager

import pytest
from sqlalchemy import event, select
from sqlalchemy.ext.asyncio import AsyncSession

from sqlalchemy_query_manager import AsyncQueryManager, session_context, transaction
from tests.models import Group, Item, Owner


@pytest.mark.asyncio
async def test_async_session_context__supports_manual_commit_and_rollback(
    create_tables,
    async_db_sessionmaker,
):
    owner_manager = AsyncQueryManager(Owner)
    item_manager = AsyncQueryManager(Item)

    async with session_context(async_db_sessionmaker) as session_ctx:
        await owner_manager.create(first_name="John", last_name="Doe")
        await session_ctx.flush()
        await session_ctx.commit()

        await item_manager.create(name="rolled back")
        await session_ctx.rollback()

    async with async_db_sessionmaker() as session:
        assert len((await session.execute(select(Owner))).scalars().all()) == 1
        assert len((await session.execute(select(Item))).scalars().all()) == 0


@pytest.mark.asyncio
async def test_async_session_context__does_not_commit_pending_work_on_exit(
    create_tables,
    async_db_sessionmaker,
):
    async with session_context(async_db_sessionmaker):
        await AsyncQueryManager(Item).create(name="not committed")

    async with async_db_sessionmaker() as session:
        assert len((await session.execute(select(Item))).scalars().all()) == 0


@pytest.mark.asyncio
async def test_async_session_context__leaves_borrowed_session_lifecycle_to_caller(
    create_tables,
    async_db_sessionmaker,
):
    async with async_db_sessionmaker() as session:
        async with session_context(session):
            await AsyncQueryManager(Item).create(name="pending")

        assert session.in_transaction()
        result = await session.execute(select(Item))
        assert result.scalars().one().name == "pending"

        await session.rollback()
        result = await session.execute(select(Item))
        assert result.scalars().all() == []


@pytest.mark.asyncio
async def test_async_session_context__supports_context_manager_provider(
    create_tables,
    async_db_sessionmaker,
):
    @asynccontextmanager
    async def session_provider():
        async with async_db_sessionmaker() as session:
            yield session

    async with session_context(session_provider) as session_ctx:
        await AsyncQueryManager(Item).create(name="committed")
        await session_ctx.commit()

    async with async_db_sessionmaker() as session:
        result = await session.execute(select(Item))
        assert result.scalars().one().name == "committed"


@pytest.mark.asyncio
async def test_async_session_context__nested_context_reuses_session(
    create_tables,
    async_db_sessionmaker,
):
    async with session_context(async_db_sessionmaker) as outer:
        owner = await AsyncQueryManager(Owner).create(
            first_name="John",
            last_name="Doe",
        )

        async with session_context() as inner:
            await AsyncQueryManager(Group).create(name="team", owner_id=owner.id)
            await inner.flush()

        await outer.commit()

    async with async_db_sessionmaker() as session:
        owners = (await session.execute(select(Owner))).scalars().all()
        groups = (await session.execute(select(Group))).scalars().all()
        assert len(owners) == 1
        assert len(groups) == 1


@pytest.mark.asyncio
async def test_async_session_context__transaction_does_not_commit_outer_transaction(
    create_tables,
    async_db_sessionmaker,
):
    async with session_context(async_db_sessionmaker) as session_ctx:
        await AsyncQueryManager(Owner).create(
            first_name="John",
            last_name="Doe",
        )
        async with transaction():
            await AsyncQueryManager(Group).create(name="savepoint")
        await session_ctx.rollback()

    async with async_db_sessionmaker() as session:
        owners = (await session.execute(select(Owner))).scalars().all()
        groups = (await session.execute(select(Group))).scalars().all()
        assert owners == []
        assert groups == []


@pytest.mark.asyncio
async def test_async_session_context__nested_context_rejects_different_source(
    create_tables,
    async_db_sessionmaker,
):
    def different_source():
        return async_db_sessionmaker()

    async with session_context(async_db_sessionmaker):
        with pytest.raises(ValueError, match="must use the ambient session"):
            async with session_context(different_source):
                pass


@pytest.mark.asyncio
async def test_async_session_context__handle_is_inactive_after_exit(
    create_tables,
    async_db_sessionmaker,
):
    async with session_context(async_db_sessionmaker) as session_ctx:
        pass

    for method_name in ("flush", "commit", "rollback"):
        with pytest.raises(RuntimeError, match="no longer active"):
            await getattr(session_ctx, method_name)()


@pytest.mark.asyncio
async def test_async_session_context__outer_context_requires_source():
    with pytest.raises(ValueError, match="requires a session source"):
        async with session_context():
            pass


@pytest.mark.asyncio
async def test_async_session_context__rejects_sync_usage_inside_async_context(
    create_tables,
    async_db_sessionmaker,
):
    async with session_context(async_db_sessionmaker):
        with pytest.raises(TypeError, match="async session context"):
            with session_context():
                pass


@pytest.mark.asyncio
async def test_async_read_with_sessionmaker__does_not_commit(
    create_tables,
    async_db_sessionmaker,
):
    commits = []

    def record_commit(session):
        commits.append(session)

    event.listen(AsyncSession.sync_session_class, "after_commit", record_commit)
    try:
        assert await AsyncQueryManager(Item, async_db_sessionmaker).count() == 0
    finally:
        event.remove(AsyncSession.sync_session_class, "after_commit", record_commit)

    assert commits == []


@pytest.mark.asyncio
async def test_async_explicit_session__preserves_configured_sessionmaker_auto_commit(
    create_tables,
    async_db_sessionmaker,
    async_item_sql_query_manager,
):
    async with async_db_sessionmaker() as session:
        await async_item_sql_query_manager.query_manager.create(
            session=session,
            name="committed",
        )

        async with async_db_sessionmaker() as other_session:
            result = await other_session.execute(select(Item))
            assert len(result.scalars().all()) == 1

    async with async_db_sessionmaker() as verification_session:
        result = await verification_session.execute(select(Item))
        assert len(result.scalars().all()) == 1
