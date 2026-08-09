import asyncio
import contextvars
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

import pytest
from sqlalchemy import select

from sqlalchemy_query_manager import (
    AsyncQueryManager,
    QueryManager,
    session_context,
    transaction,
)
from tests.models import Group, Item, Owner


WRITE_OPERATIONS = (
    "create",
    "bulk_create",
    "get_or_create",
    "update",
    "update_raw",
    "update_or_create",
    "bulk_update",
    "delete",
)
OPERATIONS_REQUIRING_EXISTING_ITEM = {
    "update",
    "update_raw",
    "update_or_create",
    "bulk_update",
    "delete",
}


async def _run_write_operation(operation, item_id=None):
    manager = AsyncQueryManager(Item)

    if operation == "create":
        await manager.create(name="created")
    elif operation == "bulk_create":
        await manager.bulk_create([{"name": "first"}, {"name": "second"}])
    elif operation == "get_or_create":
        await manager.get_or_create(name="created")
    elif operation == "update":
        await manager.where(id=item_id).update(name="updated")
    elif operation == "update_raw":
        await manager.where(id=item_id).update_raw(name="updated")
    elif operation == "update_or_create":
        await manager.update_or_create(id=item_id, defaults={"name": "updated"})
    elif operation == "bulk_update":
        await manager.bulk_update([{"id": item_id, "name": "updated"}])
    elif operation == "delete":
        await manager.where(id=item_id).delete()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", WRITE_OPERATIONS)
async def test_async_transaction__write_operation_is_rolled_back(
    operation,
    create_tables,
    async_db_sessionmaker,
):
    item_id = None
    if operation in OPERATIONS_REQUIRING_EXISTING_ITEM:
        async with async_db_sessionmaker() as session:
            item = Item(name="original")
            session.add(item)
            await session.commit()
            item_id = item.id

    with pytest.raises(RuntimeError, match="abort"):
        async with transaction(async_db_sessionmaker):
            await _run_write_operation(operation, item_id)
            raise RuntimeError("abort")

    async with async_db_sessionmaker() as session:
        items = (await session.execute(select(Item))).scalars().all()
        if operation in OPERATIONS_REQUIRING_EXISTING_ITEM:
            assert len(items) == 1
            assert items[0].name == "original"
        else:
            assert items == []


@pytest.mark.asyncio
async def test_async_transaction__uses_one_session_across_models_and_commits(
    create_tables,
    async_db_sessionmaker,
):
    owner_manager = AsyncQueryManager(Owner)
    group_manager = AsyncQueryManager(Group)
    item_manager = AsyncQueryManager(Item)

    async with transaction(async_db_sessionmaker) as tx:
        owner = await owner_manager.create(first_name="John", last_name="Doe")
        group = await group_manager.create(name="team", owner_id=owner.id)
        await item_manager.create(name="item", group_id=group.id)

        await tx.flush()

        assert await owner_manager.count() == 1
        assert await group_manager.count() == 1
        assert await item_manager.count() == 1

    async with async_db_sessionmaker() as session:
        assert len((await session.execute(select(Owner))).scalars().all()) == 1
        assert len((await session.execute(select(Group))).scalars().all()) == 1
        assert len((await session.execute(select(Item))).scalars().all()) == 1


@pytest.mark.asyncio
async def test_async_transaction__exception_rolls_back_all_models(
    create_tables,
    async_db_sessionmaker,
):
    with pytest.raises(RuntimeError, match="abort"):
        async with transaction(async_db_sessionmaker):
            await AsyncQueryManager(Owner).create(
                first_name="John",
                last_name="Doe",
            )
            await AsyncQueryManager(Group).create(name="team")
            raise RuntimeError("abort")

    async with async_db_sessionmaker() as session:
        assert len((await session.execute(select(Owner))).scalars().all()) == 0
        assert len((await session.execute(select(Group))).scalars().all()) == 0


@pytest.mark.asyncio
async def test_async_transaction__nested_exception_rolls_back_savepoint_only(
    create_tables,
    async_db_sessionmaker,
):
    async with transaction(async_db_sessionmaker):
        await AsyncQueryManager(Owner).create(
            first_name="John",
            last_name="Doe",
        )

        with pytest.raises(RuntimeError, match="nested abort"):
            async with transaction():
                await AsyncQueryManager(Group).create(name="discarded")
                raise RuntimeError("nested abort")

        await AsyncQueryManager(Item).create(name="survives")

    async with async_db_sessionmaker() as session:
        assert len((await session.execute(select(Owner))).scalars().all()) == 1
        assert len((await session.execute(select(Group))).scalars().all()) == 0
        assert len((await session.execute(select(Item))).scalars().all()) == 1


@pytest.mark.asyncio
async def test_async_transaction__outer_rollback_includes_released_savepoint(
    create_tables,
    async_db_sessionmaker,
):
    with pytest.raises(RuntimeError, match="outer abort"):
        async with transaction(async_db_sessionmaker):
            async with transaction():
                await AsyncQueryManager(Group).create(name="released savepoint")
            raise RuntimeError("outer abort")

    async with async_db_sessionmaker() as session:
        assert len((await session.execute(select(Group))).scalars().all()) == 0


@pytest.mark.asyncio
async def test_async_transaction__manual_commit_and_rollback_are_rejected(
    create_tables,
    async_db_sessionmaker,
):
    async with transaction(async_db_sessionmaker) as tx:
        with pytest.raises(RuntimeError, match="Manual commit"):
            await tx.commit()
        with pytest.raises(RuntimeError, match="Manual rollback"):
            await tx.rollback()


@pytest.mark.asyncio
async def test_async_transaction__handle_is_inactive_after_exit(
    create_tables,
    async_db_sessionmaker,
):
    async with transaction(async_db_sessionmaker) as tx:
        pass

    with pytest.raises(RuntimeError, match="no longer active"):
        await tx.flush()


@pytest.mark.asyncio
async def test_async_nested_session_context__cannot_commit_transaction(
    create_tables,
    async_db_sessionmaker,
):
    async with transaction(async_db_sessionmaker):
        async with session_context() as session_ctx:
            with pytest.raises(RuntimeError, match="Manual commit"):
                await session_ctx.commit()
            with pytest.raises(RuntimeError, match="Manual rollback"):
                await session_ctx.rollback()


@pytest.mark.asyncio
async def test_async_transaction__supports_context_manager_provider(
    create_tables,
    async_db_sessionmaker,
):
    @asynccontextmanager
    async def session_provider():
        async with async_db_sessionmaker() as session:
            yield session

    async with transaction(session_provider):
        await AsyncQueryManager(Owner).create(
            first_name="John",
            last_name="Doe",
        )

    async with async_db_sessionmaker() as session:
        assert len((await session.execute(select(Owner))).scalars().all()) == 1


@pytest.mark.asyncio
async def test_async_transaction__supports_borrowed_session_without_closing_it(
    create_tables,
    async_db_sessionmaker,
):
    async with async_db_sessionmaker() as session:
        async with transaction(session):
            await AsyncQueryManager(Item).create(name="created")

        result = await session.execute(select(Item))
        assert result.scalars().one().name == "created"


@pytest.mark.asyncio
async def test_async_transaction__uses_savepoint_for_active_borrowed_session(
    create_tables,
    async_db_sessionmaker,
):
    async with async_db_sessionmaker() as session:
        await session.begin()
        session.add(Owner(first_name="John", last_name="Doe"))

        async with transaction(session):
            await AsyncQueryManager(Group).create(name="nested")

        await session.rollback()

    async with async_db_sessionmaker() as verification_session:
        owners = (await verification_session.execute(select(Owner))).scalars().all()
        groups = (await verification_session.execute(select(Group))).scalars().all()
        assert owners == []
        assert groups == []


@pytest.mark.asyncio
async def test_async_transaction__rejects_explicit_session_override(
    create_tables,
    async_db_sessionmaker,
):
    async with async_db_sessionmaker() as other_session:
        async with transaction(async_db_sessionmaker):
            with pytest.raises(ValueError, match="cannot override"):
                await AsyncQueryManager(Item).create(
                    session=other_session,
                    name="wrong session",
                )


@pytest.mark.asyncio
async def test_async_transaction__select_for_update_uses_ambient_session(
    create_tables,
    async_db_sessionmaker,
):
    manager = AsyncQueryManager(Item, async_db_sessionmaker)
    item = await manager.create(name="lock me")

    async with transaction(async_db_sessionmaker):
        locked = await manager.where(id=item.id).select_for_update().get()
        assert locked.id == item.id


@pytest.mark.asyncio
async def test_async_transaction__context_is_reset_after_exit(
    create_tables,
    async_db_sessionmaker,
):
    async with transaction(async_db_sessionmaker):
        pass

    with pytest.raises(ValueError, match="requires a session source"):
        async with transaction():
            pass

    with pytest.raises(RuntimeError, match="abort"):
        async with transaction(async_db_sessionmaker):
            raise RuntimeError("abort")

    with pytest.raises(ValueError, match="requires a session source"):
        async with transaction():
            pass


@pytest.mark.asyncio
async def test_async_transaction__context_is_reset_when_commit_fails(
    create_tables,
    async_db_sessionmaker,
    monkeypatch,
):
    context = transaction(async_db_sessionmaker)

    async def fail_commit(transaction):
        raise RuntimeError("commit failed")

    with pytest.raises(RuntimeError, match="commit failed"):
        async with context:
            monkeypatch.setattr(type(context._transaction), "commit", fail_commit)

    with pytest.raises(ValueError, match="requires a session source"):
        async with transaction():
            pass


@pytest.mark.asyncio
async def test_async_transaction__rejects_sync_usage_inside_async_context(
    create_tables,
    async_db_sessionmaker,
):
    async with transaction(async_db_sessionmaker):
        with pytest.raises(TypeError, match="async transaction context"):
            with transaction():
                pass


@pytest.mark.asyncio
async def test_async_transaction__provider_must_yield_async_session():
    @asynccontextmanager
    async def invalid_provider():
        yield object()

    with pytest.raises(TypeError, match="must yield a SQLAlchemy AsyncSession"):
        async with transaction(invalid_provider):
            pass


@pytest.mark.asyncio
async def test_async_context__cannot_share_session_with_child_task(
    create_tables,
    async_db_sessionmaker,
):
    async with transaction(async_db_sessionmaker):
        task = asyncio.create_task(AsyncQueryManager(Item).count())
        with pytest.raises(RuntimeError, match="cannot be shared"):
            await task


@pytest.mark.asyncio
async def test_async_context__cannot_share_session_with_child_thread(
    create_tables,
    async_db_sessionmaker,
):
    def use_inherited_context():
        QueryManager(Item).count()

    loop = asyncio.get_running_loop()
    async with transaction(async_db_sessionmaker):
        inherited_context = contextvars.copy_context()
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = loop.run_in_executor(
                executor,
                inherited_context.run,
                use_inherited_context,
            )
            with pytest.raises(RuntimeError, match="cannot be shared"):
                await future


@pytest.mark.asyncio
async def test_async_context__child_task_cannot_reuse_ambient_session(
    create_tables,
    async_db_sessionmaker,
):
    async with async_db_sessionmaker() as session:

        async def reuse_ambient_session():
            async with transaction(session):
                pass

        async with transaction(session):
            task = asyncio.create_task(reuse_ambient_session())
            with pytest.raises(RuntimeError, match="cannot inherit"):
                await task


@pytest.mark.asyncio
async def test_async_context__child_task_can_open_independent_context(
    create_tables,
    async_db_sessionmaker,
):
    async def create_in_independent_context():
        async with transaction(async_db_sessionmaker):
            await AsyncQueryManager(Item).create(name="child")

    async with session_context(async_db_sessionmaker):
        await asyncio.create_task(create_in_independent_context())

    async with async_db_sessionmaker() as session:
        assert len((await session.execute(select(Item))).scalars().all()) == 1
