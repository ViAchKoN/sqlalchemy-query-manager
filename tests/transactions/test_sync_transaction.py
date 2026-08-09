import asyncio
import contextvars
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import pytest
from sqlalchemy import select

from sqlalchemy_query_manager import QueryManager, session_context, transaction
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


def _run_write_operation(operation, item_id=None):
    manager = QueryManager(Item)

    if operation == "create":
        manager.create(name="created")
    elif operation == "bulk_create":
        manager.bulk_create([{"name": "first"}, {"name": "second"}])
    elif operation == "get_or_create":
        manager.get_or_create(name="created")
    elif operation == "update":
        manager.where(id=item_id).update(name="updated")
    elif operation == "update_raw":
        manager.where(id=item_id).update_raw(name="updated")
    elif operation == "update_or_create":
        manager.update_or_create(id=item_id, defaults={"name": "updated"})
    elif operation == "bulk_update":
        manager.bulk_update([{"id": item_id, "name": "updated"}])
    elif operation == "delete":
        manager.where(id=item_id).delete()


@pytest.mark.parametrize("operation", WRITE_OPERATIONS)
def test_transaction__write_operation_is_rolled_back(
    operation,
    create_tables,
    sync_db_sessionmaker,
):
    item_id = None
    if operation in OPERATIONS_REQUIRING_EXISTING_ITEM:
        with sync_db_sessionmaker() as session:
            item = Item(name="original")
            session.add(item)
            session.commit()
            item_id = item.id

    with pytest.raises(RuntimeError, match="abort"):
        with transaction(sync_db_sessionmaker):
            _run_write_operation(operation, item_id)
            raise RuntimeError("abort")

    with sync_db_sessionmaker() as session:
        items = session.query(Item).all()
        if operation in OPERATIONS_REQUIRING_EXISTING_ITEM:
            assert len(items) == 1
            assert items[0].name == "original"
        else:
            assert items == []


def test_transaction__uses_one_session_across_models_and_commits(
    create_tables,
    sync_db_sessionmaker,
):
    owner_manager = QueryManager(Owner)
    group_manager = QueryManager(Group)
    item_manager = QueryManager(Item)

    with transaction(sync_db_sessionmaker) as control:
        owner = owner_manager.create(first_name="John", last_name="Doe")
        group = group_manager.create(name="team", owner_id=owner.id)
        item_manager.create(name="item", group_id=group.id)

        control.flush()

        assert owner_manager.count() == 1
        assert group_manager.count() == 1
        assert item_manager.count() == 1

    with sync_db_sessionmaker() as session:
        assert session.query(Owner).count() == 1
        assert session.query(Group).count() == 1
        assert session.query(Item).count() == 1


def test_transaction__exception_rolls_back_all_models(
    create_tables,
    sync_db_sessionmaker,
):
    with pytest.raises(RuntimeError, match="abort"):
        with transaction(sync_db_sessionmaker):
            QueryManager(Owner).create(first_name="John", last_name="Doe")
            QueryManager(Group).create(name="team")
            raise RuntimeError("abort")

    with sync_db_sessionmaker() as session:
        assert session.query(Owner).count() == 0
        assert session.query(Group).count() == 0


def test_transaction__nested_exception_rolls_back_savepoint_only(
    create_tables,
    sync_db_sessionmaker,
):
    with transaction(sync_db_sessionmaker):
        QueryManager(Owner).create(first_name="John", last_name="Doe")

        with pytest.raises(RuntimeError, match="nested abort"):
            with transaction():
                QueryManager(Group).create(name="discarded")
                raise RuntimeError("nested abort")

        QueryManager(Item).create(name="survives")

    with sync_db_sessionmaker() as session:
        assert session.query(Owner).count() == 1
        assert session.query(Group).count() == 0
        assert session.query(Item).count() == 1


def test_transaction__outer_rollback_includes_released_savepoint(
    create_tables,
    sync_db_sessionmaker,
):
    with pytest.raises(RuntimeError, match="outer abort"):
        with transaction(sync_db_sessionmaker):
            with transaction():
                QueryManager(Group).create(name="released savepoint")
            raise RuntimeError("outer abort")

    with sync_db_sessionmaker() as session:
        assert session.query(Group).count() == 0


def test_transaction__manual_commit_and_rollback_are_rejected(
    create_tables,
    sync_db_sessionmaker,
):
    with transaction(sync_db_sessionmaker) as control:
        with pytest.raises(RuntimeError, match="Manual commit"):
            control.commit()
        with pytest.raises(RuntimeError, match="Manual rollback"):
            control.rollback()


def test_transaction__handle_is_inactive_after_exit(
    create_tables,
    sync_db_sessionmaker,
):
    with transaction(sync_db_sessionmaker) as control:
        pass

    with pytest.raises(RuntimeError, match="no longer active"):
        control.flush()


def test_transaction__context_is_reset_after_exit(
    create_tables,
    sync_db_sessionmaker,
):
    with transaction(sync_db_sessionmaker):
        pass

    with pytest.raises(ValueError, match="requires a session source"):
        with transaction():
            pass

    with pytest.raises(RuntimeError, match="abort"):
        with transaction(sync_db_sessionmaker):
            raise RuntimeError("abort")

    with pytest.raises(ValueError, match="requires a session source"):
        with transaction():
            pass


def test_transaction__context_is_reset_when_commit_fails(
    create_tables,
    sync_db_sessionmaker,
    monkeypatch,
):
    context = transaction(sync_db_sessionmaker)

    def fail_commit():
        raise RuntimeError("commit failed")

    with pytest.raises(RuntimeError, match="commit failed"):
        with context:
            monkeypatch.setattr(context._transaction, "commit", fail_commit)

    with pytest.raises(ValueError, match="requires a session source"):
        with transaction():
            pass


def test_transaction__rejects_async_usage_inside_sync_context(
    create_tables,
    sync_db_sessionmaker,
):
    async def enter_async_transaction():
        async with transaction():
            pass

    with transaction(sync_db_sessionmaker):
        with pytest.raises(TypeError, match="synchronous transaction context"):
            asyncio.run(enter_async_transaction())


def test_transaction__provider_must_yield_session(sync_db_sessionmaker):
    @contextmanager
    def invalid_provider():
        yield object()

    with pytest.raises(TypeError, match="must yield a SQLAlchemy Session"):
        with transaction(invalid_provider):
            pass


def test_nested_session_context__cannot_commit_transaction(
    create_tables,
    sync_db_sessionmaker,
):
    with transaction(sync_db_sessionmaker):
        with session_context() as work:
            with pytest.raises(RuntimeError, match="Manual commit"):
                work.commit()
            with pytest.raises(RuntimeError, match="Manual rollback"):
                work.rollback()


def test_transaction__supports_borrowed_session_without_closing_it(
    create_tables,
    sync_db_sessionmaker,
):
    session = sync_db_sessionmaker()
    try:
        with transaction(session):
            QueryManager(Item).create(name="created")

        assert session.execute(select(Item)).scalars().one().name == "created"
    finally:
        session.close()


def test_transaction__uses_savepoint_for_active_borrowed_session(
    create_tables,
    sync_db_sessionmaker,
):
    session = sync_db_sessionmaker()
    try:
        session.begin()
        session.add(Owner(first_name="John", last_name="Doe"))

        with transaction(session):
            QueryManager(Group).create(name="nested")

        session.rollback()

        with sync_db_sessionmaker() as verification_session:
            assert verification_session.query(Owner).count() == 0
            assert verification_session.query(Group).count() == 0
    finally:
        session.close()


def test_transaction__supports_context_manager_provider(
    create_tables,
    sync_db_sessionmaker,
):
    @contextmanager
    def session_provider():
        with sync_db_sessionmaker() as session:
            yield session

    with transaction(session_provider):
        QueryManager(Owner).create(first_name="John", last_name="Doe")
        QueryManager(Group).create(name="team")

    with sync_db_sessionmaker() as session:
        assert session.query(Owner).count() == 1
        assert session.query(Group).count() == 1


def test_transaction__rejects_explicit_session_override(
    create_tables,
    sync_db_sessionmaker,
):
    with sync_db_sessionmaker() as other_session:
        with transaction(sync_db_sessionmaker):
            with pytest.raises(ValueError, match="cannot override"):
                QueryManager(Item).create(session=other_session, name="wrong session")


def test_transaction__select_for_update_uses_ambient_session(
    create_tables,
    sync_db_sessionmaker,
    item_sql_query_manager,
):
    item = item_sql_query_manager.query_manager.create(name="lock me")

    with transaction(sync_db_sessionmaker):
        locked = (
            item_sql_query_manager.query_manager.where(id=item.id)
            .select_for_update()
            .get()
        )
        assert locked.id == item.id


def test_sync_context__cannot_share_session_with_child_thread(
    create_tables,
    sync_db_sessionmaker,
):
    with transaction(sync_db_sessionmaker):
        context = contextvars.copy_context()
        with ThreadPoolExecutor(max_workers=1) as executor:
            result = executor.submit(context.run, QueryManager(Item).count)
            with pytest.raises(RuntimeError, match="cannot be shared across threads"):
                result.result()


def test_sync_context__child_thread_can_open_independent_context(
    create_tables,
    sync_db_sessionmaker,
):
    def create_in_independent_context():
        with transaction(sync_db_sessionmaker):
            QueryManager(Item).create(name="child")

    with session_context(sync_db_sessionmaker):
        context = contextvars.copy_context()
        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(context.run, create_in_independent_context).result()

    with sync_db_sessionmaker() as session:
        assert session.query(Item).count() == 1


def test_sync_context__child_thread_cannot_reuse_ambient_session(
    create_tables,
    sync_db_sessionmaker,
):
    session = sync_db_sessionmaker()

    def reuse_ambient_session():
        with transaction(session):
            pass

    try:
        with transaction(session):
            context = contextvars.copy_context()
            with ThreadPoolExecutor(max_workers=1) as executor:
                result = executor.submit(context.run, reuse_ambient_session)
                with pytest.raises(RuntimeError, match="cannot inherit"):
                    result.result()
    finally:
        session.close()
