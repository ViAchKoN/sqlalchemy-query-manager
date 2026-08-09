import asyncio
from contextlib import contextmanager

import pytest
from sqlalchemy import event

from sqlalchemy_query_manager import QueryManager, session_context, transaction
from tests.models import Group, Item, Owner


def test_session_context__supports_manual_flush_commit_and_rollback(
    create_tables,
    sync_db_sessionmaker,
):
    owner_manager = QueryManager(Owner)
    group_manager = QueryManager(Group)
    item_manager = QueryManager(Item)

    with session_context(sync_db_sessionmaker) as session_ctx:
        owner = owner_manager.create(first_name="John", last_name="Doe")
        group_manager.create(name="committed", owner_id=owner.id)
        session_ctx.flush()
        session_ctx.commit()

        item_manager.create(name="rolled back")
        session_ctx.rollback()

        assert owner_manager.count() == 1
        assert group_manager.count() == 1
        assert item_manager.count() == 0

    with sync_db_sessionmaker() as session:
        assert session.query(Owner).count() == 1
        assert session.query(Group).count() == 1
        assert session.query(Item).count() == 0


def test_session_context__does_not_commit_pending_work_on_exit(
    create_tables,
    sync_db_sessionmaker,
):
    with session_context(sync_db_sessionmaker):
        QueryManager(Item).create(name="not committed")

    with sync_db_sessionmaker() as session:
        assert session.query(Item).count() == 0


def test_session_context__leaves_borrowed_session_lifecycle_to_caller(
    create_tables,
    sync_db_sessionmaker,
):
    session = sync_db_sessionmaker()
    try:
        with session_context(session):
            QueryManager(Item).create(name="pending")

        assert session.in_transaction()
        assert session.query(Item).one().name == "pending"

        session.rollback()
        assert session.query(Item).count() == 0
    finally:
        session.close()


def test_session_context__supports_context_manager_provider(
    create_tables,
    sync_db_sessionmaker,
):
    @contextmanager
    def session_provider():
        with sync_db_sessionmaker() as session:
            yield session

    with session_context(session_provider) as session_ctx:
        QueryManager(Item).create(name="committed")
        session_ctx.commit()

    with sync_db_sessionmaker() as session:
        assert session.query(Item).one().name == "committed"


def test_session_context__nested_context_reuses_session(
    create_tables,
    sync_db_sessionmaker,
):
    with session_context(sync_db_sessionmaker) as outer:
        owner = QueryManager(Owner).create(first_name="John", last_name="Doe")

        with session_context() as inner:
            QueryManager(Group).create(name="team", owner_id=owner.id)
            inner.flush()

        outer.commit()

    with sync_db_sessionmaker() as session:
        assert session.query(Owner).count() == 1
        assert session.query(Group).count() == 1


def test_session_context__transaction_does_not_commit_outer_transaction(
    create_tables,
    sync_db_sessionmaker,
):
    with session_context(sync_db_sessionmaker) as session_ctx:
        QueryManager(Owner).create(first_name="John", last_name="Doe")
        with transaction():
            QueryManager(Group).create(name="savepoint")
        session_ctx.rollback()

    with sync_db_sessionmaker() as session:
        assert session.query(Owner).count() == 0
        assert session.query(Group).count() == 0


def test_session_context__nested_context_rejects_different_source(
    create_tables,
    sync_db_sessionmaker,
):
    def different_source():
        return sync_db_sessionmaker()

    with session_context(sync_db_sessionmaker):
        with pytest.raises(ValueError, match="must use the ambient session"):
            with session_context(different_source):
                pass


def test_session_context__handle_is_inactive_after_exit(
    create_tables,
    sync_db_sessionmaker,
):
    with session_context(sync_db_sessionmaker) as session_ctx:
        pass

    for method_name in ("flush", "commit", "rollback"):
        with pytest.raises(RuntimeError, match="no longer active"):
            getattr(session_ctx, method_name)()


def test_session_context__outer_context_requires_source():
    with pytest.raises(ValueError, match="requires a session source"):
        with session_context():
            pass


def test_session_context__rejects_async_usage_inside_sync_context(
    create_tables,
    sync_db_sessionmaker,
):
    async def enter_async_context():
        async with session_context():
            pass

    with session_context(sync_db_sessionmaker):
        with pytest.raises(TypeError, match="synchronous session context"):
            asyncio.run(enter_async_context())


def test_explicit_session__preserves_configured_sessionmaker_auto_commit(
    create_tables,
    sync_db_sessionmaker,
    item_sql_query_manager,
):
    with sync_db_sessionmaker() as session:
        item_sql_query_manager.query_manager.create(
            session=session,
            name="committed",
        )

        with sync_db_sessionmaker() as other_session:
            assert other_session.query(Item).count() == 1

    with sync_db_sessionmaker() as verification_session:
        assert verification_session.query(Item).count() == 1


def test_read_with_sessionmaker__does_not_commit(
    create_tables,
    sync_db_sessionmaker,
):
    commits = []

    def record_commit(session):
        commits.append(session)

    event.listen(sync_db_sessionmaker.class_, "after_commit", record_commit)
    try:
        assert QueryManager(Item, sync_db_sessionmaker).count() == 0
    finally:
        event.remove(sync_db_sessionmaker.class_, "after_commit", record_commit)

    assert commits == []
