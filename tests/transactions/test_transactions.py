import pytest

from tests import models_factory


def test_transaction(
    create_tables,
    sync_db_sessionmaker,
    item_sql_query_manager,
):
    with sync_db_sessionmaker() as transaction_session:
        # Checking that there are no objects in database in transaction
        assert item_sql_query_manager.query_manager.first(session=transaction_session) is None

        # Creating object in database
        item = models_factory.ItemFactory.build()

        transaction_session.add(item)
        transaction_session.flush()

        # Checking that object is not accessible from base session
        assert item_sql_query_manager.query_manager.first() is None

        # Checking that object is accessible withing transaction
        assert item_sql_query_manager.query_manager.first(session=transaction_session)

        transaction_session.commit()

    # Checking that object now accessible outside of session
    assert item_sql_query_manager.query_manager.first()


@pytest.mark.asyncio
async def test_async_transaction(
    create_tables,
    async_db_sessionmaker,
    async_item_sql_query_manager,
):
    async with async_db_sessionmaker() as transaction_session:
        # Checking that there are no objects in database in transaction
        assert await async_item_sql_query_manager.query_manager.first(session=transaction_session) is None

        # Creating object in database
        item = models_factory.ItemFactory.build()

        transaction_session.add(item)
        await transaction_session.flush()

        # Checking that object is not accessible from base session
        assert await async_item_sql_query_manager.query_manager.first() is None

        # Checking that object is accessible withing transaction
        assert await async_item_sql_query_manager.query_manager.first(session=transaction_session)

        await transaction_session.commit()

    # Checking that object now accessible outside of session
    assert await async_item_sql_query_manager.query_manager.first()
