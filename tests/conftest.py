import os
from contextlib import contextmanager

import pytest
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from sqlalchemy_utils import create_database, database_exists, drop_database

from tests.models import BaseModel


TEST_DB_URL = "postgresql+psycopg2://{POSTGRES_CREDENTIALS}{POSTGRES_HOST}/sqlalchemy_name"

POSTGRES_CREDENTIALS = ""
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", None)
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
if POSTGRES_PASSWORD:
    POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_CREDENTIALS = f"{POSTGRES_USER}:{POSTGRES_PASSWORD}@"


TEST_DB_URL = TEST_DB_URL.format(
    POSTGRES_CREDENTIALS=POSTGRES_CREDENTIALS, POSTGRES_HOST=POSTGRES_HOST
)
ASYNC_TEST_DB_URL = TEST_DB_URL.replace("psycopg2", "asyncpg")


@pytest.fixture(scope="package")
def setup_database():
    if database_exists(TEST_DB_URL):
        drop_database(TEST_DB_URL)
    create_database(TEST_DB_URL)
    yield
    drop_database(TEST_DB_URL)


@pytest.fixture(scope="package")
def sync_db_engine():
    return create_engine(TEST_DB_URL)


@pytest.fixture(scope="package")
def sync_db_sessionmaker(sync_db_engine):
    return sessionmaker(autocommit=False, autoflush=False, bind=sync_db_engine)


@pytest.fixture(scope="function")
def create_tables(setup_database, sync_db_engine) -> None:
    BaseModel.metadata.create_all(sync_db_engine)
    yield
    BaseModel.metadata.drop_all(sync_db_engine)


@pytest.fixture(scope="function")
def db_session(create_tables, sync_db_sessionmaker):
    session = sync_db_sessionmaker()
    yield session
    session.close()


@pytest.fixture(scope="function")
def async_db_engine(create_tables):
    return create_async_engine(
        ASYNC_TEST_DB_URL,
        future=True,
        poolclass=NullPool,
    )

@pytest.fixture(scope="function")
def async_db_sessionmaker(
    async_db_engine
):
    return sessionmaker(
        async_db_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )


def get_test_session():
    test_engine = create_engine(TEST_DB_URL)
    test_session = sessionmaker(
        autoflush=False,
        bind=test_engine,
        expire_on_commit=False,
    )
    return test_session


@contextmanager
def db_test_session():
    test_session = get_test_session()
    with test_session() as session:
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
