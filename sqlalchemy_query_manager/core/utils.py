from functools import wraps

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from sqlalchemy_query_manager.core.transaction_context_manager import (
    AsyncTransactionSessionContextManager,
    TransactionSessionContextManager,
)


def _get_explicit_session(self, session):
    if session is not None:
        return session

    if getattr(self, "_session_is_explicit", False):
        return self.session

    return None


def _has_for_update(self):
    return getattr(self, "_for_update", None) is not None


def _validate_sync_for_update_session(self, session):
    if not _has_for_update(self):
        return

    explicit_session = _get_explicit_session(self, session)
    if not isinstance(explicit_session, Session):
        raise ValueError(
            "select_for_update() requires an explicit SQLAlchemy Session. "
            "Pass session=... to the executing method or use with_session()."
        )


def _validate_async_for_update_session(self, session):
    if not _has_for_update(self):
        return

    explicit_session = _get_explicit_session(self, session)
    if not isinstance(explicit_session, AsyncSession):
        raise ValueError(
            "select_for_update() requires an explicit SQLAlchemy AsyncSession. "
            "Pass session=... to the executing method or use with_session()."
        )


def get_session(func):
    """Decorator that provides a sync session from inside a class."""

    @wraps(func)
    def wrapper(self, *args, session=None, **kwargs):
        _validate_sync_for_update_session(self, session)

        ctx_manager = TransactionSessionContextManager(
            session=session or self.session,
        )

        with ctx_manager as managed_session:
            expunge = True
            if session or ctx_manager.is_session_already_set:
                expunge = False

            return func(self, session=managed_session, expunge=expunge, *args, **kwargs)

    return wrapper


def get_async_session(func):
    """Decorator that provides an async session from inside a class."""

    @wraps(func)
    async def wrapper(self, *args, session=None, **kwargs):
        _validate_async_for_update_session(self, session)

        async with AsyncTransactionSessionContextManager(
            session=session or self.session
        ) as managed_session:
            return await func(self, session=managed_session, *args, **kwargs)

    return wrapper
