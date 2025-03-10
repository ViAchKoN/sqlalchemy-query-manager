from functools import wraps

from sqlalchemy_query_manager.core.transaction_context_manager import (
    AsyncTransactionSessionContextManager,
    TransactionSessionContextManager,
)


def get_session(func):
    """Decorator that provides a sync session from inside a class."""

    @wraps(func)
    def wrapper(self, *args, session=None, **kwargs):
        with TransactionSessionContextManager(
            session=session or self.session,
        ) as managed_session:
            expunge = True
            if session:
                expunge = False

            return func(self, session=managed_session, expunge=expunge, *args, **kwargs)

    return wrapper


def get_async_session(func):
    """Decorator that provides an async session from inside a class."""

    @wraps(func)
    async def wrapper(self, *args, session=None, **kwargs):
        async with AsyncTransactionSessionContextManager(
            session=session or self.session,
        ) as managed_session:
            return await func(self, session=managed_session, *args, **kwargs)

    return wrapper
