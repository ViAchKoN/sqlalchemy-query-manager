from contextlib import _AsyncGeneratorContextManager, _GeneratorContextManager

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session, sessionmaker


def is_generator_context_manager(obj):
    """
    Helper function to check if the object is a generator-based context manager
    (created by @contextmanager).
    """
    # Ensure the object is callable, and if called, returns a generator.
    if callable(obj):
        try:
            # Check if calling it returns a generator (context manager)
            result = obj()
            if isinstance(result, _GeneratorContextManager) or isinstance(
                result, _AsyncGeneratorContextManager
            ):
                return True
        except Exception:
            pass
    return False


def is_async_context_manager(obj):
    return callable(getattr(obj, "__aenter__", None)) and callable(
        getattr(obj, "__aexit__", None)
    )


class BaseSessionContextManager:
    def __init__(
        self,
        session,
    ) -> None:  # type: ignore
        self.session = session
        self._to_exit = False
        self._ctx = None  # if a session passed as a context manager


class TransactionSessionContextManager(BaseSessionContextManager):
    def __enter__(self):  # type: ignore
        if isinstance(self.session, sessionmaker):
            self.resource = self.session().__enter__()
            self._to_exit = True
        elif isinstance(self.session, Session):
            self.resource = self.session
        elif is_generator_context_manager(self.session):
            self._ctx = self.session()
            self.resource = self._ctx.__enter__()
            self._to_exit = True
        else:
            raise NotImplementedError
        return self.resource

    def __exit__(self, exc_type, exc, tb):  # type: ignore
        if self._to_exit:
            if self._ctx:
                self._ctx.__exit__(exc_type, exc, tb)
            else:
                self.resource.__exit__(exc_type, exc, tb)


class AsyncTransactionSessionContextManager(BaseSessionContextManager):
    async def __aenter__(self):  # type: ignore
        if isinstance(self.session, sessionmaker):
            self.resource = await self.session().__aenter__()
            self._to_exit = True
        elif isinstance(self.session, AsyncSession):
            self.resource = self.session
        elif is_generator_context_manager(self.session):
            self._ctx = self.session()
            self.resource = await self._ctx.__aenter__()
            self._to_exit = True
        else:
            raise NotImplementedError
        return self.resource

    async def __aexit__(self, exc_type, exc, tb):  # type: ignore
        if self._to_exit:
            if self._ctx:
                await self._ctx.__aexit__(exc_type, exc, tb)
            else:
                await self.resource.__aexit__(exc_type, exc, tb)
