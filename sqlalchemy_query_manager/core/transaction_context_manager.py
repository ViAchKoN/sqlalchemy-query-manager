from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session


class BaseSessionContextManager:
    def __init__(self, session) -> None:  # type: ignore
        self.session = session
        # if a session is passed, and we need already existing one
        self.is_session_already_set = False
        self.owns_session = False

        self._to_exit = False
        self._ctx = None  # if a session passed as a context manager

    def _create_resource(self):
        if callable(self.session):
            return self.session()
        return self.session


class TransactionSessionContextManager(BaseSessionContextManager):
    def __enter__(self):  # type: ignore
        if isinstance(self.session, Session):
            self.resource = self.session
            self.is_session_already_set = True
        else:
            resource = self._create_resource()

            if isinstance(resource, Session):
                self.resource = resource
                self.owns_session = True
                self._to_exit = True
            elif callable(getattr(resource, "__enter__", None)):
                self._ctx = resource
                self.resource = self._ctx.__enter__()
                if not isinstance(self.resource, Session):
                    raise TypeError(
                        "The session context manager must yield a SQLAlchemy Session."
                    )
                self.owns_session = True
                self._to_exit = True
            else:
                raise NotImplementedError

        return self.resource

    def __exit__(self, exc_type, exc, tb):  # type: ignore
        if not self._to_exit:
            return False

        if self._ctx:
            return self._ctx.__exit__(exc_type, exc, tb)

        self.resource.close()

        return False


class AsyncTransactionSessionContextManager(BaseSessionContextManager):
    async def __aenter__(self):  # type: ignore
        if isinstance(self.session, AsyncSession):
            self.resource = self.session
            self.is_session_already_set = True
        else:
            resource = self._create_resource()

            if isinstance(resource, AsyncSession):
                self.resource = resource
                self.owns_session = True
                self._to_exit = True
            elif callable(getattr(resource, "__aenter__", None)):
                self._ctx = resource
                self.resource = await self._ctx.__aenter__()
                if not isinstance(self.resource, AsyncSession):
                    raise TypeError(
                        "The async session context manager must yield a SQLAlchemy "
                        "AsyncSession."
                    )
                self.owns_session = True
                self._to_exit = True
            else:
                raise NotImplementedError

        return self.resource

    async def __aexit__(self, exc_type, exc, tb):  # type: ignore
        if not self._to_exit:
            return False

        if self._ctx:
            return await self._ctx.__aexit__(exc_type, exc, tb)

        await self.resource.close()

        return False
