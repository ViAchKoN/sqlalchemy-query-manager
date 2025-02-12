from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session, sessionmaker


class BaseSessionContextManager:
    def __init__(
        self,
        session,
    ) -> None:  # type: ignore
        self.session = session
        self.__to_exit = False


class TransactionSessionContextManager(BaseSessionContextManager):
    def __enter__(self):  # type: ignore
        if isinstance(self.session, sessionmaker):
            self.resource = self.session().__enter__()
            self._to_exit = True
        elif isinstance(self.session, Session):
            self.resource = self.session
        else:
            raise NotImplementedError
        return self.resource

    def __exit__(self, exc_type, exc, tb):  # type: ignore
        if self._to_exit:
            self.resource.__exit__(exc_type, exc, tb)


class AsyncTransactionSessionContextManager(BaseSessionContextManager):
    async def __aenter__(self):  # type: ignore
        if isinstance(self.session, sessionmaker):
            self.resource = await self.session().__aenter__()
            self.__to_exit = True
        elif isinstance(self.session, AsyncSession):
            self.resource = self.session
        else:
            raise NotImplementedError
        return self.resource

    async def __aexit__(self, exc_type, exc, tb):  # type: ignore
        if self.__to_exit:
            await self.resource.__aexit__(exc_type, exc, tb)
