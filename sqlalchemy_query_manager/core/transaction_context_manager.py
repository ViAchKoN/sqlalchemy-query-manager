class BaseSessionContextManager:
    def __init__(
        self,
        sessionmaker,
        session=None,
    ) -> None:  # type: ignore
        self.sessionmaker = sessionmaker
        self.session = session


class TransactionSessionContextManager(BaseSessionContextManager):
    def __enter__(self):  # type: ignore
        if self.session is not None:
            self.resource = self.session
        else:
            self.resource = self.sessionmaker().__enter__()
        return self.resource

    def __exit__(self, exc_type, exc, tb):  # type: ignore
        if self.session is None:
            self.resource.__exit__(exc_type, exc, tb)


class AsyncTransactionSessionContextManager(BaseSessionContextManager):
    async def __aenter__(self):  # type: ignore
        if self.session is not None:
            self.resource = self.session
        else:
            self.resource = await self.sessionmaker().__aenter__()
        return self.resource

    async def __aexit__(self, exc_type, exc, tb):  # type: ignore
        if self.session is None:
            await self.resource.__aexit__(exc_type, exc, tb)
