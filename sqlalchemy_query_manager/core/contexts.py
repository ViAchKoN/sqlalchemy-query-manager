import asyncio
import contextvars
import dataclasses
import sys
import threading
import typing

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from sqlalchemy_query_manager.core.transaction_context_manager import (
    AsyncTransactionSessionContextManager,
    TransactionSessionContextManager,
)


@dataclasses.dataclass
class _ContextState:
    session: typing.Union[Session, AsyncSession]
    source: typing.Any
    is_async: bool
    owner_task: typing.Any = None
    owner_thread_id: typing.Optional[int] = None
    transaction_depth: int = 0


_current_context = contextvars.ContextVar(
    "sqlalchemy_query_manager_context",
    default=None,
)


def _get_asyncio_task():
    try:
        return asyncio.current_task()
    except RuntimeError:
        return None


def _validate_context_owner(state):
    if state.is_async:
        current_task = _get_asyncio_task()
        if current_task is not state.owner_task:
            raise RuntimeError(
                "An ambient AsyncSession cannot be shared across concurrent tasks. "
                "Open a separate context inside each task."
            )
    elif threading.get_ident() != state.owner_thread_id:
        raise RuntimeError(
            "An ambient Session cannot be shared across threads. "
            "Open a separate context inside each thread."
        )


def get_current_context():
    state = _current_context.get()
    if state is not None:
        _validate_context_owner(state)
    return state


def _get_context_for_entry(source, is_async):
    state = _current_context.get()
    if state is None or state.is_async != is_async:
        return state

    if is_async:
        is_owner = _get_asyncio_task() is state.owner_task
        worker = "task"
        session_type = "AsyncSession"
    else:
        is_owner = threading.get_ident() == state.owner_thread_id
        worker = "thread"
        session_type = "Session"

    if is_owner:
        return state

    if source is None or source is state.session:
        raise RuntimeError(
            "A child {worker} cannot inherit the ambient {session_type}. Pass a "
            "session factory or context-manager provider to open an independent "
            "context inside the child {worker}.".format(
                worker=worker,
                session_type=session_type,
            )
        )
    return None


def _validate_nested_source(state, source):
    if source is None or source is state.source or source is state.session:
        return
    raise ValueError(
        "A nested context must use the ambient session. Omit the session source "
        "or pass the same source used by the outer context."
    )


class SessionContextHandle:
    def __init__(self, state):
        self._state = state
        self._active = True

    def _ensure_active(self):
        if not self._active:
            raise RuntimeError("The session context is no longer active.")
        _validate_context_owner(self._state)

    def flush(self):
        self._ensure_active()
        return self._state.session.flush()

    def commit(self):
        self._ensure_active()
        if self._state.transaction_depth:
            raise RuntimeError("Manual commit is not allowed inside transaction().")
        return self._state.session.commit()

    def rollback(self):
        self._ensure_active()
        if self._state.transaction_depth:
            raise RuntimeError("Manual rollback is not allowed inside transaction().")
        return self._state.session.rollback()


class TransactionHandle(SessionContextHandle):
    def commit(self):
        raise RuntimeError(
            "Manual commit is not allowed inside transaction(). "
            "Use session_context() for commit-as-you-go workflows."
        )

    def rollback(self):
        raise RuntimeError(
            "Manual rollback is not allowed inside transaction(). "
            "Use a nested transaction() savepoint or session_context()."
        )


class _SessionContext:
    def __init__(self, source):
        self.source = source
        self._state = None
        self._token = None
        self._manager = None
        self._handle = None
        self._is_outermost = False

    def __enter__(self):
        current = _get_context_for_entry(self.source, is_async=False)
        if current is not None:
            if current.is_async:
                raise TypeError("Use 'async with' inside an async session context.")
            _validate_nested_source(current, self.source)
            self._state = current
        else:
            if self.source is None:
                raise ValueError(
                    "An outer session_context() requires a session source."
                )
            self._manager = TransactionSessionContextManager(
                session=self.source,
            )
            session = self._manager.__enter__()
            self._state = _ContextState(
                session=session,
                source=self.source,
                is_async=False,
                owner_thread_id=threading.get_ident(),
            )
            self._token = _current_context.set(self._state)
            self._is_outermost = True

        self._handle = SessionContextHandle(self._state)
        return self._handle

    def __exit__(self, exc_type, exc, tb):
        self._handle._active = False
        if not self._is_outermost:
            return False

        try:
            if self._manager.owns_session and self._state.session.in_transaction():
                self._state.session.rollback()
            return self._manager.__exit__(exc_type, exc, tb)
        finally:
            _current_context.reset(self._token)

    async def __aenter__(self):
        current = _get_context_for_entry(self.source, is_async=True)
        if current is not None:
            if not current.is_async:
                raise TypeError("Use 'with' inside a synchronous session context.")
            _validate_nested_source(current, self.source)
            self._state = current
        else:
            if self.source is None:
                raise ValueError(
                    "An outer session_context() requires a session source."
                )
            self._manager = AsyncTransactionSessionContextManager(
                session=self.source,
            )
            session = await self._manager.__aenter__()
            self._state = _ContextState(
                session=session,
                source=self.source,
                is_async=True,
                owner_task=_get_asyncio_task(),
            )
            self._token = _current_context.set(self._state)
            self._is_outermost = True

        self._handle = SessionContextHandle(self._state)
        return self._handle

    async def __aexit__(self, exc_type, exc, tb):
        self._handle._active = False
        if not self._is_outermost:
            return False

        try:
            if self._manager.owns_session and self._state.session.in_transaction():
                await self._state.session.rollback()
            return await self._manager.__aexit__(exc_type, exc, tb)
        finally:
            _current_context.reset(self._token)


class _TransactionContext:
    def __init__(self, source):
        self.source = source
        self._state = None
        self._token = None
        self._manager = None
        self._transaction = None
        self._handle = None
        self._is_outermost = False

    def __enter__(self):
        current = _get_context_for_entry(self.source, is_async=False)
        if current is not None:
            if current.is_async:
                raise TypeError("Use 'async with' inside an async transaction context.")
            _validate_nested_source(current, self.source)
            self._state = current
            self._transaction = self._state.session.begin_nested()
        else:
            if self.source is None:
                raise ValueError("An outer transaction() requires a session source.")
            self._manager = TransactionSessionContextManager(
                session=self.source,
            )
            session = self._manager.__enter__()
            self._state = _ContextState(
                session=session,
                source=self.source,
                is_async=False,
                owner_thread_id=threading.get_ident(),
            )
            self._token = _current_context.set(self._state)
            self._is_outermost = True
            if session.in_transaction():
                self._transaction = session.begin_nested()
            else:
                self._transaction = session.begin()

        self._handle = TransactionHandle(self._state)
        self._state.transaction_depth += 1
        return self._handle

    def __exit__(self, exc_type, exc, tb):
        self._handle._active = False
        manager_exception = (exc_type, exc, tb)
        try:
            if exc_type is None:
                self._transaction.commit()
            else:
                self._transaction.rollback()
        except BaseException:
            manager_exception = sys.exc_info()
            raise
        finally:
            self._state.transaction_depth -= 1
            if self._is_outermost:
                try:
                    self._manager.__exit__(*manager_exception)
                finally:
                    _current_context.reset(self._token)
        return False

    async def __aenter__(self):
        current = _get_context_for_entry(self.source, is_async=True)
        if current is not None:
            if not current.is_async:
                raise TypeError("Use 'with' inside a synchronous transaction context.")
            _validate_nested_source(current, self.source)
            self._state = current
            self._transaction = await self._state.session.begin_nested()
        else:
            if self.source is None:
                raise ValueError("An outer transaction() requires a session source.")
            self._manager = AsyncTransactionSessionContextManager(
                session=self.source,
            )
            session = await self._manager.__aenter__()
            self._state = _ContextState(
                session=session,
                source=self.source,
                is_async=True,
                owner_task=_get_asyncio_task(),
            )
            self._token = _current_context.set(self._state)
            self._is_outermost = True
            if session.in_transaction():
                self._transaction = await session.begin_nested()
            else:
                self._transaction = await session.begin()

        self._handle = TransactionHandle(self._state)
        self._state.transaction_depth += 1
        return self._handle

    async def __aexit__(self, exc_type, exc, tb):
        self._handle._active = False
        manager_exception = (exc_type, exc, tb)
        try:
            if exc_type is None:
                await self._transaction.commit()
            else:
                await self._transaction.rollback()
        except BaseException:
            manager_exception = sys.exc_info()
            raise
        finally:
            self._state.transaction_depth -= 1
            if self._is_outermost:
                try:
                    await self._manager.__aexit__(*manager_exception)
                finally:
                    _current_context.reset(self._token)
        return False


def session_context(session_source=None):
    """Create an ambient session context with explicit transaction control."""
    return _SessionContext(session_source)


def transaction(session_source=None):
    """Create an atomic transaction context, using SAVEPOINTs when nested."""
    return _TransactionContext(session_source)
