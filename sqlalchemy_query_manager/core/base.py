import typing

from dataclass_sqlalchemy_mixins.base.mixins import SqlAlchemyFilterConverterMixin, SqlAlchemyOrderConverterMixin
from sqlalchemy import select, func, inspect
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import InstrumentedAttribute, Session, DeclarativeMeta, joinedload

from sqlalchemy_query_manager.consts import classproperty
from sqlalchemy_query_manager.core.transaction_context_manager import AsyncTransactionSessionContextManager, \
    TransactionSessionContextManager


class QueryManager(SqlAlchemyFilterConverterMixin, SqlAlchemyOrderConverterMixin):
    def __init__(self, model, sessionmaker=None, session=None):
        self.ConverterConfig.model = model

        self.sessionmaker: "sessionmaker" = sessionmaker
        self.session: typing.Union[Session, AsyncSession] = session

        self.fields = None

        self.filters = {}
        self._order_by = set()

        self.models_to_join = []

        self._limit = None
        self._offset = None

        self._binary_expressions = []
        self._unary_expressions = []

    def join_models(
        self,
        query,
        models: typing.List[DeclarativeMeta],
    ):
        query = query

        joined_models = []
        join_methods = [
            "_join_entities",  # sqlalchemy <= 1.3
            "_legacy_setup_joins",  # sqlalchemy == 1.4
            "_setup_joins",  # sqlalchemy == 2.0
        ]
        for join_method in join_methods:
            if hasattr(query, join_method):
                joined_models = [
                    join[0].entity_namespace for join in getattr(query, join_method)
                ]
                # Different sqlalchemy versions might have several join methods
                # but only one of them will return correct joined models list
                if joined_models:
                    break

        for model in models:
            if model != self.ConverterConfig.model and model not in joined_models:
                query = query.join(model)

        return query

    def get_model_field(
        self,
        field: str,
    ):
        db_field = None

        if "__" in field:
            # There might be several relationship
            # that is why string might look like
            # related_model1__related_model2__related_model2_field
            field_params = field.split("__")

            if len(field_params) > 1:
                models, db_field = self.get_foreign_key_filtered_column(
                    models_path_to_look=field_params,
                )
                if db_field is None:
                    raise ValueError
            else:
                field = field_params[0]

        if db_field is None:
            db_field = getattr(self.ConverterConfig.model, field)

        return db_field

    def only(self, *fields):
        _fields = []
        for field in fields:
            if isinstance(field, InstrumentedAttribute):
                pass
            elif isinstance(field, str):
                field = self.get_model_field(field)
            else:
                raise NotImplementedError(
                    'Should be either InstrumentedAttribute class or str'
                )
            _fields.append(field)

        self.fields = _fields
        return self

    def limit(self, limit):
        self._limit = limit
        return self

    def offset(self, offset):
        self._offset = offset
        return self

    @property
    def binary_expressions(self):
        if not self._binary_expressions and self.filters:
            models_binary_expressions = self.get_models_binary_expressions(
                filters=self.filters
            )

            for model_binary_expression in models_binary_expressions:
                self.models_to_join.extend(model_binary_expression.get('models'))
                self._binary_expressions.append(model_binary_expression.get('binary_expression'))
        return self._binary_expressions

    @property
    def unary_expressions(self):
        if not self._unary_expressions and self._order_by:
            models_unary_expressions = self.get_models_unary_expressions(
                order_by=self._order_by
            )

            for model_unary_expression in models_unary_expressions:
                self.models_to_join.extend(model_unary_expression.get('models'))
                self._binary_expressions.append(model_unary_expression.get('unary_expression'))

        return self._binary_expressions

    @property
    def query(self):
        query = select(self.ConverterConfig.model)

        if self.fields:
            query = select(*self.fields)

        if self.binary_expressions:
            if self.models_to_join != [
                self.ConverterConfig.model,
            ]:
                query = self.join_models(
                    query=query,
                    models=self.models_to_join,
                )
            query = query.where(*self.binary_expressions)

        if self.unary_expressions:
            if self.models_to_join != [
                self.ConverterConfig.model,
            ]:
                query = self.join_models(
                    query=query,
                    models=self.models_to_join
                )
            query = query.order_by(*self.unary_expressions)

        if self._offset:
            query = query.offset(self._offset)

        if self._limit:
            query = query.limit(self._limit)

        return query

    def all(self, session=None):
        with TransactionSessionContextManager(
            sessionmaker=self.sessionmaker,
            session=session,
        ) as session:
            result = session.execute(self.query)

            if not self.fields:
                result = result.scalars()

            result = result.all()

            if not session or not self.session:
                session.expunge_all()

            return result

    def first(self, session=None):
        with TransactionSessionContextManager(
            sessionmaker=self.sessionmaker,
            session=session,
        ) as session:
            result = session.execute(self.query)

            if not self.fields:
                result = result.scalars()

            result = result.first()

            if self.fields:
                pass
            elif result and (not session or not self.session):
                session.expunge(result)

            return result

    def last(self, session=None):
        primary_key = inspect(self.ConverterConfig.model).primary_key[0].name
        primary_key_row = getattr(self.ConverterConfig.model, primary_key)

        with TransactionSessionContextManager(
            sessionmaker=self.sessionmaker,
            session=session,
        ) as session:
            query = self.query.order_by(-primary_key_row)

            result = session.execute(query)

            if not self.fields:
                result = result.scalars()

            result = result.first()

            if self.fields:
                pass
            elif result and (not session or not self.session):
                session.expunge(result)

            return result

    def get(self, session=None, **kwargs):
        binary_expressions = self.get_binary_expressions(
            filters=kwargs
        )

        with TransactionSessionContextManager(
            sessionmaker=self.sessionmaker,
            session=session or self.session,
        ) as session:
            result = session.query(self.ConverterConfig.model).filter(*binary_expressions).first()

            if self.fields:
                pass
            elif result and (not session or not self.session):
                session.expunge(result)

        return result

    def where(self, **kwargs):
        self.filters = {
            **self.filters,
            **kwargs,
        }

        return self

    def order_by(self, *args):
        self._order_by.update(set(args))

        return self

    def count(self, session=None):
        with TransactionSessionContextManager(
            sessionmaker=self.sessionmaker,
            session=session,
        ) as session:
            count = session.execute(select(func.count()).select_from(self.query)).scalar_one()
        return count

    def with_session(self, session):
        self.session = session
        return self


class AsyncQueryManager(QueryManager):
    async def first(self, session=None):
        async with AsyncTransactionSessionContextManager(
            sessionmaker=self.sessionmaker,
            session=session
        ) as session:
            result = await session.execute(self.query)

            if not self.fields:
                result = result.scalars()
            return result.first()

    async def last(self, session=None):
        primary_key = inspect(self.ConverterConfig.model).primary_key[0].name
        primary_key_row = getattr(self.ConverterConfig.model, primary_key)

        async with AsyncTransactionSessionContextManager(
            sessionmaker=self.sessionmaker,
            session=session
        ) as session:
            query = self.query.order_by(-primary_key_row)

            result = await session.execute(query)

            if not self.fields:
                result = result.scalars()
            return result.first()

    async def get(self, session=None, **kwargs):
        binary_expressions = self.get_binary_expressions(
            filters=kwargs
        )

        async with AsyncTransactionSessionContextManager(
            sessionmaker=self.sessionmaker,
            session=session or self.session,
        ) as session:
            result = (
                await session.execute(
                    select(self.ConverterConfig.model).where(*binary_expressions)
                )
            ).scalars().first()
        return result

    async def all(self, session=None):
        async with AsyncTransactionSessionContextManager(
            sessionmaker=self.sessionmaker,
            session=session
        ) as session:
            result = await session.execute(self.query)

            if not self.fields:
                result = result.scalars()

            return result.all()

    async def count(self, session=None):
        async with AsyncTransactionSessionContextManager(
            sessionmaker=self.sessionmaker,
            session=session
        ) as session:
            count = (await session.execute(select(func.count()).select_from(self.query))).scalar_one()
        return count


class BaseModelQueryManagerMixin:
    class QueryManagerConfig:
        sessionmaker = None
        session = None


class ModelQueryManagerMixin(BaseModelQueryManagerMixin):
    @classproperty
    def query_manager(cls):
        return QueryManager(
            model=cls,
            sessionmaker=getattr(cls.QueryManagerConfig, "sessionmaker", None),
            session=getattr(cls.QueryManagerConfig, "session", None),
        )


class AsyncModelQueryManagerMixin(BaseModelQueryManagerMixin):
    @classproperty
    def query_manager(cls):
        return AsyncQueryManager(
            model=cls,
            sessionmaker=getattr(cls.QueryManagerConfig, "sessionmaker", None),
            session=getattr(cls.QueryManagerConfig, "session", None),
        )
