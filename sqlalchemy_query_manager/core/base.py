from dataclass_sqlalchemy_mixins.base.mixins import SqlAlchemyFilterConverterMixin, SqlAlchemyOrderConverterMixin
from sqlalchemy import select, func, inspect
from sqlalchemy.orm import InstrumentedAttribute

from sqlalchemy_query_manager.consts import classproperty


class QueryManager(SqlAlchemyFilterConverterMixin, SqlAlchemyOrderConverterMixin):
    def __init__(self, model, sessionmaker):
        self.ConverterConfig.model = model

        self.sessionmaker = sessionmaker

        self.fields = None

        self.filters = {}
        self._order_by = set()

        self.models_to_join = []

        self._binary_expressions = []
        self._unary_expressions = []

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
                    models=self.models_to_join
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

        return query

    def all(self):
        with self.sessionmaker() as session:
            result = session.execute(self.query)

            if not self.fields:
                result = result.scalars()
            return result.all()

    def first(self):
        with self.sessionmaker() as session:
            result = session.execute(self.query)

            if not self.fields:
                result = result.scalars()
            return result.first()

    def last(self):
        primary_key = inspect(self.ConverterConfig.model).primary_key[0].name
        primary_key_row = getattr(self.ConverterConfig.model, primary_key)

        with self.sessionmaker() as session:
            query = self.query.order_by(-primary_key_row)

            result = session.execute(query)

            if not self.fields:
                result = result.scalars()

            return result.first()

    def get(self, **kwargs):
        binary_expressions = self.get_binary_expressions(
            filters=kwargs
        )

        with self.sessionmaker() as session:
            obj = session.query(self.ConverterConfig.model).filter(*binary_expressions).first()

        return obj

    def where(self, **kwargs):
        self.filters = {
            **self.filters,
            **kwargs,
        }

        return self

    def order_by(self, *args):
        self._order_by.update(set(args))

        return self

    def count(self):
        with self.sessionmaker() as session:
            count = session.execute(select(func.count()).select_from(self.query)).scalar_one()
        return count


class AsyncQueryManager(QueryManager):
    async def first(self):
        async with self.sessionmaker() as session:
            result = await session.execute(self.query)

            if not self.fields:
                result = result.scalars()

            return result.first()

    async def last(self):
        primary_key = inspect(self.ConverterConfig.model).primary_key[0].name
        primary_key_row = getattr(self.ConverterConfig.model, primary_key)

        async with self.sessionmaker() as session:
            query = self.query.order_by(-primary_key_row)

            result = await session.execute(query)

            if not self.fields:
                result = result.scalars()

            return result.first()

    async def get(self, **kwargs):
        binary_expressions = self.get_binary_expressions(
            filters=kwargs
        )

        async with self.sessionmaker() as session:
            obj = (
                await session.execute(
                    select(self.ConverterConfig.model).where(*binary_expressions)
                )
            ).scalars().first()

        return obj

    async def all(self):
        async with self.sessionmaker() as session:
            result = await session.execute(self.query)

            if not self.fields:
                result = result.scalars()

            return result.all()

    async def count(self):
        async with self.sessionmaker() as session:
            count = (await session.execute(select(func.count()).select_from(self.query))).scalar_one()
        return count


class ModelQueryManager(QueryManager):
    @classproperty
    def query_manager(cls):
        return QueryManager(
            model=cls,
            sessionmaker=cls.sessionmaker,
        )


class AsyncModelQueryManager(ModelQueryManager):
    @classproperty
    def query_manager(cls):
        return AsyncQueryManager(
            model=cls,
            sessionmaker=cls.sessionmaker,
        )
