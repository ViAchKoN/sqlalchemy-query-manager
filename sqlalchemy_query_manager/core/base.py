from dataclass_sqlalchemy_mixins.base.mixins import SqlAlchemyFilterConverterMixin
from sqlalchemy import select
from sqlalchemy.orm import InstrumentedAttribute

from sqlalchemy_query_manager.consts import classproperty


class QueryManager(SqlAlchemyFilterConverterMixin):
    def __init__(self, model, sessionmaker, filters=None):
        self.ConverterConfig.model = model

        self.sessionmaker = sessionmaker

        self.fields = None

        self.filters = filters
        self._binary_expressions = None

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
        if self._binary_expressions is None and self.filters:
            self._binary_expressions = self.get_binary_expressions(
                filters=self.filters
            )
        return self._binary_expressions

    @property
    def query(self):
        query = select(self.ConverterConfig.model)

        if self.fields:
            query = select(*self.fields)

        if self.binary_expressions:
            query = query.where(*self.binary_expressions)

        return query

    def all(self):
        with self.sessionmaker() as session:
            result = session.execute(self.query)

            if not self.fields:
                result = result.scalars()
            return result.all()

    def first(self):
        with self.sessionmaker() as session:
            return session.execute(self.query).first()

    def get(self, **kwargs):
        binary_expressions = self.get_binary_expressions(
            filters=kwargs
        )

        with self.sessionmaker() as session:
            obj = session.query(self.ConverterConfig.model).filter(*binary_expressions).first()

        return obj

    def where(self, **kwargs):
        self.filters = kwargs

        return self


class AsyncQueryManager(QueryManager):
    async def first(self):
        async with self.sessionmaker() as session:
            return (await session.execute(self.query)).first()

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
