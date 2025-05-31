import typing

from dataclass_sqlalchemy_mixins.base.mixins import (
    SqlAlchemyFilterConverterMixin,
    SqlAlchemyOrderConverterMixin,
)
from sqlalchemy import delete, func, inspect, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import DeclarativeMeta, InstrumentedAttribute, Session, sessionmaker

from sqlalchemy_query_manager.consts import classproperty
from sqlalchemy_query_manager.core.helpers import E
from sqlalchemy_query_manager.core.utils import get_async_session, get_session


class QueryManager(SqlAlchemyFilterConverterMixin, SqlAlchemyOrderConverterMixin):
    def __init__(self, model, session=None):
        self.ConverterConfig.model = model

        self.session: typing.Union[Session, AsyncSession, sessionmaker] = session

        self._to_commit = False

        if isinstance(self.session, sessionmaker):
            self._to_commit = True

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
            if field == "*":
                for column in self.ConverterConfig.model.__table__.columns:
                    _fields.append(column)
                hybrids = [
                    getattr(self.ConverterConfig.model, name)
                    for name, attr in vars(self.ConverterConfig.model).items()
                    if isinstance(attr, hybrid_property)
                ]
                _fields.extend(hybrids)
                continue

            if isinstance(field, InstrumentedAttribute):
                pass
            elif isinstance(field, str):
                field = self.get_model_field(field)
            else:
                raise NotImplementedError(
                    "Should be either InstrumentedAttribute class or str"
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
                self.models_to_join.extend(model_binary_expression.get("models"))
                self._binary_expressions.append(
                    model_binary_expression.get("binary_expression")
                )
        return self._binary_expressions

    @property
    def unary_expressions(self):
        if not self._unary_expressions and self._order_by:
            _order_by = list(self._order_by)

            to_order_by = []
            e_pos = []
            for pos, order_by in enumerate(_order_by):
                if isinstance(order_by, E):
                    to_order_by.append(order_by.field_name)
                    e_pos.append(pos)
                else:
                    to_order_by.append(order_by)

            models_unary_expressions = self.get_models_unary_expressions(
                order_by=to_order_by
            )

            for pos, model_unary_expression in enumerate(models_unary_expressions):
                self.models_to_join.extend(model_unary_expression.get("models"))

                unary_expression = model_unary_expression.get("unary_expression")

                if pos in e_pos:
                    func_to_apply = _order_by[pos].func
                    unary_expression = func_to_apply(unary_expression)

                self._unary_expressions.append(unary_expression)

        return self._unary_expressions

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
                query = self.join_models(query=query, models=self.models_to_join)
            query = query.order_by(*self.unary_expressions)

        if self._offset:
            query = query.offset(self._offset)

        if self._limit:
            query = query.limit(self._limit)

        return query

    @get_session
    def all(self, session=None, expunge=True):
        result = session.execute(self.query)

        if not self.fields:
            result = result.scalars()

        result = result.all()

        if result and expunge:
            session.expunge_all()

        return result

    @get_session
    def first(self, session=None, expunge=True):
        result = session.execute(self.query)

        if not self.fields:
            result = result.scalars()

        result = result.first()

        if self.fields:
            pass
        elif result and expunge:
            session.expunge(result)

        return result

    @get_session
    def last(self, session=None, expunge=True):
        primary_key = inspect(self.ConverterConfig.model).primary_key[0].name
        primary_key_row = getattr(self.ConverterConfig.model, primary_key)

        query = self.query.order_by(-primary_key_row)

        result = session.execute(query)

        if not self.fields:
            result = result.scalars()

        result = result.first()

        if self.fields:
            pass
        elif result and expunge:
            session.expunge(result)

        return result

    @get_session
    def get(self, session=None, expunge=True, **kwargs):
        binary_expressions = self.get_binary_expressions(filters=kwargs)

        result = (
            session.query(self.ConverterConfig.model)
            .filter(*binary_expressions)
            .first()
        )

        if result and expunge:
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

    @get_session
    def count(self, session=None, **kwargs):
        count = session.execute(
            select(func.count()).select_from(self.query)
        ).scalar_one()
        return count

    def with_session(self, session):
        self.session = session
        return self

    # ENHANCED CREATE METHODS
    @get_session
    def create(self, session=None, expunge=True, **kwargs):
        """
        Create a new instance of the model with the provided kwargs.

        Args:
            session: Database session (optional, will use self.session if not provided)
            expunge: Whether to expunge the object from session after creation
            **kwargs: Field values for the new instance

        Returns:
            The created model instance

        Raises:
            ValueError: If required fields are missing
            IntegrityError: If database constraints are violated
        """
        new_obj = self.ConverterConfig.model(**kwargs)
        session.add(new_obj)

        if self._to_commit:
            session.commit()
        else:
            session.flush()

        session.refresh(new_obj)

        if expunge:
            session.expunge(new_obj)

        return new_obj

    @get_session
    def bulk_create(self, data: typing.List[typing.Dict], session=None, expunge=True):
        """
        Create multiple instances efficiently using bulk operations.

        Args:
            session: Database session
            data: List of dictionaries containing field values
            expunge: Whether to expunge objects from session

        Returns:
            List of created instances
        """
        if not data:
            return []

        objects = [self.ConverterConfig.model(**item) for item in data]
        session.add_all(objects)

        if self._to_commit:
            session.commit()
        else:
            session.flush()

        # Refresh all objects to get their IDs and computed fields
        for obj in objects:
            session.refresh(obj)

        if expunge:
            for obj in objects:
                session.expunge(obj)

        return objects

    @get_session
    def get_or_create(self, session=None, expunge=True, defaults=None, **kwargs):
        """
        Get an existing instance or create a new one if it doesn't exist.

        Args:
            session: Database session
            expunge: Whether to expunge the object from session
            defaults: Default values to use when creating (if needed)
            **kwargs: Filter criteria for finding existing instance

        Returns:
            Tuple of (instance, created) where created is True if instance was created
        """
        # Try to get existing instance
        existing = self.get(session=session, expunge=False, **kwargs)

        if existing:
            if expunge:
                session.expunge(existing)
            return existing, False

        # Create new instance with defaults
        create_kwargs = kwargs.copy()
        if defaults:
            create_kwargs.update(defaults)

        new_obj = self.create(session=session, expunge=expunge, **create_kwargs)
        return new_obj, True

    @get_session
    def update(self, session=None, expunge=True, **kwargs):
        """
        Update records matching the current filters and return updated objects.

        Args:
            session: Database session
            expunge: Whether to expunge the objects from session after update
            **kwargs: Field values to update

        Returns:
            List of updated model instances

        Raises:
            ValueError: If no filters are set (to prevent accidental full table updates)
        """
        if not self.filters:
            raise ValueError(
                "Cannot update without filters. Use where() to specify criteria."
            )

        # Build update query with current filters
        update_query = update(self.ConverterConfig.model)

        if self.binary_expressions:
            update_query = update_query.where(*self.binary_expressions)

        update_query = update_query.values(**kwargs)

        update_query_no_returning = update_query.values(**kwargs)

        session.execute(update_query_no_returning)

        if self._to_commit:
            session.commit()
        else:
            session.flush()

        # Then fetch the updated objects
        updated_objects = self.all(session=session)
        return updated_objects if len(updated_objects) > 1 else updated_objects[0]

    @get_session
    def update_or_create(self, session=None, expunge=True, defaults=None, **kwargs):
        """
        Update an existing instance or create a new one if it doesn't exist.

        Args:
            session: Database session
            expunge: Whether to expunge the object from session
            defaults: Default values to use when creating or updating
            **kwargs: Filter criteria for finding existing instance

        Returns:
            Tuple of (instance, created) where created is True if instance was created
        """
        # Try to get existing instance
        existing = self.get(session=session, expunge=False, **kwargs)

        if existing:
            # Update existing instance
            if defaults:
                for key, value in defaults.items():
                    if hasattr(existing, key):
                        setattr(existing, key, value)

            if self._to_commit:
                session.commit()
            else:
                session.flush()
                session.refresh(existing)

            if expunge:
                session.expunge(existing)

            return existing, False

        # Create new instance
        create_kwargs = kwargs.copy()
        if defaults:
            create_kwargs.update(defaults)

        new_obj = self.create(session=session, expunge=expunge, **create_kwargs)
        return new_obj, True

    @get_session
    def bulk_update(
        self,
        data: typing.List[typing.Dict],
        session=None,
        key_fields: typing.List[str] = None,
        expunge=True,
    ):
        """
        Update multiple records efficiently and return updated objects.

        Args:
            session: Database session
            data: List of dictionaries containing field values and identifiers
            key_fields: Fields to use for matching existing records (defaults to primary key)
            expunge: Whether to expunge the objects from session after update

        Returns:
            List of updated model instances
        """
        if not data:
            return []

        if key_fields is None:
            # Use primary key as default
            primary_keys = [
                pk.name for pk in inspect(self.ConverterConfig.model).primary_key
            ]
            key_fields = primary_keys

        updated_objects = []

        for item in data:
            # Extract key fields for filtering
            filter_kwargs = {key: item[key] for key in key_fields if key in item}
            update_kwargs = {k: v for k, v in item.items() if k not in key_fields}

            if update_kwargs and filter_kwargs:
                # Create a new query manager instance for each update
                query_manager = self.__class__(self.ConverterConfig.model, session)
                query_manager.where(**filter_kwargs)
                updated_objs = query_manager.update(
                    session=session, expunge=False, **update_kwargs
                )
                updated_objects.extend(updated_objs)

        if expunge:
            for obj in updated_objects:
                session.expunge(obj)

        return updated_objects

    @get_session
    def delete(self, session=None):
        """
        Delete records matching the current filters.

        Args:
            session: Database session

        Returns:
            Number of deleted rows

        Raises:
            ValueError: If no filters are set (to prevent accidental full table deletions)
        """
        if not self.filters:
            raise ValueError(
                "Cannot delete without filters. Use where() to specify criteria."
            )

        # Build delete query with current filters
        delete_query = delete(self.ConverterConfig.model)

        if self.binary_expressions:
            delete_query = delete_query.where(*self.binary_expressions)

        result = session.execute(delete_query)

        if self._to_commit:
            session.commit()
        else:
            session.flush()

        return result.rowcount

    @get_session
    def exists(self, session=None, **kwargs):
        """
        Check if any records exist matching the criteria.

        Args:
            session: Database session
            **kwargs: Additional filter criteria

        Returns:
            Boolean indicating if records exist
        """
        if kwargs:
            # Create new query manager with additional filters
            query_manager = self.__class__(self.ConverterConfig.model, session)
            query_manager.filters = {**self.filters, **kwargs}
            return query_manager.exists(session=session)

        # Use current filters
        query = select(self.ConverterConfig.model).where(*self.binary_expressions)
        exists_query = select(query.exists())

        return session.execute(exists_query).scalar()

    def clone(self):
        """
        Create a copy of the current QueryManager with the same filters and settings.

        Returns:
            New QueryManager instance
        """
        new_manager = self.__class__(self.ConverterConfig.model, self.session)
        new_manager.filters = self.filters.copy()
        new_manager._order_by = self._order_by.copy()
        new_manager._limit = self._limit
        new_manager._offset = self._offset

        if self.fields:
            new_manager.fields = self.fields.copy()

        return new_manager


class AsyncQueryManager(QueryManager):

    @get_async_session
    async def first(self, session=None):
        result = await session.execute(self.query)

        if not self.fields:
            result = result.scalars()
        return result.first()

    @get_async_session
    async def last(self, session=None):
        primary_key = inspect(self.ConverterConfig.model).primary_key[0].name
        primary_key_row = getattr(self.ConverterConfig.model, primary_key)

        query = self.query.order_by(-primary_key_row)

        result = await session.execute(query)

        if not self.fields:
            result = result.scalars()
        return result.first()

    @get_async_session
    async def get(self, session=None, **kwargs):
        binary_expressions = self.get_binary_expressions(filters=kwargs)

        result = (
            (
                await session.execute(
                    select(self.ConverterConfig.model).where(*binary_expressions)
                )
            )
            .scalars()
            .first()
        )
        return result

    @get_async_session
    async def all(self, session=None):
        result = await session.execute(self.query)

        if not self.fields:
            result = result.scalars()

        return result.all()

    @get_async_session
    async def count(self, session=None):
        count = (
            await session.execute(select(func.count()).select_from(self.query))
        ).scalar_one()
        return count

    @get_async_session
    async def create(self, session=None, **kwargs):
        """Async version of create method."""
        new_obj = self.ConverterConfig.model(**kwargs)
        session.add(new_obj)

        if isinstance(self.session, sessionmaker):
            await session.commit()
        else:
            await session.flush()

        await session.refresh(new_obj)
        return new_obj

    @get_async_session
    async def bulk_create(
        self,
        data: typing.List[typing.Dict],
        session=None,
    ):
        """Async version of bulk_create method."""
        if not data:
            return []

        objects = [self.ConverterConfig.model(**item) for item in data]
        session.add_all(objects)

        if isinstance(self.session, sessionmaker):
            await session.commit()
        else:
            await session.flush()

        for obj in objects:
            await session.refresh(obj)

        return objects

    @get_async_session
    async def get_or_create(self, session=None, defaults=None, **kwargs):
        """Async version of get_or_create method."""
        existing = await self.get(session=session, **kwargs)

        if existing:
            return existing, False

        create_kwargs = kwargs.copy()
        if defaults:
            create_kwargs.update(defaults)

        new_obj = await self.create(session=session, **create_kwargs)
        return new_obj, True

    @get_async_session
    async def update(self, session=None, expunge=True, **kwargs):
        """Async version of update method that returns updated objects."""
        if not self.filters:
            raise ValueError(
                "Cannot update without filters. Use where() to specify criteria."
            )

        # Build update query with current filters
        update_query = update(self.ConverterConfig.model)

        if self.binary_expressions:
            update_query = update_query.where(*self.binary_expressions)

        update_query = update_query.values(**kwargs)

        await session.execute(update_query)

        if isinstance(self.session, sessionmaker):
            await session.commit()
        else:
            await session.flush()

        # Then fetch the updated objects
        updated_objects = await self.all(session=session)
        return updated_objects if len(updated_objects) > 1 else updated_objects[0]

    @get_async_session
    async def update_raw(self, session=None, **kwargs):
        """
        Async version of update_raw method for better performance.

        Args:
            session: Database session
            **kwargs: Field values to update

        Returns:
            Number of affected rows
        """
        if not self.filters:
            raise ValueError(
                "Cannot update without filters. Use where() to specify criteria."
            )

        update_query = update(self.ConverterConfig.model)

        if self.binary_expressions:
            update_query = update_query.where(*self.binary_expressions)

        update_query = update_query.values(**kwargs)

        result = await session.execute(update_query)

        if isinstance(self.session, sessionmaker):
            await session.commit()
        else:
            await session.flush()

        return result.rowcount

    @get_async_session
    async def update_or_create(self, session=None, defaults=None, **kwargs):
        """Async version of update_or_create method."""
        existing = await self.get(session=session, **kwargs)

        if existing:
            if defaults:
                for key, value in defaults.items():
                    if hasattr(existing, key):
                        setattr(existing, key, value)

            if isinstance(self.session, sessionmaker):
                await session.commit()
            else:
                await session.flush()
                await session.refresh(existing)

            return existing, False

        create_kwargs = kwargs.copy()
        if defaults:
            create_kwargs.update(defaults)

        new_obj = await self.create(session=session, **create_kwargs)
        return new_obj, True

    @get_async_session
    async def bulk_update(
        self,
        data: typing.List[typing.Dict],
        session=None,
        key_fields: typing.List[str] = None,
        expunge=True,
    ):
        """Async version of bulk_update method that returns updated objects."""
        if not data:
            return []

        if key_fields is None:
            primary_keys = [
                pk.name for pk in inspect(self.ConverterConfig.model).primary_key
            ]
            key_fields = primary_keys

        updated_objects = []

        for item in data:
            filter_kwargs = {key: item[key] for key in key_fields if key in item}
            update_kwargs = {k: v for k, v in item.items() if k not in key_fields}

            if update_kwargs and filter_kwargs:
                query_manager = self.__class__(self.ConverterConfig.model, session)
                query_manager.where(**filter_kwargs)
                updated_objs = await query_manager.update(
                    session=session, expunge=False, **update_kwargs
                )
                updated_objects.extend(updated_objs)

        return updated_objects

    @get_async_session
    async def delete(self, session=None):
        """Async version of delete method."""
        if not self.filters:
            raise ValueError(
                "Cannot delete without filters. Use where() to specify criteria."
            )

        delete_query = delete(self.ConverterConfig.model)

        if self.binary_expressions:
            delete_query = delete_query.where(*self.binary_expressions)

        result = await session.execute(delete_query)

        if isinstance(self.session, sessionmaker):
            await session.commit()
        else:
            await session.flush()

        return result.rowcount

    @get_async_session
    async def exists(self, session=None, **kwargs):
        """Async version of exists method."""
        if kwargs:
            query_manager = self.__class__(self.ConverterConfig.model, session)
            query_manager.filters = {**self.filters, **kwargs}
            return await query_manager.exists(session=session)

        query = select(self.ConverterConfig.model).where(*self.binary_expressions)
        exists_query = select(query.exists())

        return (await session.execute(exists_query)).scalar()


class BaseModelQueryManagerMixin:
    class QueryManagerConfig:
        session = None

    def as_dict(self) -> typing.Dict[str, str]:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}  # type: ignore


class ModelQueryManagerMixin(BaseModelQueryManagerMixin):
    @classproperty
    def query_manager(cls):
        return QueryManager(
            model=cls,
            session=getattr(cls.QueryManagerConfig, "session", None),
        )


class AsyncModelQueryManagerMixin(BaseModelQueryManagerMixin):
    @classproperty
    def query_manager(cls):
        return AsyncQueryManager(
            model=cls,
            session=getattr(cls.QueryManagerConfig, "session", None),
        )
