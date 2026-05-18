import typing

from sqlalchemy import delete, func, inspect, select, text, update
from sqlalchemy.orm import sessionmaker

from sqlalchemy_query_manager.core.helpers import AggregateFunc
from sqlalchemy_query_manager.core.sync_query_manager import QueryManager
from sqlalchemy_query_manager.core.utils import get_async_session


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
        """Async version of get — raises DoesNotExist or MultipleObjectsReturned."""
        from sqlalchemy_query_manager.core.exceptions import (
            DoesNotExist,
            MultipleObjectsReturned,
        )

        if kwargs:
            query_manager = self._clone()
            query_manager._filters = {**self._filters, **kwargs}
        else:
            query_manager = self

        results = (await session.execute(query_manager.query.limit(2))).scalars().all()

        model_name = self.ConverterConfig.model.__name__

        if not results:
            raise DoesNotExist(f"{model_name} matching query does not exist.")
        if len(results) > 1:
            raise MultipleObjectsReturned(f"get() returned more than one {model_name}.")

        return results[0]

    @get_async_session
    async def all(self, session=None):
        result = await session.execute(self.query)

        if not self.fields:
            result = result.scalars()
            if self._select_related:
                result = result.unique()

        return result.all()

    @get_async_session
    async def count(self, session=None):
        count = (
            await session.execute(select(func.count()).select_from(self.query))
        ).scalar_one()
        return count

    @get_async_session
    async def aggregate(self, session=None, **kwargs):
        """Async version of aggregate method."""
        columns = [
            agg_func.resolve(self).label(name)
            for name, agg_func in kwargs.items()
            if isinstance(agg_func, AggregateFunc)
        ]

        query = select(*columns)

        if self.binary_expressions:
            query = self.join_models(query=query, join_configs=self.models_to_join)
            query = query.where(*self.binary_expressions)

        result = (await session.execute(query)).mappings().one()
        return dict(result)

    @get_async_session
    async def raw(self, sql: str, session=None, **params):
        """Async version of raw method."""
        result = await session.execute(text(sql), params)
        return result.mappings().all()

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
        from sqlalchemy_query_manager.core.exceptions import DoesNotExist

        try:
            existing = await self.get(session=session, **kwargs)
            return existing, False
        except DoesNotExist:
            pass

        create_kwargs = kwargs.copy()
        if defaults:
            create_kwargs.update(defaults)

        new_obj = await self.create(session=session, **create_kwargs)
        return new_obj, True

    @get_async_session
    async def update(self, session=None, expunge=True, **kwargs):
        """Async version of update method that returns updated objects.

        Uses ``UPDATE ... RETURNING <primary_key>`` to get the primary keys of
        affected rows directly from the UPDATE (this is correct even if the
        UPDATE itself changes the primary key) and then fetches the fresh ORM
        objects via a second SELECT by those PKs. This avoids re-using the
        original WHERE clause, which would silently miss rows whenever the
        UPDATE changes a column that the filter references, and also avoids
        ``IndexError`` when no rows matched.
        """
        if not self._filters and not self._q_filters:
            raise ValueError(
                "Cannot update without filters. Use where() to specify criteria."
            )

        model = self.ConverterConfig.model
        pk_cols = inspect(model).primary_key
        if len(pk_cols) != 1:
            # Composite PK is uncommon; fall back to legacy path (still
            # vulnerable to the filter-on-mutated-column issue, but we don't
            # have a single-column key to RETURNING here).
            return await self._update_legacy_path(
                session=session, expunge=expunge, **kwargs
            )
        pk_col = pk_cols[0]

        update_query = update(model)

        if self.binary_expressions:
            update_query = update_query.where(*self.binary_expressions)

        update_query = update_query.values(**kwargs).returning(pk_col)

        result = await session.execute(
            update_query, execution_options={"synchronize_session": False}
        )
        returned_pks = [row[0] for row in result]

        if isinstance(self.session, sessionmaker):
            await session.commit()
        else:
            await session.flush()

        if not returned_pks:
            return []

        select_stmt = select(model).where(pk_col.in_(returned_pks))
        updated_objects = (await session.execute(select_stmt)).scalars().all()

        return updated_objects if len(updated_objects) > 1 else updated_objects[0]

    async def _update_legacy_path(self, session, expunge=True, **kwargs):
        """Composite-PK fallback: original UPDATE + follow-up SELECT path."""
        update_query = update(self.ConverterConfig.model)

        if self.binary_expressions:
            update_query = update_query.where(*self.binary_expressions)

        update_query = update_query.values(**kwargs)

        await session.execute(update_query)

        if isinstance(self.session, sessionmaker):
            await session.commit()
        else:
            await session.flush()

        updated_objects = await self.all(session=session)
        if not updated_objects:
            return []
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
        # Remove expunge parameter injected by decorator since we don't use it
        kwargs.pop("expunge", None)

        if not self._filters and not self._q_filters:
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
        from sqlalchemy_query_manager.core.exceptions import DoesNotExist

        try:
            existing = await self.get(session=session, **kwargs)
        except DoesNotExist:
            existing = None

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
                query_manager = query_manager.where(**filter_kwargs)
                updated_objs = await query_manager.update(
                    session=session, expunge=False, **update_kwargs
                )
                if isinstance(updated_objs, list):
                    updated_objects.extend(updated_objs)
                else:
                    updated_objects.append(updated_objs)

        return updated_objects

    @get_async_session
    async def delete(self, session=None, synchronize_session=True):
        """Async version of delete method."""
        if not self._filters and not self._q_filters:
            raise ValueError(
                "Cannot delete without filters. Use where() to specify criteria."
            )

        delete_query = delete(self.ConverterConfig.model)

        if self.binary_expressions:
            delete_query = delete_query.where(*self.binary_expressions)

        # delete_query.compile(compile_kwargs={'literal_binds': True})
        result = await session.execute(
            delete_query, execution_options={"synchronize_session": False}
        )

        if isinstance(self.session, sessionmaker):
            await session.commit()
        else:
            await session.flush()

        return result.rowcount

    @get_async_session
    async def exists(self, session=None, **kwargs):
        """Async version of exists method."""
        if kwargs:
            query_manager = self._clone()
            query_manager._filters = {**self._filters, **kwargs}
            return await query_manager.exists(session=session)

        query = select(self.ConverterConfig.model).where(*self.binary_expressions)
        exists_query = select(query.exists())

        return (await session.execute(exists_query)).scalar()
