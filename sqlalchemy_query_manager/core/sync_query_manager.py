import typing

from dataclass_sqlalchemy_mixins.base.mixins import (
    SqlAlchemyFilterConverterMixin,
    SqlAlchemyOrderConverterMixin,
)
from sqlalchemy import delete, func, inspect, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import InstrumentedAttribute, Session, sessionmaker

from sqlalchemy_query_manager.core.helpers import AggregateFunc, E, Q, _format_sql_value
from sqlalchemy_query_manager.core.types import JoinConfig, JoinType
from sqlalchemy_query_manager.core.utils import get_session


class QueryManager(SqlAlchemyFilterConverterMixin, SqlAlchemyOrderConverterMixin):
    def __init__(self, model, session=None):
        super().__init__()

        self.ConverterConfig.model = model

        self.session: typing.Union[Session, AsyncSession, sessionmaker] = session

        self._to_commit = False

        if isinstance(self.session, sessionmaker):
            self._to_commit = True

        self.fields = None

        self._filters = {}
        self._order_by: typing.List = []

        self.models_to_join = []
        self.explicit_joins: typing.List[JoinConfig] = []

        self._limit = None
        self._offset = None

        self._binary_expressions = []
        self._unary_expressions = []

        self._distinct = None
        self._q_filters: typing.List[Q] = []
        self._select_related: typing.List[str] = []
        self._prefetch_related: typing.List[str] = []
        self._for_update = None
        self._session_is_explicit = False

    def _clone(self):
        """Create a copy of the current QueryManager"""
        new_manager = self.__class__(
            model=self.ConverterConfig.model, session=self.session
        )

        # Copy all mutable state
        new_manager._filters = self._filters.copy()
        new_manager._order_by = self._order_by.copy()
        new_manager._limit = self._limit
        new_manager._offset = self._offset
        new_manager.fields = self.fields.copy() if self.fields else None
        new_manager.models_to_join = self.models_to_join.copy()
        new_manager.explicit_joins = self.explicit_joins.copy()

        new_manager._to_commit = self._to_commit

        new_manager._distinct = self._distinct

        # Copy internal state
        new_manager._binary_expressions = self._binary_expressions.copy()
        new_manager._unary_expressions = self._unary_expressions.copy()
        new_manager._q_filters = self._q_filters.copy()
        new_manager._select_related = self._select_related.copy()
        new_manager._prefetch_related = self._prefetch_related.copy()
        new_manager._for_update = self._for_update.copy() if self._for_update else None
        new_manager._session_is_explicit = self._session_is_explicit

        return new_manager

    def join_models(
        self,
        query,
        join_configs: typing.List[JoinConfig],
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

        for join_config in join_configs:
            model = join_config.model
            if model != self.ConverterConfig.model and model not in joined_models:
                # Use relationship-based join when relationship_attr is set
                # (needed for contains_eager to work correctly)
                target = (
                    join_config.relationship_attr
                    if join_config.relationship_attr is not None
                    else model
                )

                if join_config.join_type == JoinType.INNER:
                    query = query.join(target)
                elif join_config.join_type == JoinType.LEFT:
                    query = query.outerjoin(target)
                elif join_config.join_type == JoinType.FULL:
                    query = query.outerjoin(target, full=True)
                else:
                    raise NotImplementedError

                joined_models.append(model)

        return query

    def inner_join(self, *relationships):
        """
        Specify relationships to be joined using INNER JOIN.

        Args:
            *relationships: Relationship paths like 'group', 'group__owner'

        Returns:
            QueryManager: New instance with join specifications

        Usage:
            Item.query_manager.inner_join('group').all()
            Item.query_manager.inner_join('group', 'group__owner').all()
            Item.query_manager.inner_join('group__owner').where(name='test').all()
        """
        new_manager = self._clone()

        for relationship in relationships:
            relationship = relationship.split("__")
            models, _ = self.get_foreign_key_path(
                relationship,
                to_return_column=False,
            )

            for model in models:
                new_manager.explicit_joins.append(
                    JoinConfig(
                        model=model,
                        join_type=JoinType.INNER,
                    )
                )

        return new_manager

    def join(self, *relationships):
        """
        Proxy to INNER JOIN.

        Args:
            *relationships: Relationship paths like 'group', 'group__owner'

        Returns:
            QueryManager: New instance with join specifications

        Usage:
            Item.query_manager.join('group').all()
            Item.query_manager.join('group', 'group__owner').all()
            Item.query_manager.join('group__owner').where(name='test').all()
        """

        return self.inner_join(*relationships)

    def left_join(self, *relationships):
        """
        Specify relationships to be joined using LEFT JOIN.

        Args:
            *relationships: Relationship paths like 'group', 'group__owner'

        Returns:
            QueryManager: New instance with join specifications

        Usage:
            Item.query_manager.left_join('group').all()
            Item.query_manager.left_join('group', 'group__owner').all()
            Item.query_manager.left_join('group__owner').where(name='test').all()
        """
        new_manager = self._clone()

        for relationship in relationships:
            relationship = relationship.split("__")
            models, _ = self.get_foreign_key_path(
                relationship,
                to_return_column=False,
            )

            for model in models:
                new_manager.explicit_joins.append(
                    JoinConfig(
                        model=model,
                        join_type=JoinType.LEFT,
                    )
                )

        return new_manager

    def full_join(self, *relationships):
        """
        Specify relationships to be joined using FULL JOIN.

        Args:
            *relationships: Relationship paths like 'group', 'group__owner'

        Returns:
            QueryManager: New instance with join specifications

        Usage:
            Item.query_manager.full_join('group').all()
            Item.query_manager.full_join('group', 'group__owner').all()
            Item.query_manager.full_join('group__owner').where(name='test').all()
        """
        new_manager = self._clone()

        for relationship in relationships:
            relationship = relationship.split("__")
            models, _ = self.get_foreign_key_path(
                relationship,
                to_return_column=False,
            )

            for model in models:
                new_manager.explicit_joins.append(
                    JoinConfig(
                        model=model,
                        join_type=JoinType.FULL,
                    )
                )

        return new_manager

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
                models, db_field = self.get_foreign_key_path(
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
        query_manager = self._clone()

        _fields = []
        for field in fields:
            if field == "*":
                models = [self.ConverterConfig.model]

                if query_manager.explicit_joins:
                    for explicit_join in query_manager.explicit_joins:
                        models.append(explicit_join.model)

                for model in models:
                    for column in model.__table__.columns:
                        _fields.append(column)
                    hybrids = [
                        getattr(model, name)
                        for name, attr in vars(model).items()
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

        query_manager.fields = _fields
        return query_manager

    def limit(self, limit):
        query_manager = self._clone()

        query_manager._limit = limit
        return query_manager

    def offset(self, offset):
        query_manager = self._clone()

        query_manager._offset = offset
        return query_manager

    def distinct(self):
        query_manager = self._clone()

        query_manager._distinct = True
        return query_manager

    def select_for_update(
        self,
        nowait: bool = False,
        skip_locked: bool = False,
        no_key: bool = False,
    ):
        """
        Lock selected rows until the end of the current transaction.

        Requires an explicit Session when the query is executed, either via a
        method's session= argument or with_session().
        """
        if nowait and skip_locked:
            raise ValueError("nowait and skip_locked cannot both be True.")

        query_manager = self._clone()
        query_manager._for_update = {
            "nowait": nowait,
            "skip_locked": skip_locked,
            "key_share": no_key,
        }
        return query_manager

    @property
    def binary_expressions(self):
        if not self._binary_expressions and (self._filters or self._q_filters):
            if self._filters:
                models_binary_expressions = self.get_models_binary_expressions(
                    filters=self._filters
                )

                for model_binary_expression in models_binary_expressions:
                    for model in model_binary_expression.get("models"):
                        self.models_to_join.append(JoinConfig(model=model))
                    self._binary_expressions.append(
                        model_binary_expression.get("binary_expression")
                    )

            for q in self._q_filters:
                expr, models = q.resolve(self)
                for model in models:
                    self.models_to_join.append(JoinConfig(model=model))
                self._binary_expressions.append(expr)

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
                for model in model_unary_expression.get("models"):
                    self.models_to_join.append(JoinConfig(model=model))
                unary_expression = model_unary_expression.get("unary_expression")

                if pos in e_pos:
                    func_to_apply = _order_by[pos].func
                    unary_expression = func_to_apply(unary_expression)

                self._unary_expressions.append(unary_expression)

        return self._unary_expressions

    @property
    def query(self):
        query = select(self.ConverterConfig.model)

        # Apply explicit joins
        if self.explicit_joins:
            query = self.join_models(
                query=query,
                join_configs=self.explicit_joins,
            )

        # Trigger binary_expressions computation (populates models_to_join)
        binary_exprs = self.binary_expressions

        # Build eager loading options AFTER models_to_join is populated
        # so conflicts between FK filters and select_related are detected
        eager_options = []
        if self._select_related or self._prefetch_related:
            eager_options = self._build_eager_options()

        # Apply FK filter joins (models_to_join may be updated by _build_eager_options)
        if binary_exprs:
            query = self.join_models(
                query=query,
                join_configs=self.models_to_join,
            )
            query = query.where(*binary_exprs)

        # Apply unary expressions
        if self.unary_expressions:
            query = self.join_models(query=query, join_configs=self.models_to_join)
            query = query.order_by(*self.unary_expressions)

        # Select fields
        if self.fields:
            query = query.with_only_columns(*self.fields)

        if self._offset is not None:
            query = query.offset(self._offset)

        if self._limit is not None:
            query = query.limit(self._limit)

        if self._distinct:
            query = query.distinct()

        if eager_options:
            query = query.options(*eager_options)

        if self._for_update is not None:
            query = query.with_for_update(**self._for_update)

        return query

    @get_session
    def all(self, session=None, expunge=True):
        result = session.execute(self.query)

        if not self.fields:
            result = result.scalars()
            if self._select_related:
                # joinedload/contains_eager can produce duplicate rows
                result = result.unique()

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
        """
        Return exactly one object matching the query.

        Raises:
            DoesNotExist: If no object matches.
            MultipleObjectsReturned: If more than one object matches.
        """
        from sqlalchemy_query_manager.core.exceptions import (
            DoesNotExist,
            MultipleObjectsReturned,
        )

        # Merge extra kwargs into filters while preserving _q_filters and all other state
        if kwargs:
            query_manager = self._clone()
            query_manager._filters = {**self._filters, **kwargs}
        else:
            query_manager = self

        # LIMIT 2 to efficiently detect MultipleObjectsReturned
        results = session.execute(query_manager.query.limit(2))
        if not query_manager.fields:
            results = results.scalars()
        results = results.all()

        model_name = self.ConverterConfig.model.__name__

        if not results:
            raise DoesNotExist(f"{model_name} matching query does not exist.")
        if len(results) > 1:
            raise MultipleObjectsReturned(f"get() returned more than one {model_name}.")

        result = results[0]
        if not query_manager.fields and expunge:
            session.expunge(result)

        return result

    def where(self, *args, **kwargs):
        query_manager = self._clone()

        query_manager._filters = {
            **self._filters,
            **kwargs,
        }

        for arg in args:
            if not isinstance(arg, Q):
                raise TypeError(
                    f"Positional arguments to where() must be Q objects, got {type(arg)}"
                )
        query_manager._q_filters = self._q_filters + list(args)

        return query_manager

    def _detect_dialect(self):
        """
        Try to detect the SQLAlchemy dialect from the current session or engine.
        Falls back to PostgreSQL as the most feature-rich dialect for debugging.
        """
        try:
            session = self.session
            if isinstance(session, sessionmaker):
                # SA 1.4: sessionmaker may have a bound engine via kw['bind']
                engine = session.kw.get("bind")
                if engine is not None:
                    return engine.dialect
            elif hasattr(session, "bind") and session.bind is not None:
                return session.bind.dialect
            elif hasattr(session, "get_bind"):
                return session.get_bind().dialect
        except Exception:
            pass

        from sqlalchemy.dialects import postgresql

        return postgresql.dialect()

    def get_sql_query(self, dialect=None) -> str:
        """
        Return the compiled SQL query as a string with literal values substituted.
        Useful for debugging and logging.

        Args:
            dialect: SQLAlchemy dialect instance to use for compilation.
                     If None, auto-detected from session/engine.
                     Falls back to PostgreSQL dialect if detection fails.

        Tries SQLAlchemy's literal_binds first (handles all standard types).
        Falls back to manual formatting for custom Python types (enum, datetime, etc.).

        Usage:
            print(Item.query_manager.where(name="foo", is_valid=True).get_sql_query())

            # Explicit dialect:
            from sqlalchemy.dialects import sqlite
            print(Item.query_manager.where(name="foo").get_sql_query(dialect=sqlite.dialect()))
        """
        if dialect is None:
            dialect = self._detect_dialect()

        try:
            compiled = self.query.compile(
                dialect=dialect,
                compile_kwargs={"literal_binds": True},
            )
            return str(compiled)
        except Exception:
            compiled = self.query.compile(dialect=dialect)
            sql = str(compiled)

            # Sort by key length descending to avoid partial replacements
            params = sorted(
                compiled.params.items(),
                key=lambda x: len(x[0]),
                reverse=True,
            )
            for key, value in params:
                sql = sql.replace(f"%({key})s", _format_sql_value(value))

            return sql

    def _resolve_relationship_path(self, path: str) -> typing.List[typing.Tuple]:
        """
        Resolve 'group__owner' → [(Item.group, Group), (Group.owner, Owner)]
        """
        parts = path.split("__")
        model = self.ConverterConfig.model
        segments = []

        for part in parts:
            rel_attr = getattr(model, part)
            target_model = rel_attr.property.mapper.class_
            segments.append((rel_attr, target_model))
            model = target_model

        return segments

    def _build_eager_options(self) -> typing.List:
        """
        Build SQLAlchemy eager loading options for select_related / prefetch_related.

        Uses a tree structure to avoid strategy conflicts when paths share prefixes,
        e.g. select_related('group') + prefetch_related('group__owner').

        - select_related  → joinedload (or contains_eager when already joined)
        - prefetch_related → selectinload
        """
        from sqlalchemy.orm import contains_eager, joinedload, selectinload

        already_joined = {jc.model for jc in self.models_to_join}

        strategy_map = {
            "joinedload": joinedload,
            "selectinload": selectinload,
            "contains_eager": contains_eager,
        }

        def get_strategy_name(rel_attr, target_model, default: str) -> str:
            if target_model in already_joined:
                # Upgrade JoinConfig to relationship-based join for contains_eager
                for jc in self.models_to_join:
                    if jc.model == target_model and jc.relationship_attr is None:
                        jc.relationship_attr = rel_attr
                return "contains_eager"
            return default

        # Build a tree: {attr_key: {"rel_attr": ..., "strategy": ..., "children": {...}}}
        # Select_related is inserted first (higher priority) — existing nodes are NOT overridden.
        roots: typing.Dict[str, typing.Any] = {}

        def insert_path(path: str, default_strategy: str) -> None:
            segments = self._resolve_relationship_path(path)
            node_dict = roots

            for rel_attr, target_model in segments:
                key = rel_attr.key
                if key not in node_dict:
                    node_dict[key] = {
                        "rel_attr": rel_attr,
                        "strategy": get_strategy_name(
                            rel_attr, target_model, default_strategy
                        ),
                        "children": {},
                    }
                # Descend regardless — children may differ
                node_dict = node_dict[key]["children"]

        for path in self._select_related:
            insert_path(path, "joinedload")

        for path in self._prefetch_related:
            insert_path(path, "selectinload")

        # Convert tree nodes to SQLAlchemy Load objects.
        # Nodes with children produce one option per leaf to preserve full chains.
        def build_options_from_node(
            node: typing.Dict, parent_load: typing.Any = None
        ) -> typing.List:
            fn = strategy_map[node["strategy"]]
            rel_attr = node["rel_attr"]

            this_load = (
                fn(rel_attr)
                if parent_load is None
                else getattr(parent_load, fn.__name__)(rel_attr)
            )

            if not node["children"]:
                return [this_load]

            result = []
            for child in node["children"].values():
                result.extend(build_options_from_node(child, this_load))
            return result

        options = []
        for root_node in roots.values():
            options.extend(build_options_from_node(root_node))

        return options

    def select_related(self, *paths: str) -> "QueryManager":
        """
        Eagerly load relationships using JOIN (joinedload).
        When the relationship is already JOINed for a FK filter,
        uses contains_eager to avoid duplicate JOINs.

        Usage:
            Item.query_manager.select_related('group').all()
            Item.query_manager.select_related('group__owner').all()
            Item.query_manager.where(group__name='foo').select_related('group').all()
        """
        query_manager = self._clone()
        query_manager._select_related = self._select_related + list(paths)
        return query_manager

    def prefetch_related(self, *paths: str) -> "QueryManager":
        """
        Eagerly load relationships using a separate SELECT IN query (selectinload).
        No JOIN conflicts — always safe to combine with FK filters.

        Usage:
            Item.query_manager.prefetch_related('group').all()
            Item.query_manager.prefetch_related('group__owner').all()
        """
        query_manager = self._clone()
        query_manager._prefetch_related = self._prefetch_related + list(paths)
        return query_manager

    def order_by(self, *args):
        query_manager = self._clone()

        # dict.fromkeys preserves insertion order and deduplicates
        combined = list(dict.fromkeys(self._order_by + list(args)))
        query_manager._order_by = combined

        return query_manager

    @get_session
    def count(self, session=None, **kwargs):
        count = session.execute(
            select(func.count()).select_from(self.query)
        ).scalar_one()
        return count

    @get_session
    def aggregate(self, session=None, expunge=True, **kwargs):
        """
        Execute aggregate functions and return results as a dict.

        Args:
            session: Database session
            **kwargs: Mapping of result key to AggregateFunc instance

        Returns:
            Dict of aggregated values

        Usage:
            Item.query_manager.aggregate(total=Sum('number'), avg=Avg('number'))
            Item.query_manager.where(is_valid=True).aggregate(count=Count('id'))
        """
        columns = [
            agg_func.resolve(self).label(name)
            for name, agg_func in kwargs.items()
            if isinstance(agg_func, AggregateFunc)
        ]

        query = select(*columns)

        if self.binary_expressions:
            query = self.join_models(query=query, join_configs=self.models_to_join)
            query = query.where(*self.binary_expressions)

        result = session.execute(query).mappings().one()
        return dict(result)

    @get_session
    def raw(self, sql: str, session=None, expunge=True, **params):
        """
        Execute a raw SQL query and return results as a list of dict-like objects.

        Args:
            sql: Raw SQL string with named placeholders (e.g. WHERE id = :id)
            **params: Named parameters to bind safely into the query

        Returns:
            List of RowMapping objects (accessible by column name)

        Usage:
            Item.query_manager.raw("SELECT * FROM item WHERE id = :id", id=1)
            Item.query_manager.raw(
                "SELECT group_id, COUNT(*) as cnt FROM item GROUP BY group_id HAVING COUNT(*) > :min",
                min=2,
            )
        """
        result = session.execute(text(sql), params)
        return result.mappings().all()

    def with_session(self, session):
        query_manager = self._clone()

        query_manager.session = session
        query_manager._session_is_explicit = True
        return query_manager

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
        from sqlalchemy_query_manager.core.exceptions import DoesNotExist

        # Try to get existing instance
        try:
            existing = self.get(session=session, **kwargs)
            if expunge:
                session.expunge(existing)
            return existing, False
        except DoesNotExist:
            pass

        # Create new instance with defaults
        create_kwargs = kwargs.copy()
        if defaults:
            create_kwargs.update(defaults)

        new_obj = self.create(session=session, **create_kwargs)
        return new_obj, True

    @get_session
    def update(self, session=None, expunge=True, **kwargs):
        """
        Update records matching the current filters and return updated objects.

        Uses ``UPDATE ... RETURNING <primary_key>`` to get the primary keys of
        affected rows directly from the UPDATE (this is correct even if the
        UPDATE itself changes the primary key) and then fetches the fresh ORM
        objects via a second SELECT by those PKs. This avoids re-using the
        original WHERE clause, which would silently miss rows whenever the
        UPDATE changes a column that the filter references, and also avoids
        ``IndexError`` when no rows matched.

        Args:
            session: Database session
            expunge: Whether to expunge the objects from session after update
            **kwargs: Field values to update

        Returns:
            ``[]`` when no rows were updated, a single model instance when
            exactly one row was updated, otherwise a list of model instances.

        Raises:
            ValueError: If no filters are set (to prevent accidental full table updates)
        """
        if not self._filters and not self._q_filters:
            raise ValueError(
                "Cannot update without filters. Use where() to specify criteria."
            )

        model = self.ConverterConfig.model
        pk_cols = inspect(model).primary_key
        if len(pk_cols) != 1:
            return self._update_legacy_path(session=session, expunge=expunge, **kwargs)
        pk_col = pk_cols[0]

        update_query = update(model)

        if self.binary_expressions:
            update_query = update_query.where(*self.binary_expressions)

        update_query = update_query.values(**kwargs).returning(pk_col)

        result = session.execute(
            update_query, execution_options={"synchronize_session": False}
        )
        returned_pks = [row[0] for row in result]

        # Flush so the UPDATE is visible to the follow-up SELECT under the
        # same transaction. We commit (or not) only after the read below.
        session.flush()

        if not returned_pks:
            if self._to_commit:
                session.commit()
            return []

        select_stmt = select(model).where(pk_col.in_(returned_pks))
        updated_objects = list(session.execute(select_stmt).scalars().all())

        # Order matters: expunge while rows are still attached and loaded, so
        # a later commit() (with the default expire_on_commit=True) cannot
        # expire our already-detached objects and break attribute access.
        if expunge:
            session.expunge_all()

        if self._to_commit:
            session.commit()

        if not updated_objects:
            return []
        return updated_objects if len(updated_objects) > 1 else updated_objects[0]

    def _update_legacy_path(self, session, expunge=True, **kwargs):
        """Composite-PK fallback: original UPDATE + follow-up SELECT path."""
        update_query = update(self.ConverterConfig.model)

        if self.binary_expressions:
            update_query = update_query.where(*self.binary_expressions)

        update_query = update_query.values(**kwargs)

        session.execute(update_query)
        session.flush()

        updated_objects = self.all(session=session)

        if expunge:
            session.expunge_all()
        if self._to_commit:
            session.commit()

        if not updated_objects:
            return []
        return updated_objects if len(updated_objects) > 1 else updated_objects[0]

    @get_session
    def update_raw(self, session=None, **kwargs):
        """
        Update records matching the current filters without returning objects.

        This method provides better performance than update() when you don't need
        the updated objects returned, as it avoids the additional query to fetch them.

        Args:
            session: Database session (optional, will use self.session if not provided)
            **kwargs: Field values to update

        Returns:
            Number of affected rows

        Raises:
            ValueError: If no filters are set (to prevent accidental full table updates)
        """

        # Remove expunge parameter injected by decorator since we don't use it
        kwargs.pop("expunge", None)

        if not self._filters and not self._q_filters:
            raise ValueError(
                "Cannot update without filters. Use where() to specify criteria."
            )

        # Build update query with current filters
        update_query = update(self.ConverterConfig.model)

        if self.binary_expressions:
            update_query = update_query.where(*self.binary_expressions)

        update_query = update_query.values(**kwargs)

        result = session.execute(update_query)

        if self._to_commit:
            session.commit()
        else:
            session.flush()

        return result.rowcount

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
        from sqlalchemy_query_manager.core.exceptions import DoesNotExist

        # Try to get existing instance
        try:
            existing = self.get(session=session, **kwargs)
        except DoesNotExist:
            existing = None

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

        new_obj = self.create(session=session, **create_kwargs)
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
                query_manager = query_manager.where(**filter_kwargs)
                updated_objs = query_manager.update(session=session, **update_kwargs)
                if isinstance(updated_objs, list):
                    updated_objects.extend(updated_objs)
                else:
                    updated_objects.append(updated_objs)

        if expunge:
            for obj in updated_objects:
                session.expunge(obj)

        return updated_objects

    @get_session
    def delete(self, session=None, expunge=True):
        """
        Delete records matching the current filters.

        Args:
            session: Database session

        Returns:
            Number of deleted rows

        Raises:
            ValueError: If no filters are set (to prevent accidental full table deletions)
        """
        if not self._filters and not self._q_filters:
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
    def exists(self, session=None, expunge=True, **kwargs):
        """
        Check if any records exist matching the criteria.

        Args:
            session: Database session
            **kwargs: Additional filter criteria

        Returns:
            Boolean indicating if records exist
        """
        if kwargs:
            # Clone preserves _q_filters and all other state
            query_manager = self._clone()
            query_manager._filters = {**self._filters, **kwargs}
            return query_manager.exists(session=session)

        # Use current filters
        query = select(self.ConverterConfig.model).where(*self.binary_expressions)
        exists_query = select(query.exists())

        return session.execute(exists_query).scalar()

    def clone(self):
        """
        Create a full copy of the current QueryManager with all filters,
        joins, ordering, eager loading and other settings preserved.

        Returns:
            New QueryManager instance with identical state
        """
        return self._clone()
