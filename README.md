# SQLAlchemy Query Manager

[![PyPI version](https://img.shields.io/pypi/v/sqlalchemy-query-manager.svg)](https://pypi.org/project/sqlalchemy-query-manager/)
[![Python versions](https://img.shields.io/pypi/pyversions/sqlalchemy-query-manager.svg)](https://pypi.org/project/sqlalchemy-query-manager/)
[![Downloads](https://static.pepy.tech/badge/sqlalchemy-query-manager/month)](https://pepy.tech/project/sqlalchemy-query-manager)
[![test](https://github.com/ViAchKoN/dataclass-sqlalchemy-mixins/workflows/Test/badge.svg?query=branch%3Amaster+event%3Apush)](https://github.com/ViAchKoN/dataclass-sqlalchemy-mixins/actions?query=branch%3Amaster+event%3Apush+workflow%3ATest++)

Django-style ORM interface for SQLAlchemy — `Q` filters, eager loading, async support, and zero session boilerplate.

Built on top of [dataclass-sqlalchemy-mixins](https://github.com/ViAchKoN/dataclass-sqlalchemy-mixins).
___
### Package requirements
- `python >= 3.8.1`
- `sqlalchemy >= 1.4.0`
___
### Installation
```bash
pip install sqlalchemy-query-manager
```

---
### Before / After

**Before** — vanilla SQLAlchemy:

```python
from sqlalchemy import create_engine, select, or_
from sqlalchemy.orm import joinedload, sessionmaker

engine = create_engine(DB_URL)
Session = sessionmaker(engine)

with Session() as session:
    results = (
        session.execute(
            select(Item)
            .join(Group)
            .where(or_(Item.is_valid == True, Item.number > 100))
            .where(Group.is_active == True)
            .options(joinedload(Item.group))
            .order_by(Item.number.desc())
            .limit(20)
        )
        .scalars()
        .all()
    )
```

**After** — with `sqlalchemy-query-manager`:

```python
from sqlalchemy_query_manager.core.helpers import Q

results = (
    Item.query_manager
    .where(Q(is_valid=True) | Q(number__gt=100), group__is_active=True)
    .select_related("group")
    .order_by("-number")
    .limit(20)
    .all()
)
```

---

### Features

**Filtering**
- Django-style `__` lookups — `where(number__gte=100, name__ilike="test%")`
- `Q` objects — `Q(x=1) | Q(y=2)`, nesting with `&` and `|`
- Auto-join on FK filters — `where(group__is_active=True)` joins automatically

**Eager Loading**
- `select_related()` — JOIN-based loading (joinedload)
- `prefetch_related()` — separate query loading (selectinload)
- Combinable and nestable: `select_related("group").prefetch_related("group__owner")`

**Explicit Joins**
- `join()` / `inner_join()`, `left_join()`, `full_join()`

**CRUD**
- `create()`, `bulk_create()`
- `update()`, `bulk_update()`, `update_raw()`
- `get_or_create()`, `update_or_create()`
- `delete()`

**Querying**
- `get()`, `first()`, `last()`, `all()`, `count()`, `exists()`
- `only()` — select specific columns
- `select_for_update()` — row-level locking inside an explicit transaction
- `order_by()` with `E(field, nulls_last)` / `E(field, nulls_first)`
- `limit()`, `offset()`, `distinct()`
- `aggregate()` — `Sum`, `Avg`, `Count`, `Min`, `Max`
- `raw()` — execute raw SQL safely
- `get_sql_query()` — inspect the generated SQL string

**Session Management**
- `sessionmaker` — session created and committed automatically per operation
- Direct `session` — use your own session, commit manually
- Context manager — full lifecycle control with auto rollback
- `with_session()` — override session per query
- Works with Flask, FastAPI, and any framework

**Async**
- Every method has a native async counterpart — just `await`

**Exceptions**
- `DoesNotExist`, `MultipleObjectsReturned`

---

### Quick Start

#### Setup

Define your models by inheriting `ModelQueryManagerMixin` (or `AsyncModelQueryManagerMixin` for async) and configure the session once in `QueryManagerConfig`. After that, no session management is needed in your query code.

##### Sync

```python
from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, create_engine
from sqlalchemy.orm import DeclarativeBase, relationship, sessionmaker
from sqlalchemy_query_manager.core.base import ModelQueryManagerMixin

engine = create_engine(DB_URL)
Session = sessionmaker(engine)


class Base(DeclarativeBase):
    pass


class Group(Base, ModelQueryManagerMixin):
    class QueryManagerConfig:
        session = Session

    __tablename__ = "group"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    is_active = Column(Boolean)
    owner = relationship("Owner")


class Item(Base, ModelQueryManagerMixin):
    class QueryManagerConfig:
        session = Session

    __tablename__ = "item"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    number = Column(Integer)
    is_valid = Column(Boolean)
    group_id = Column(Integer, ForeignKey("group.id"))
    group = relationship("Group")
```

##### Async

```python
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_query_manager.core.base import AsyncModelQueryManagerMixin

engine = create_async_engine(DB_URL)
AsyncSessionMaker = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Group(Base, AsyncModelQueryManagerMixin):
    class QueryManagerConfig:
        session = AsyncSessionMaker

    __tablename__ = "group"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    is_active = Column(Boolean)
    owner = relationship("Owner")


class Item(Base, AsyncModelQueryManagerMixin):
    class QueryManagerConfig:
        session = AsyncSessionMaker

    __tablename__ = "item"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    number = Column(Integer)
    is_valid = Column(Boolean)
    group_id = Column(Integer, ForeignKey("group.id"))
    group = relationship("Group")
```

---

#### Session Management

The package supports three ways to manage sessions. Choose the one that fits your framework.

##### 1. `sessionmaker`

A session is created automatically for each operation and committed at the end. Objects are expunged after the call, so they are safe to use outside of a session context.

```python
# Sync
Session = sessionmaker(engine)

class Item(Base, ModelQueryManagerMixin):
    class QueryManagerConfig:
        session = Session

# Async
AsyncSessionMaker = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

class Item(Base, AsyncModelQueryManagerMixin):
    class QueryManagerConfig:
        session = AsyncSessionMaker
```

##### 2. Direct `session` — for frameworks like Flask

Pass an already-open session. The package will use it as-is and will not commit or expunge. Objects remain attached to the session, so lazy relationship access works freely.

```python
# Flask
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

class Item(Base, ModelQueryManagerMixin):
    class QueryManagerConfig:
        session = db.session

# FastAPI — pass session per query
async def get_items(session: AsyncSession = Depends(get_db)):
    return await Item.query_manager.with_session(session).where(is_valid=True).all()
```

##### 3. Context manager — full lifecycle control

The package calls your context manager for each operation. Commit, rollback, and close are handled by your code, giving you full transactional control.

```python
# Sync
from contextlib import contextmanager

@contextmanager
def session_scope():
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

class Item(Base, ModelQueryManagerMixin):
    class QueryManagerConfig:
        session = session_scope

# Async
from contextlib import asynccontextmanager

@asynccontextmanager
async def async_session_scope():
    session = AsyncSession(engine)
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()

class Item(Base, AsyncModelQueryManagerMixin):
    class QueryManagerConfig:
        session = async_session_scope
```

##### Per-query session override

You can always override the configured session for a specific operation, without changing the model configuration.

```python
# Pass directly to a method
items = Item.query_manager.all(session=your_session)

# Or set for a chain of operations
qm = Item.query_manager.with_session(your_session)
items = qm.where(is_valid=True).all()
item = qm.get(id=1)
```

> ⚠️ With `sessionmaker` and context managers, objects are **expunged** after each operation.
> Accessing lazy relationship attributes on detached objects raises `DetachedInstanceError`.
> Use `select_related()` or `prefetch_related()` to load relationships upfront, or use a direct `session`.

---

#### Filtering

Filter records using Django-style `__` lookup operators. Multiple `where()` calls are combined with `AND`. Use `Q` objects for `OR` / `AND` combinations and complex nested conditions. Foreign key filters trigger automatic JOINs — no explicit join needed.

```python
from sqlalchemy_query_manager.core.helpers import Q

# Simple filters
item = Item.query_manager.get(id=1)
items = Item.query_manager.where(is_valid=True).all()
item = Item.query_manager.first()
item = Item.query_manager.last()
count = Item.query_manager.where(is_valid=True).count()

# Lookup operators: eq, in, not_in, gt, lt, gte, lte, not, is, is_not, like, ilike, isnull
items = Item.query_manager.where(number__gte=100, name__ilike="test%").all()
items = Item.query_manager.where(number__in=[1, 2, 3]).all()
items = Item.query_manager.where(name__isnull=False).all()

# OR / AND with Q
items = Item.query_manager.where(Q(is_valid=True) | Q(number__gt=100)).all()
items = Item.query_manager.where(
    (Q(is_valid=True) | Q(number__gt=100)) & Q(group__is_active=True)
).all()

# Auto-join on FK — no explicit join needed
items = Item.query_manager.where(group__is_active=True).all()
items = Item.query_manager.where(group__owner__email__isnull=False).all()
```

##### Async

```python
item = await Item.query_manager.get(id=1)
items = await Item.query_manager.where(is_valid=True).all()
item = await Item.query_manager.first()
item = await Item.query_manager.last()
count = await Item.query_manager.where(is_valid=True).count()
items = await Item.query_manager.where(number__gte=100, name__ilike="test%").all()
items = await Item.query_manager.where(number__in=[1, 2, 3]).all()
items = await Item.query_manager.where(Q(is_valid=True) | Q(number__gt=100)).all()
items = await Item.query_manager.where(
    (Q(is_valid=True) | Q(number__gt=100)) & Q(group__is_active=True)
).all()
items = await Item.query_manager.where(group__is_active=True).all()
items = await Item.query_manager.where(group__owner__email__isnull=False).all()
```

---

#### Ordering, Limit, Offset, Distinct

Control the shape of your result set. Prefix a field name with `-` for descending order. Use the `E` class to control `NULL` placement. `only()` restricts which columns are fetched and returns `Row` objects instead of model instances.

```python
from sqlalchemy.sql.expression import nulls_last, nulls_first
from sqlalchemy_query_manager.core.helpers import E

# Ordering
items = Item.query_manager.order_by("name").all()                      # ASC
items = Item.query_manager.order_by("-number").all()                   # DESC
items = Item.query_manager.order_by(E("name", nulls_last)).all()      # NULLs at end
items = Item.query_manager.order_by(E("name", nulls_first)).all()     # NULLs at start

# Limit / Offset
items = Item.query_manager.order_by("id").limit(10).offset(20).all()

# Distinct
items = Item.query_manager.distinct().all()

# Select specific columns — returns Row objects
items = Item.query_manager.only("id", "name").all()
items = Item.query_manager.only("*").all()  # all columns as Row
```

##### Async

```python
items = await Item.query_manager.order_by("-number").all()
items = await Item.query_manager.order_by(E("name", nulls_last)).all()
items = await Item.query_manager.order_by("id").limit(10).offset(20).all()
items = await Item.query_manager.distinct().all()
items = await Item.query_manager.only("id", "name").all()
```

---

#### Row Locking

Use `select_for_update()` to add `SELECT ... FOR UPDATE` row locks. The query must be executed with an explicit SQLAlchemy session, so the lock is held until your transaction commits or rolls back.

```python
with Session() as session:
    item = (
        Item.query_manager
        .where(id=1)
        .select_for_update()
        .get(session=session)
    )
    item.name = "locked update"
    session.commit()
```

Optional lock modes:

```python
item = Item.query_manager.where(id=1).select_for_update(nowait=True).get(session=session)
items = Item.query_manager.where(status="pending").select_for_update(skip_locked=True).all(session=session)
item = Item.query_manager.where(id=1).select_for_update(no_key=True).get(session=session)
```

`nowait=True` and `skip_locked=True` cannot be used together. `no_key=True` maps to PostgreSQL's `FOR NO KEY UPDATE`.

##### Async

```python
async with AsyncSessionMaker() as session:
    item = await (
        Item.query_manager
        .where(id=1)
        .select_for_update()
        .get(session=session)
    )
    item.name = "locked update"
    await session.commit()
```

---

#### Eager Loading

Load relationships upfront to avoid `DetachedInstanceError` when objects are detached from the session. Use `select_related()` for JOIN-based loading and `prefetch_related()` for a separate `SELECT IN` query. Both can be combined and support nested paths like `"group__owner"`.

```python
# select_related — JOIN-based (joinedload)
items = Item.query_manager.select_related("group").all()

# prefetch_related — separate SELECT IN query, never conflicts with FK filters
items = Item.query_manager.prefetch_related("group").all()

# Nested paths
items = Item.query_manager.prefetch_related("group__owner").all()

# Combined — select_related and prefetch_related work together
items = (
    Item.query_manager
    .select_related("group")
    .prefetch_related("group__owner")
    .all()
)

# Safe with Q filters — no duplicate JOINs
items = (
    Item.query_manager
    .where(Q(group__is_active=True) | Q(name="standalone"))
    .select_related("group")
    .all()
)
```

##### Async

```python
items = await Item.query_manager.select_related("group").all()
items = await Item.query_manager.prefetch_related("group__owner").all()
items = await (
    Item.query_manager
    .select_related("group")
    .prefetch_related("group__owner")
    .all()
)
items = await (
    Item.query_manager
    .where(Q(group__is_active=True) | Q(name="standalone"))
    .select_related("group")
    .all()
)
```

---

#### Explicit Joins

Specify the join type explicitly when you need control over how tables are joined. Unlike FK-based auto-joins triggered by `where()`, these joins are applied unconditionally and affect which rows are included in the result.

```python
# INNER JOIN
items = Item.query_manager.join("group").all()
items = Item.query_manager.inner_join("group").where(group__is_active=True).all()

# LEFT JOIN — includes items without a group
items = Item.query_manager.left_join("group").all()

# FULL JOIN
items = Item.query_manager.full_join("group").all()

# Nested relationships
items = Item.query_manager.left_join("group__owner").all()
```

##### Async

```python
items = await Item.query_manager.join("group").all()
items = await Item.query_manager.left_join("group").all()
items = await Item.query_manager.left_join("group__owner").all()
```

---

#### CRUD

Full set of create, update, and delete operations. Write operations require `where()` filters before `update()` and `delete()` to prevent accidental full-table modifications. `get_or_create()` and `update_or_create()` perform a lookup first and create or update only if needed.

```python
# Create
item = Item.query_manager.create(name="widget", number=42, is_valid=True)
items = Item.query_manager.bulk_create([
    {"name": "widget", "number": 1, "is_valid": True},
    {"name": "gadget", "number": 2, "is_valid": False},
])

# Update — returns updated object(s)
item = Item.query_manager.where(id=1).update(name="updated", is_valid=False)
items = Item.query_manager.bulk_update([
    {"id": 1, "name": "updated_1"},
    {"id": 2, "name": "updated_2"},
])

# update_raw — returns rowcount only, skips the extra SELECT for fetching updated objects
rowcount = Item.query_manager.where(is_valid=False).update_raw(number=0)

# Upsert
item, created = Item.query_manager.get_or_create(
    name="widget", defaults={"number": 42, "is_valid": True}
)
item, created = Item.query_manager.update_or_create(
    id=1, defaults={"name": "updated", "is_valid": False}
)

# Delete — returns number of deleted rows
rowcount = Item.query_manager.where(is_valid=False).delete()

# Existence check
exists = Item.query_manager.exists(name="widget")        # True / False
exists = Item.query_manager.where(is_valid=True).exists()
```

##### Async

```python
item = await Item.query_manager.create(name="widget", number=42, is_valid=True)
items = await Item.query_manager.bulk_create([
    {"name": "widget", "number": 1, "is_valid": True},
    {"name": "gadget", "number": 2, "is_valid": False},
])

item = await Item.query_manager.where(id=1).update(name="updated", is_valid=False)
items = await Item.query_manager.bulk_update([
    {"id": 1, "name": "updated_1"},
    {"id": 2, "name": "updated_2"},
])

rowcount = await Item.query_manager.where(is_valid=False).update_raw(number=0)

item, created = await Item.query_manager.get_or_create(
    name="widget", defaults={"number": 42, "is_valid": True}
)
item, created = await Item.query_manager.update_or_create(
    id=1, defaults={"name": "updated", "is_valid": False}
)

rowcount = await Item.query_manager.where(is_valid=False).delete()
exists = await Item.query_manager.exists(name="widget")
```

---

#### Aggregate

Compute summary statistics over a queryset in a single database call. Accepts any combination of `Sum`, `Avg`, `Count`, `Min`, `Max` and returns a plain `dict`. Can be combined with `where()` to aggregate over a filtered subset.

```python
from sqlalchemy_query_manager.core.helpers import Sum, Count, Avg, Min, Max

stats = Item.query_manager.where(is_valid=True).aggregate(
    total=Sum("number"),
    count=Count("id"),
    avg=Avg("number"),
    min=Min("number"),
    max=Max("number"),
)
# {"total": 9200, "count": 42, "avg": 219.0, "min": 1, "max": 999}
```

##### Async

```python
stats = await Item.query_manager.where(is_valid=True).aggregate(
    total=Sum("number"),
    count=Count("id"),
    avg=Avg("number"),
    min=Min("number"),
    max=Max("number"),
)
```

---

#### Raw SQL

Execute a raw SQL string when the query manager API is not expressive enough. Parameters are passed as named keyword arguments and bound safely — no string interpolation, no SQL injection risk. Returns a list of `RowMapping` objects accessible by column name.

```python
rows = Item.query_manager.raw(
    "SELECT * FROM item WHERE group_id = :group_id AND is_valid = :is_valid",
    group_id=1,
    is_valid=True,
)

for row in rows:
    print(row["id"], row["name"])
```

##### Async

```python
rows = await Item.query_manager.raw(
    "SELECT * FROM item WHERE group_id = :group_id AND is_valid = :is_valid",
    group_id=1,
    is_valid=True,
)
```

---

#### Get SQL Query

Inspect the SQL string that would be generated for a given query, without executing it. Useful for debugging, logging, or verifying that filters and joins are constructed as expected.

```python
from sqlalchemy_query_manager.core.helpers import Q

sql = Item.query_manager.where(
    Q(is_valid=True) | Q(number__gt=100), group__is_active=True
).get_sql_query()

print(sql)
# SELECT item.id, item.name, item.number, item.is_valid, item.group_id
# FROM item JOIN "group" ON "group".id = item.group_id
# WHERE (item.is_valid = true OR item.number > 100) AND "group".is_active = true
```

---

### Links

- [GitHub](https://github.com/ViAchKoN/sqlalchemy-query-manager)
- [PyPI](https://pypi.org/project/sqlalchemy-query-manager/)
- [dataclass-sqlalchemy-mixins](https://github.com/ViAchKoN/dataclass-sqlalchemy-mixins)
