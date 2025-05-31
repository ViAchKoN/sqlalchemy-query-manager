# SQLAlchemy query manager
___
### Package requirements
- `python >= 3.8.1`
- `sqlalchemy >= 1.4.0`
___
### Installation
```bash
pip install sqlalchemy-query-manager
```
___
### Introduction

Recently, I published my package [dataclass-sqlalchemy-mixins](https://github.com/ViAchKoN/dataclass-sqlalchemy-mixins), which allows converting dataclasses into SQLAlchemy filters directly, without extra work. Based on that experience, I decided to expand on it and build a new package that operates at the database level, where additional simplicity can be beneficial.

This package is designed for **SQLAlchemy** and provides functionality similar to **Django's ORM**. 
In short, it offers methods like `get`, `first`, `last`, `filter`, and other, without requiring explicit session management—sessions 
are automatically handled when a model is defined.
But it still possible to manually manage session if it is required.

___

### Why This Package?

Typically, retrieving an object from the database in SQLAlchemy requires a lot of boilerplate code:

```python
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

engine = create_engine(DB_URL)
Session = sessionmaker(engine)

async with Session() as session:
    query = select(ObjectModel).where(ObjectModel.id == 1)
    result = await session.execute(query)
    db_object = result.scalars().first()
```

I noticed that helper functions are often written to cover such cases. 
To save time, I decided to write a package that automates these operations.

Moreover, another reason was to simplify filtering queries. As seen in the example above, filtering requires writing the full model name (ObjectModel) along with the attribute (id).
In contrast, **Django allows writing it more concisely like**:

```python
ObjectModel.objects.filter(id=1)
```

___ 
### Key Advantage

✅ User-friendly API, inspired by Django’s ORM.

___
### Description
The package introduces `QueryManager`, which acts as a **proxy** to manage database queries.  
It works with either a **`session`** or a **`sessionmaker`** instance to determine which database connection to use.

Internally, `QueryManager` stores:  

- **Filters** – to refine queries based on conditions.  
- **Orderings** – to control the sorting of results.  
- **Fields** – to specify which columns to retrieve.  

This design allows queries to be **chained** into more complex operations, 
providing a **cleaner and more intuitive** way to interact with SQLAlchemy.

Both **`session`** and **`sessionmaker`** are supported because different frameworks handle database sessions differently:

- **Flask** allows maintaining a session throughout an API call.
- **FastAPI** typically creates a session **only when needed**.

Although **FastAPI** supports dependencies to set a session for the entire request ([example](https://fastapi.tiangolo.com/tutorial/sql-databases/#create-a-session-dependency)),  
in other situations, you might prefer using `sessionmaker` to create a session **only when required** to avoid holding a connection when it is not used.

___
### Usage

To use the package, you first need to set a **session** or **sessionmaker** in either  
`ModelQueryManagerMixin` (for synchronous queries) or `AsyncModelQueryManagerMixin` (for async queries).

### ❗️Important

If a session is not provided, all objects will be **expunged** from the session.  
This means that accessing **relationship fields** on these objects **may cause issues** because they are no longer attached to an active session.  

To avoid this, provide a session when defining `QueryManagerConfig` or pass session to an operation.

#### Setting up `ModelQueryManagerMixin`

```python
from sqlalchemy_query_manager.core.base import ModelQueryManagerMixin
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from sqlalchemy import create_engine


class BaseModel(DeclarativeBase):
    ...


engine = create_engine(DB_URL)
Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class ObjectModel(BaseModel, ModelQueryManagerMixin):
    class QueryManagerConfig:
        session = Session 
```

For **`async`** support, import `AsyncModelQueryManagerMixin`.

```python
from sqlalchemy_query_manager.core.base import AsyncModelQueryManagerMixin
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker


class BaseModel(DeclarativeBase):
    ...


engine = create_async_engine(DB_URL)
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class ObjectModel(BaseModel, AsyncModelQueryManagerMixin):
    class QueryManagerConfig:
        session = Session 
```

**Flask Integration**

If you're using `Flask`, you can directly assign the session from Flask’s `SQLAlchemy` extension:

```python
from sqlalchemy_query_manager.core.base import ModelQueryManagerMixin
from sqlalchemy.orm import DeclarativeBase
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class BaseModel(DeclarativeBase):
    ...


class ObjectModel(BaseModel, ModelQueryManagerMixin):
    class QueryManagerConfig:
        session = db.session
```

This ensures that `QueryManager` works seamlessly within `Flask` applications.

**Session Context managers**

From the version `0.2.0` you can use `context managers` for session management, both synchronous and asynchronous. 
This provides a cleaner way to manage sessions by automatically handling commits and rollbacks.  

You can use a synchronous context manager to manage the session lifecycle:

```python
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from sqlalchemy_query_manager.core.base import ModelQueryManagerMixin
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy_query_manager.core.base import ModelQueryManagerMixin


sync_db_engine = create_engine(DB_URL)
sync_db_sessionmaker = sessionmaker(autocommit=False, autoflush=False, bind=sync_db_engine)

@contextmanager
def session_scope():
    session = sync_db_sessionmaker()  # Create a session
    try:
        yield session  # Yield the session to be used in operations
        session.commit()  # Commit if no exception occurs
    except Exception:
        session.rollback()  # Rollback in case of an exception
        raise
    finally:
        session.close()  # Close the session after use
        
        
class BaseModel(DeclarativeBase):
    ...

        
class ObjectModel(BaseModel, ModelQueryManagerMixin):
    class QueryManagerConfig:
        session = session_scope
```

Async context manager:
```python
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from sqlalchemy_query_manager.core.base import ModelQueryManagerMixin


async_db_engine = create_async_engine(DB_URL, future=True)
async_db_sessionmaker = sessionmaker(async_db_engine, class_=AsyncSession)

@asynccontextmanager
async def async_session_scope():
    session = async_db_sessionmaker()  # Create an async session
    try:
        yield session  # Yield the session to be used in operations
        await session.commit()  # Commit if no exception occurs
    except Exception:
        await session.rollback()  # Rollback in case of an exception
        raise
    finally:
        await session.close()  # Close the session after use
        
class BaseModel(DeclarativeBase):
    ...


class ObjectModel(BaseModel, ModelQueryManagerMixin):
    class QueryManagerConfig:
        session = async_session_scope
```

#### Main operations

The package provides a set of common query operations. 
These operations can be chained and combined to perform complex queries.


**Create Operations**

The `create` method allows you to create new objects in the database with the provided field values.

- If a **session is provided**, the object will be added to that session but **won't be committed automatically** (you must commit manually).  
- If **no session is provided**, the operation will be **automatically committed** at the end.

```python
# Create a new object with automatic commit
db_object = ObjectModel.query_manager.create(field1="value1", field2="value2")

# Create a new object within an existing session (manual commit required)
db_object = ObjectModel.query_manager.create(field1="value1", field2="value2", session=your_session)
your_session.commit()  # Commit manually if session is provided
```

**Async Create**

For async operations, use the same syntax with `await`:

```python
# Async create with automatic commit
db_object = await ObjectModel.query_manager.create(field1="value1", field2="value2")

# Async create within an existing session
db_object = await ObjectModel.query_manager.create(field1="value1", field2="value2", session=your_session)
await your_session.commit()  # Commit manually if session is provided
```

**Update Operations**

The `update` method allows you to update existing records that match the current filters and returns the updated objects.

**⚠️ Important**: You must use `where()` to set filters before calling `update()` to prevent accidental full table updates.

```python
# Update records matching specific criteria
updated_objects = ObjectModel.query_manager.where(status="active").update(
    last_updated=datetime.now(),
    processed=True
)

# Update a single record by ID
updated_object = ObjectModel.query_manager.where(id=1).update(name="New Name")

# Chain multiple filters before updating
updated_objects = ObjectModel.query_manager.where(
    category="electronics"
).where(
    price__lt=100
).update(discount=0.1)
```

**Async Update**

For async operations, use the same syntax with `await`:

```python
# Async update with filters
updated_objects = await ObjectModel.query_manager.where(status="pending").update(
    status="completed",
    completed_at=datetime.now()
)

# Async update with session management
updated_object = await ObjectModel.query_manager.where(id=1).update(
    name="Updated Name",
    session=your_session
)
await your_session.commit()  # Commit manually if session is provided
```

**Setting a Session for Create/Update Operations**

You can provide a session directly to these methods using the `session` parameter:

```python
# Sync operations with session
db_object = ObjectModel.query_manager.create(name="Test", session=your_session)
updated_object = ObjectModel.query_manager.where(id=1).update(name="Updated", session=your_session)

# Async operations with session  
db_object = await ObjectModel.query_manager.create(name="Test", session=your_session)
updated_object = await ObjectModel.query_manager.where(id=1).update(name="Updated", session=your_session)
```


**Get First Value**

```python
# To get the first value:
db_object = ObjectModel.query_manager.first()

# Get the Last Value
# To get the last value using the model's primary key (PK)
db_object = ObjectModel.query_manager.last()
```

**Select specific fields**

You can choose to return specific fields from your model using the `only` method. 
This will return the fields you requested, and the results will be instances of the `Row` class.
Starting from `0.2.0` you can use `*` to return all fields for a model, 
the results will be the instance of the `Row` class.

```python
db_object = ObjectModel.query_manager.only('id', 'description').first()

# access the fields
print(db_object.id)
print(db_object.description)

db_object = ObjectModel.query_manager.only('*').first()

# access the fields
print(db_object.id)
print(db_object.description)
print(db_object.your_other_fields)
```

**Apply filters to a Query**

```python
# To apply filters and get the first result
db_object = ObjectModel.query_manager.where(id=1).first()

# Get all Values with a filter
db_objects = ObjectModel.query_manager.where(id=1).all()
```

**Apply Ordering to a Query**

```python
# To order results in ascending order (default is ascending)
db_objects = ObjectModel.query_manager.order_by('id').all()

# To order results in descending order
db_objects = ObjectModel.query_manager.order_by('-id').all()
```

The package introduces the **`E` class**, which allows applying specific **ordering methods** to fields.  
Currently, it supports:  

- **`nulls_last`** – Places `NULL` values **at the end**  
- **`nulls_first`** – Places `NULL` values **at the beginning**  

```python
from sqlalchemy_query_manager.helpers import E

# Order by name, placing NULL values last
results = ObjectModel.query_manager.order_by(
    E('name', nulls_last)
).all()

# Order by name, placing NULL values first
results = ObjectModel.query_manager.order_by(
    E('name', nulls_first)
).all()
```

**Supported Operations**

The package supports the following operations for filtering queries:

- `eq` (Equal to)
- `in` (In list)
- `not_in` (Not in list)
- `gt` (Greater than)
- `lt` (Less than)
- `gte` (Greater than or equal to)
- `lte` (Less than or equal to)
- `not` (Not equal to)
- `is` (IS statement)
- `is_not` (IS NOT statement)
- `like` (Like pattern match)
- `ilike` (Case-insensitive like)
- `isnull` (Is null)

They should provided after `__`, similar to `Django`. For example:

```python
# Filtering with 'eq' for equality
db_objects = ObjectModel.query_manager.where(id=1).all()

# Filtering with 'in' for multiple values
db_objects = ObjectModel.query_manager.where(id__in=[1, 2, 3]).all()

# Filtering with 'like' for pattern matching
db_objects = ObjectModel.query_manager.where(name__like='John%').all()
```

**Foreign Keys**

If a model has foreign keys, you can write `field_name__foreign_key_field__{sql_op}` to automatically handle the join operation.

For example:

```python
class ParentModel(BaseModel):
    id = Column(Integer, primary_key=True)
    name = Column(String)

class ChildModel(BaseModel):
    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, ForeignKey('parent_model.id'))
    parent = relationship('ParentModel')
    description = Column(String)

# Filter ChildModel by the 'name' field in ParentModel
db_objects = ChildModel.query_manager.where(parent__name='John').all()

# Filter ChildModel by the 'id' field in ParentModel using the 'gt' (greater than) operator
db_objects = ChildModel.query_manager.where(parent__id__gt=10).all()
```

In this case, the package automatically performs the necessary join operation between `ChildModel` and `ParentModel` based on the foreign key parent_id.

### Setting a Session for Queries  

You can provide a session directly to methods such as `.all()`, `.get()`, etc., using the `session` parameter.  
This ensures that the operation runs within the specified session:  

```python
db_objects = ObjectModel.query_manager.all(session=your_session)
db_object = ObjectModel.query_manager.get(id=1, session=your_session)
```

Alternatively, you can set a session for the entire `QueryManager` instance using the `.with_session()` method.
This will ensure that all subsequent operations use the provided session:

```python
query_manager = ObjectModel.query_manager.with_session(your_session)

db_objects = query_manager.all()  # Uses the session set earlier
db_object = query_manager.get(id=1)  # Also uses the session
```

____
### Links
[Github](https://github.com/ViAchKoN/sqlalchemy-query-manager)
