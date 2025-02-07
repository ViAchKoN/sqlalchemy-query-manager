# SQLAlchemy query manager
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
`ModelQueryManager` (for synchronous queries) or `AsyncModelQueryManager` (for async queries).

#### Setting up `ModelQueryManager`

```python
from sqlalchemy_query_manager.core.base import ModelQueryManager
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from sqlalchemy import create_engine

class BaseModel(DeclarativeBase):
    ...

engine = create_engine(DB_URL)
Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class ObjectModel(BaseModel, ModelQueryManager):
    class QueryManagerConfig:
        sessionmaker = Session 
```

For **`async`** support, import `AsyncModelQueryManager`.

```python
The rest of the setup remains the same.

from sqlalchemy_query_manager.core.base import AsyncModelQueryManager
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

class BaseModel(DeclarativeBase):
    ...

engine = create_async_engine(DB_URL)
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

class ObjectModel(BaseModel, AsyncModelQueryManager):
    class QueryManagerConfig:
        sessionmaker = Session 
```

**Flask Integration**

If you're using `Flask`, you can directly assign the session from Flask’s SQLAlchemy extension:

```python
from sqlalchemy_query_manager.core.base import ModelQueryManager
from sqlalchemy.orm import DeclarativeBase
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

class BaseModel(DeclarativeBase):
    ...

class ObjectModel(BaseModel, ModelQueryManager):
    class QueryManagerConfig:
        session = db.session
```

This ensures that `QueryManager` works seamlessly within `Flask` applications.

#### Main operations

The package provides a set of common query operations. 
These operations can be chained and combined to perform complex queries.

**Get First Value**

```python
# To get the first value:
db_object = ObjectModel.query_manager.first()

# Get the Last Value
# To get the last value using the model's primary key (PK)
db_object = ObjectModel.query_manager.last()
```

**Select specific fields

You can choose to return specific fields from your model using the `only` method. 
This will return the fields you requested, and the results will be instances of the `Row` class.

```python
db_object = ObjectModel.query_manager.only('id', 'description').first()

# access the fields
print(db_object.id)
print(db_object.description)
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

____
### Links
[Github](https://github.com/ViAchKoN/sqlalchemy-query-manager)
