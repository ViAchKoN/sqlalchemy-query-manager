import datetime
import enum as python_enum

from sqlalchemy import and_
from sqlalchemy import func as sa_func
from sqlalchemy import or_


def _format_sql_value(value) -> str:
    """Format a Python value as a SQL literal string (fallback for literal_binds)."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, datetime.datetime):
        return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
    if isinstance(value, datetime.date):
        return f"'{value.strftime('%Y-%m-%d')}'"
    if isinstance(value, python_enum.Enum):
        return f"'{value.name}'"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, (list, tuple)):
        return "(" + ", ".join(_format_sql_value(v) for v in value) + ")"
    return str(value)


class E:
    """
    Class for sqlalchemy methods which might be applied to fields.
    As of now only nulls_last, nulls_first are fully tested
    """

    def __init__(self, field_name: str, func):
        self.field_name = field_name
        self.func = func


class Q:
    """
    Encapsulates filter conditions with support for OR and AND combinations.

    Usage:
        Item.query_manager.where(Q(status="active") | Q(status="pending")).all()
        Item.query_manager.where(Q(number__gt=5) | Q(number__lt=2)).all()
        Item.query_manager.where(Q(a=1) | Q(b=2), is_valid=True).all()
        Item.query_manager.where((Q(a=1) | Q(b=2)) & Q(c=3)).all()
    """

    AND = "AND"
    OR = "OR"

    def __init__(self, **kwargs):
        self.filters = kwargs
        self.connector = self.AND
        self.children = []

    def __or__(self, other: "Q") -> "Q":
        q = Q()
        q.connector = self.OR
        q.children = [self, other]
        return q

    def __and__(self, other: "Q") -> "Q":
        q = Q()
        q.connector = self.AND
        q.children = [self, other]
        return q

    def resolve(self, query_manager) -> tuple:
        """
        Recursively convert Q tree to a SQLAlchemy expression.

        Returns:
            Tuple of (sqlalchemy_expression, list_of_models_to_join)
        """
        if self.children:
            all_models = []
            child_exprs = []
            for child in self.children:
                expr, models = child.resolve(query_manager)
                child_exprs.append(expr)
                all_models.extend(models)

            if self.connector == self.OR:
                return or_(*child_exprs), all_models
            else:
                return and_(*child_exprs), all_models

        # Leaf node — convert filters to binary expressions
        models_binary_expressions = query_manager.get_models_binary_expressions(
            filters=self.filters
        )
        exprs = []
        models = []
        for mbe in models_binary_expressions:
            models.extend(mbe.get("models", []))
            exprs.append(mbe.get("binary_expression"))

        return and_(*exprs), models


class AggregateFunc:
    """
    Base class for aggregate functions used in aggregate().

    Usage:
        Item.query_manager.aggregate(total=Sum('number'), avg=Avg('number'))
        Item.query_manager.where(is_valid=True).aggregate(count=Count('id'))
    """

    _sa_func_name: str

    def __init__(self, field: str):
        self.field = field

    def resolve(self, query_manager):
        """Convert to a SQLAlchemy aggregate expression."""
        db_field = query_manager.get_model_field(self.field)
        return getattr(sa_func, self._sa_func_name)(db_field)


class Sum(AggregateFunc):
    _sa_func_name = "sum"


class Avg(AggregateFunc):
    _sa_func_name = "avg"


class Count(AggregateFunc):
    _sa_func_name = "count"


class Min(AggregateFunc):
    _sa_func_name = "min"


class Max(AggregateFunc):
    _sa_func_name = "max"
