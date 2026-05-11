from sqlalchemy import and_, or_


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
