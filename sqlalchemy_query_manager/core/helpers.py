class E:
    """
    Class for sqlalchemy methods which might be applied to fields.
    As of now only nulls_last, nulls_first are fully tested
    """

    def __init__(self, field_name: str, func):
        self.field_name = field_name
        self.func = func
