class DoesNotExist(Exception):
    """
    Raised by get() when no object matches the given query.
    Mirrors Django's Model.DoesNotExist behavior.
    """

    pass


class MultipleObjectsReturned(Exception):
    """
    Raised by get() when more than one object matches the given query.
    Mirrors Django's Model.MultipleObjectsReturned behavior.
    """

    pass
