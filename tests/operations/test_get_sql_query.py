import datetime as dt

from sqlalchemy import nulls_first, nulls_last

from sqlalchemy_query_manager.core.helpers import E
from tests import models_factory


def test_order_by__id__ok(db_session, item_sql_query_manager):
    items = models_factory.ItemFactory.create_batch(size=5)

    for order_by in [
        "id",
        "-id",
    ]:
        expected_items = items
        if order_by in ["-id"]:
            expected_items = list(reversed(items))

        results = item_sql_query_manager.query_manager.order_by(order_by).all()

        for expected_item, result in zip(expected_items, results):
            assert result.as_dict() == expected_item.as_dict()


def test_order_by__dates__ok(
    db_session,
    item_sql_query_manager,
):
    now = dt.datetime.now()

    first_date = now + dt.timedelta(days=1)
    first_item = models_factory.ItemFactory.create(
        created_at=first_date,
    )

    second_date = now + dt.timedelta(days=2)
    second_item = models_factory.ItemFactory.create(
        created_at=second_date,
    )

    third_date = now + dt.timedelta(days=3)
    third_item = models_factory.ItemFactory.create(
        created_at=third_date,
    )

    fourth_date = now + dt.timedelta(days=4)
    fourth_item = models_factory.ItemFactory.create(
        created_at=fourth_date,
    )

    fifth_date = now + dt.timedelta(days=5)
    fifth_item = models_factory.ItemFactory.create(
        created_at=fifth_date,
    )

    items = [first_item, second_item, third_item, fourth_item, fifth_item]

    for order_by in [
        "created_at",
        "-created_at",
    ]:
        expected_items = items
        if order_by in [
            "-created_at",
        ]:
            expected_items = list(reversed(items))

        results = item_sql_query_manager.query_manager.order_by(order_by).all()

        for expected_item, result in zip(expected_items, results):
            assert result.as_dict() == expected_item.as_dict()


def test_order_by__name__ok(
    db_session,
    item_sql_query_manager,
):
    items = []

    for name in [
        "aar",
        "abc",
        "cat",
        "wow",
    ]:
        items.append(models_factory.ItemFactory.create(name=name))

    for order_by in [
        "name",
        "-name",
    ]:
        expected_items = items
        if order_by in [
            "-name",
        ]:
            expected_items = list(reversed(items))

        results = item_sql_query_manager.query_manager.order_by(order_by).all()

        for expected_item, result in zip(expected_items, results):
            assert result.as_dict() == expected_item.as_dict()


def test_async_all__order_by__name__nulls_last__ok(
    db_session,
    item_sql_query_manager,
):
    items = []

    for name in [
        "aar",
        "abc",
        "cat",
        "wow",
    ]:
        items.append(models_factory.ItemFactory.create(name=name))

    null_name_item = models_factory.ItemFactory.create(name=None)

    for order_by in [
        "name",
        "-name",
    ]:
        expected_items = items.copy()
        if order_by in [
            "-name",
        ]:
            expected_items = list(reversed(items))

        expected_items.append(null_name_item)

        results = item_sql_query_manager.query_manager.order_by(
            E(order_by, nulls_last)
        ).all()

        for expected_item, result in zip(expected_items, results):
            assert result.as_dict() == expected_item.as_dict()


def test_async_all__order_by__name__nulls_first__ok(
    db_session,
    item_sql_query_manager,
):
    items = []

    for name in [
        "aar",
        "abc",
        "cat",
        "wow",
    ]:
        items.append(models_factory.ItemFactory.create(name=name))

    null_name_item = models_factory.ItemFactory.create(name=None)

    for order_by in [
        "name",
        "-name",
    ]:
        expected_items = items.copy()
        if order_by in [
            "-name",
        ]:
            expected_items = list(reversed(items))

        expected_items.insert(0, null_name_item)

        results = item_sql_query_manager.query_manager.order_by(
            E(order_by, nulls_first)
        ).all()

        for expected_item, result in zip(expected_items, results):
            assert result.as_dict() == expected_item.as_dict()


def test_order_by__multiple_fields_order_preserved__ok(
    db_session,
    item_sql_query_manager,
):
    """Order of fields in multi-column ORDER BY must be preserved (set was breaking this)."""
    import datetime as _dt

    now = _dt.datetime.now()

    # Two items with same created_at bucket — distinguishable only by number
    item_a = models_factory.ItemFactory.create(
        created_at=now + _dt.timedelta(days=1), number=10
    )
    item_b = models_factory.ItemFactory.create(
        created_at=now + _dt.timedelta(days=1), number=5
    )
    item_c = models_factory.ItemFactory.create(
        created_at=now + _dt.timedelta(days=2), number=99
    )

    # ORDER BY created_at ASC, number DESC
    results = item_sql_query_manager.query_manager.order_by(
        "created_at", "-number"
    ).all()

    # Same created_at: item_a (10) before item_b (5) DESC → item_a first
    assert results[0].id == item_a.id
    assert results[1].id == item_b.id
    assert results[2].id == item_c.id


def test_order_by__deduplication__ok(
    db_session,
    item_sql_query_manager,
):
    """Duplicate field in chained order_by must appear only once — first occurrence wins."""
    models_factory.ItemFactory.create_batch(size=3)

    qm1 = item_sql_query_manager.query_manager.order_by("id")
    qm2 = qm1.order_by("id")  # duplicate

    assert qm2._order_by == ["id"]  # deduplicated, not ["id", "id"]

    # Results still correct
    results = qm2.all()
    assert len(results) == 3
    assert [r.id for r in results] == sorted(r.id for r in results)


def test_order_by__chained_order_preserved__ok(
    db_session,
    item_sql_query_manager,
):
    """Chained .order_by() calls must preserve the first field first."""
    items = []
    for number in [3, 1, 2]:
        items.append(models_factory.ItemFactory.create(number=number))

    # order_by("number") then chaining order_by("id") — number must be primary
    results = (
        item_sql_query_manager.query_manager.order_by("number").order_by("id").all()
    )

    numbers = [r.number for r in results]
    assert numbers == sorted(numbers)
