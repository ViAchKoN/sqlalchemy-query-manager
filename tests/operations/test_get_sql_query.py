import datetime
import re

from tests.models import ItemStatus


def _build_select_item() -> str:
    """
    Compute the base SELECT clause from the actual model column order.
    This is version-agnostic — works with SQLAlchemy 1.4 and 2.0+
    regardless of how inherited columns are ordered.
    """
    from sqlalchemy import select
    from sqlalchemy.dialects import postgresql

    from tests.models import Item

    sql = str(select(Item).compile(dialect=postgresql.dialect()))
    return re.sub(r"\s+", " ", sql).strip()


SELECT_ITEM = _build_select_item()
SELECT_DISTINCT_ITEM = SELECT_ITEM.replace("SELECT ", "SELECT DISTINCT ", 1)


def normalize(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip()


def test_get_sql_query__no_filters__ok(
    db_session,
    item_sql_query_manager,
):
    sql = item_sql_query_manager.query_manager.get_sql_query()

    assert normalize(sql) == normalize(SELECT_ITEM)


def test_get_sql_query__string__ok(
    db_session,
    item_sql_query_manager,
):
    sql = item_sql_query_manager.query_manager.where(name="foo").get_sql_query()

    assert normalize(sql) == normalize(f"{SELECT_ITEM} WHERE item.name = 'foo'")


def test_get_sql_query__integer__ok(
    db_session,
    item_sql_query_manager,
):
    sql = item_sql_query_manager.query_manager.where(number=42).get_sql_query()

    assert normalize(sql) == normalize(f"{SELECT_ITEM} WHERE item.number = 42")


def test_get_sql_query__bool__ok(
    db_session,
    item_sql_query_manager,
):
    sql_true = item_sql_query_manager.query_manager.where(is_valid=True).get_sql_query()
    sql_false = item_sql_query_manager.query_manager.where(
        is_valid=False
    ).get_sql_query()

    assert normalize(sql_true) == normalize(f"{SELECT_ITEM} WHERE item.is_valid = true")
    assert normalize(sql_false) == normalize(
        f"{SELECT_ITEM} WHERE item.is_valid = false"
    )


def test_get_sql_query__datetime__ok(
    db_session,
    item_sql_query_manager,
):
    dt = datetime.datetime(2024, 1, 1)

    sql = item_sql_query_manager.query_manager.where(created_at__gt=dt).get_sql_query()

    assert normalize(sql) == normalize(
        f"{SELECT_ITEM} WHERE item.created_at > '2024-01-01 00:00:00'"
    )


def test_get_sql_query__enum__ok(
    db_session,
    item_sql_query_manager,
):
    sql = item_sql_query_manager.query_manager.where(
        status=ItemStatus.ACTIVE
    ).get_sql_query()

    assert normalize(sql) == normalize(f"{SELECT_ITEM} WHERE item.status = 'ACTIVE'")


def test_get_sql_query__in__ok(
    db_session,
    item_sql_query_manager,
):
    sql = item_sql_query_manager.query_manager.where(
        name__in=["foo", "bar"]
    ).get_sql_query()

    assert normalize(sql) == normalize(
        f"{SELECT_ITEM} WHERE item.name IN ('foo', 'bar')"
    )


def test_get_sql_query__multiple_filters__ok(
    db_session,
    item_sql_query_manager,
):
    sql = item_sql_query_manager.query_manager.where(
        name="foo",
        is_valid=True,
        number=42,
    ).get_sql_query()

    assert normalize(sql) == normalize(
        f"{SELECT_ITEM} WHERE item.name = 'foo' AND item.is_valid = true AND item.number = 42"
    )


def test_get_sql_query__fk_join__ok(
    db_session,
    item_sql_query_manager,
):
    sql = item_sql_query_manager.query_manager.where(
        group__name="target"
    ).get_sql_query()

    assert normalize(sql) == normalize(
        f'{SELECT_ITEM} JOIN "group" ON "group".id = item.group_id'
        f" WHERE \"group\".name = 'target'"
    )


def test_get_sql_query__q_filter__ok(
    db_session,
    item_sql_query_manager,
):
    from sqlalchemy_query_manager.core.helpers import Q

    sql = item_sql_query_manager.query_manager.where(
        Q(name="foo") | Q(is_valid=True)
    ).get_sql_query()

    assert normalize(sql) == normalize(
        f"{SELECT_ITEM} WHERE item.name = 'foo' OR item.is_valid = true"
    )


def test_get_sql_query__q_and_kwargs__ok(
    db_session,
    item_sql_query_manager,
):
    from sqlalchemy_query_manager.core.helpers import Q

    sql = item_sql_query_manager.query_manager.where(
        Q(name="foo") | Q(is_valid=True),
        number=42,
    ).get_sql_query()

    # kwargs-фильтры идут до Q-фильтров в binary_expressions
    assert normalize(sql) == normalize(
        f"{SELECT_ITEM} WHERE item.number = 42 AND (item.name = 'foo' OR item.is_valid = true)"
    )


def test_get_sql_query__enum_and_datetime__ok(
    db_session,
    item_sql_query_manager,
):
    sql = item_sql_query_manager.query_manager.where(
        status=ItemStatus.PENDING,
        created_at__gt=datetime.datetime(2024, 6, 1, 12, 0, 0),
    ).get_sql_query()

    assert normalize(sql) == normalize(
        f"{SELECT_ITEM} WHERE item.status = 'PENDING'"
        f" AND item.created_at > '2024-06-01 12:00:00'"
    )


def test_get_sql_query__only__ok(
    db_session,
    item_sql_query_manager,
):
    sql = item_sql_query_manager.query_manager.only("name", "number").get_sql_query()

    assert normalize(sql) == "SELECT item.name, item.number FROM item"


def test_get_sql_query__only_and_where__ok(
    db_session,
    item_sql_query_manager,
):
    sql = (
        item_sql_query_manager.query_manager.only("name", "number")
        .where(is_valid=True)
        .get_sql_query()
    )

    assert normalize(sql) == normalize(
        "SELECT item.name, item.number FROM item WHERE item.is_valid = true"
    )


def test_get_sql_query__order_by_asc__ok(
    db_session,
    item_sql_query_manager,
):
    sql = item_sql_query_manager.query_manager.order_by("name").get_sql_query()

    assert normalize(sql) == normalize(f"{SELECT_ITEM} ORDER BY item.name ASC")


def test_get_sql_query__order_by_desc__ok(
    db_session,
    item_sql_query_manager,
):
    sql = item_sql_query_manager.query_manager.order_by("-name").get_sql_query()

    assert normalize(sql) == normalize(f"{SELECT_ITEM} ORDER BY item.name DESC")


def test_get_sql_query__limit__ok(
    db_session,
    item_sql_query_manager,
):
    sql = item_sql_query_manager.query_manager.limit(5).get_sql_query()

    assert normalize(sql) == normalize(f"{SELECT_ITEM} LIMIT 5")


def test_get_sql_query__offset__ok(
    db_session,
    item_sql_query_manager,
):
    sql = item_sql_query_manager.query_manager.offset(10).get_sql_query()

    assert normalize(sql) == normalize(f"{SELECT_ITEM} LIMIT ALL OFFSET 10")


def test_get_sql_query__distinct__ok(
    db_session,
    item_sql_query_manager,
):
    sql = item_sql_query_manager.query_manager.distinct().get_sql_query()

    assert normalize(sql) == normalize(SELECT_DISTINCT_ITEM)


def test_get_sql_query__distinct_and_where__ok(
    db_session,
    item_sql_query_manager,
):
    sql = (
        item_sql_query_manager.query_manager.distinct()
        .where(name="foo")
        .get_sql_query()
    )

    assert normalize(sql) == normalize(
        f"{SELECT_DISTINCT_ITEM} WHERE item.name = 'foo'"
    )


def test_get_sql_query__left_join__ok(
    db_session,
    item_sql_query_manager,
):
    sql = item_sql_query_manager.query_manager.left_join("group").get_sql_query()

    assert normalize(sql) == normalize(
        f'{SELECT_ITEM} LEFT OUTER JOIN "group" ON "group".id = item.group_id'
    )


def test_get_sql_query__full_join__ok(
    db_session,
    item_sql_query_manager,
):
    sql = item_sql_query_manager.query_manager.full_join("group").get_sql_query()

    assert normalize(sql) == normalize(
        f'{SELECT_ITEM} FULL OUTER JOIN "group" ON "group".id = item.group_id'
    )


def test_get_sql_query__left_join_and_where__ok(
    db_session,
    item_sql_query_manager,
):
    sql = (
        item_sql_query_manager.query_manager.left_join("group")
        .where(group__name="target")
        .get_sql_query()
    )

    assert normalize(sql) == normalize(
        f'{SELECT_ITEM} LEFT OUTER JOIN "group" ON "group".id = item.group_id'
        f" WHERE \"group\".name = 'target'"
    )


def test_get_sql_query__combined__ok(
    db_session,
    item_sql_query_manager,
):
    sql = (
        item_sql_query_manager.query_manager.where(name="foo")
        .order_by("-number")
        .limit(5)
        .offset(2)
        .get_sql_query()
    )

    assert normalize(sql) == normalize(
        f"{SELECT_ITEM} WHERE item.name = 'foo' ORDER BY item.number DESC LIMIT 5 OFFSET 2"
    )


def test_get_sql_query__order_by_field_order_preserved__ok(
    db_session,
    item_sql_query_manager,
):
    """ORDER BY field order in SQL must match order_by() call, not arbitrary set order."""
    sql = item_sql_query_manager.query_manager.order_by("number", "-id").get_sql_query()
    sql_normalized = normalize(sql)

    pos_number = sql_normalized.index("item.number ASC")
    pos_id = sql_normalized.index("item.id DESC")

    assert (
        pos_number < pos_id
    ), "item.number ASC must appear before item.id DESC in ORDER BY clause"


def test_get_sql_query__explicit_dialect__ok(
    db_session,
    item_sql_query_manager,
):
    """get_sql_query(dialect=...) must use the provided dialect, not auto-detect."""
    from sqlalchemy.dialects import postgresql

    sql = item_sql_query_manager.query_manager.where(name="foo").get_sql_query(
        dialect=postgresql.dialect()
    )

    assert normalize(sql) == normalize(f"{SELECT_ITEM} WHERE item.name = 'foo'")
