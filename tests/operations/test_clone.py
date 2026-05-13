from sqlalchemy_query_manager.core.helpers import Q
from tests import models_factory


def test_clone__preserves_filters__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    expected = models_factory.ItemFactory.create(name="target", is_valid=True)
    models_factory.ItemFactory.create(name="other", is_valid=False)

    original = item_sql_query_manager.query_manager.where(is_valid=True)
    cloned = original.clone()

    assert original.all()[0].id == expected.id
    assert cloned.all()[0].id == expected.id


def test_clone__preserves_q_filters__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    a = models_factory.ItemFactory.create(is_valid=True)
    b = models_factory.ItemFactory.create(number=999)
    models_factory.ItemFactory.create(is_valid=False, number=1)

    original = item_sql_query_manager.query_manager.where(
        Q(is_valid=True) | Q(number=999)
    )
    cloned = original.clone()

    original_ids = {obj.id for obj in original.all()}
    cloned_ids = {obj.id for obj in cloned.all()}

    assert original_ids == cloned_ids == {a.id, b.id}


def test_clone__preserves_order_limit_offset__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    for i in range(5):
        models_factory.ItemFactory.create(number=i)

    original = (
        item_sql_query_manager.query_manager.order_by("number").limit(2).offset(1)
    )
    cloned = original.clone()

    original_results = original.all()
    cloned_results = cloned.all()

    assert len(original_results) == len(cloned_results) == 2
    assert [obj.id for obj in original_results] == [obj.id for obj in cloned_results]


def test_clone__preserves_select_related__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    group = models_factory.GroupFactory.create(name="clone_group")
    models_factory.ItemFactory.create(group=group)

    original = item_sql_query_manager.query_manager.select_related("group")
    cloned = original.clone()

    results = cloned.all()
    assert len(results) == 1
    assert results[0].group.name == "clone_group"


def test_clone__is_independent__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    """Modifying clone must not affect the original."""
    original = item_sql_query_manager.query_manager.where(is_valid=True)
    cloned = original.clone()

    cloned = cloned.where(number=42).limit(5)

    assert original._limit is None
    assert "number" not in original._filters


def test_clone__independent_q_filters__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    original = item_sql_query_manager.query_manager.where(Q(name="foo") | Q(name="bar"))
    cloned = original.clone()

    # Modifying cloned _q_filters list must not change original
    cloned._q_filters.clear()

    assert len(original._q_filters) == 1
