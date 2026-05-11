import pytest

from sqlalchemy_query_manager.core.helpers import Q
from tests import models, models_factory


def test_all__q__or__ok(
    db_session,
    item_sql_query_manager,
):
    first_item = models_factory.ItemFactory.create(name="first")
    second_item = models_factory.ItemFactory.create(name="second")

    # Create unexpected item
    models_factory.ItemFactory.create(name="third")

    results = item_sql_query_manager.query_manager.where(
        Q(name="first") | Q(name="second")
    ).all()

    assert len(results) == 2
    assert {r.id for r in results} == {first_item.id, second_item.id}


def test_all__q__or__with_lookup__ok(
    db_session,
    item_sql_query_manager,
):
    low_item = models_factory.ItemFactory.create(number=1)
    high_item = models_factory.ItemFactory.create(number=10)

    # Create unexpected item (in the middle)
    models_factory.ItemFactory.create(number=5)

    results = item_sql_query_manager.query_manager.where(
        Q(number__lt=2) | Q(number__gt=9)
    ).all()

    assert len(results) == 2
    assert {r.id for r in results} == {low_item.id, high_item.id}


def test_all__q__and__ok(
    db_session,
    item_sql_query_manager,
):
    expected_item = models_factory.ItemFactory.create(name="target", is_valid=True)

    # Create unexpected items
    models_factory.ItemFactory.create(name="target", is_valid=False)
    models_factory.ItemFactory.create(name="other", is_valid=True)

    results = item_sql_query_manager.query_manager.where(
        Q(name="target") & Q(is_valid=True)
    ).all()

    assert len(results) == 1
    assert results[0].id == expected_item.id


def test_all__q__combined_with_kwargs__ok(
    db_session,
    item_sql_query_manager,
):
    expected_item = models_factory.ItemFactory.create(name="first", is_valid=True)

    # Matches Q but not kwargs
    models_factory.ItemFactory.create(name="first", is_valid=False)
    # Matches kwargs but not Q
    models_factory.ItemFactory.create(name="other", is_valid=True)

    results = item_sql_query_manager.query_manager.where(
        Q(name="first") | Q(name="second"),
        is_valid=True,
    ).all()

    assert len(results) == 1
    assert results[0].id == expected_item.id


def test_all__q__nested__ok(
    db_session,
    item_sql_query_manager,
):
    expected_item = models_factory.ItemFactory.create(name="first", is_valid=True)

    # Matches inner OR but is_valid=False
    models_factory.ItemFactory.create(name="first", is_valid=False)
    # Matches is_valid but not name
    models_factory.ItemFactory.create(name="other", is_valid=True)

    results = item_sql_query_manager.query_manager.where(
        (Q(name="first") | Q(name="second")) & Q(is_valid=True)
    ).all()

    assert len(results) == 1
    assert results[0].id == expected_item.id


def test_all__q__chained_with_where__ok(
    db_session,
    item_sql_query_manager,
):
    expected_item = models_factory.ItemFactory.create(name="first", number=1)

    # Matches Q but not kwargs
    models_factory.ItemFactory.create(name="first", number=999)
    # Matches kwargs but not Q
    models_factory.ItemFactory.create(name="other", number=1)

    results = (
        item_sql_query_manager.query_manager.where(
            Q(name="first") | Q(name="second")
        ).where(number=1)
    ).all()

    assert len(results) == 1
    assert results[0].id == expected_item.id


def test_all__q__or__fk_field__ok(
    db_session,
    item_sql_query_manager,
):
    first_group = models_factory.GroupFactory.create(name="first_group", with_item=True)
    first_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == first_group.id)
        .first()
    )

    second_group = models_factory.GroupFactory.create(
        name="second_group", with_item=True
    )
    second_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == second_group.id)
        .first()
    )

    # Create unexpected item in a different group
    models_factory.GroupFactory.create(name="other_group", with_item=True)

    results = item_sql_query_manager.query_manager.where(
        Q(group__name="first_group") | Q(group__name="second_group")
    ).all()

    assert len(results) == 2
    assert {r.id for r in results} == {first_item.id, second_item.id}


def test_all__q__or__fk_and_direct_field__ok(
    db_session,
    item_sql_query_manager,
):
    active_group = models_factory.GroupFactory.create(is_active=True, with_item=True)
    group_item = (
        db_session.query(models.Item)
        .filter(models.Item.group_id == active_group.id)
        .first()
    )

    inactive_group = models_factory.GroupFactory.create(is_active=False)
    standalone_item = models_factory.ItemFactory.create(
        name="standalone", group=inactive_group
    )

    # Create unexpected items
    models_factory.GroupFactory.create(is_active=False, with_item=True)
    models_factory.ItemFactory.create(name="other", group=inactive_group)

    results = item_sql_query_manager.query_manager.where(
        Q(group__is_active=True) | Q(name="standalone")
    ).all()

    assert len(results) == 2
    assert {r.id for r in results} == {group_item.id, standalone_item.id}


def test_all__q__or__multi_filter_leaf__ok(
    db_session,
    item_sql_query_manager,
):
    first_item = models_factory.ItemFactory.create(name="foo", number=1)
    second_item = models_factory.ItemFactory.create(name="bar", number=2)

    # name matches but number doesn't
    models_factory.ItemFactory.create(name="foo", number=2)
    models_factory.ItemFactory.create(name="bar", number=1)
    # neither matches
    models_factory.ItemFactory.create(name="other", number=5)

    results = item_sql_query_manager.query_manager.where(
        Q(name="foo", number=1) | Q(name="bar", number=2)
    ).all()

    assert len(results) == 2
    assert {r.id for r in results} == {first_item.id, second_item.id}


def test_all__q__or__three_way__ok(
    db_session,
    item_sql_query_manager,
):
    first_item = models_factory.ItemFactory.create(name="first")
    second_item = models_factory.ItemFactory.create(name="second")
    third_item = models_factory.ItemFactory.create(name="third")

    # Create unexpected item
    models_factory.ItemFactory.create(name="other")

    results = item_sql_query_manager.query_manager.where(
        Q(name="first") | Q(name="second") | Q(name="third")
    ).all()

    assert len(results) == 3
    assert {r.id for r in results} == {first_item.id, second_item.id, third_item.id}


def test_all__q__multiple_q_args__ok(
    db_session,
    item_sql_query_manager,
):
    first_item = models_factory.ItemFactory.create(name="first", number=7)
    second_item = models_factory.ItemFactory.create(name="second", number=1)

    # matches first Q arg but not second (number in middle range)
    models_factory.ItemFactory.create(name="first", number=4)
    # matches second Q arg but not first (name doesn't match)
    models_factory.ItemFactory.create(name="other", number=7)

    results = item_sql_query_manager.query_manager.where(
        Q(name="first") | Q(name="second"),
        Q(number__gt=5) | Q(number__lt=2),
    ).all()

    assert len(results) == 2
    assert {r.id for r in results} == {first_item.id, second_item.id}


def test_all__q__invalid_positional_arg__raises_type_error(
    item_sql_query_manager,
):
    with pytest.raises(TypeError):
        item_sql_query_manager.query_manager.where("not a Q object")
