import pytest

from tests import models_factory


# ──────────────────────────────────────────────────────────────────────────────
# select_related (joinedload / contains_eager)
# ──────────────────────────────────────────────────────────────────────────────


def test_select_related__group__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    group = models_factory.GroupFactory.create(name="test_group")
    models_factory.ItemFactory.create(group=group)

    results = item_sql_query_manager.query_manager.select_related("group").all()

    assert len(results) == 1
    # group is accessible after expunge — loaded before detach
    assert results[0].group.name == "test_group"


def test_select_related__nested__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    owner = models_factory.OwnerFactory.create(first_name="John")
    group = models_factory.GroupFactory.create(owner=owner)
    models_factory.ItemFactory.create(group=group)

    results = item_sql_query_manager.query_manager.select_related("group__owner").all()

    assert len(results) == 1
    assert results[0].group.owner.first_name == "John"


def test_select_related__with_fk_filter__uses_contains_eager__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    """
    When where(group__name=...) and select_related('group') are combined,
    the table is already JOINed for the filter — contains_eager is used
    instead of joinedload to avoid duplicate JOINs.
    """
    group = models_factory.GroupFactory.create(name="target_group")
    models_factory.ItemFactory.create(group=group)
    # Unexpected item in a different group
    models_factory.GroupFactory.create(name="other_group", with_item=True)

    results = (
        item_sql_query_manager.query_manager.where(group__name="target_group")
        .select_related("group")
        .all()
    )

    assert len(results) == 1
    assert results[0].group.name == "target_group"


def test_select_related__detached_attributes_accessible__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    """Attributes of eagerly loaded relationships must be accessible after expunge."""
    group = models_factory.GroupFactory.create(name="detach_test")
    models_factory.ItemFactory.create(name="item_one", group=group)

    results = item_sql_query_manager.query_manager.select_related("group").all()

    # No session active at this point — objects are detached
    # This would raise DetachedInstanceError without eager loading
    assert results[0].group.name == "detach_test"


# ──────────────────────────────────────────────────────────────────────────────
# prefetch_related (selectinload)
# ──────────────────────────────────────────────────────────────────────────────


def test_prefetch_related__group__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    group = models_factory.GroupFactory.create(name="prefetch_group")
    models_factory.ItemFactory.create(group=group)

    results = item_sql_query_manager.query_manager.prefetch_related("group").all()

    assert len(results) == 1
    assert results[0].group.name == "prefetch_group"


def test_prefetch_related__nested__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    owner = models_factory.OwnerFactory.create(first_name="Alice")
    group = models_factory.GroupFactory.create(owner=owner)
    models_factory.ItemFactory.create(group=group)

    results = item_sql_query_manager.query_manager.prefetch_related(
        "group__owner"
    ).all()

    assert len(results) == 1
    assert results[0].group.owner.first_name == "Alice"


def test_prefetch_related__with_fk_filter__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    """prefetch_related never conflicts with FK filter JOINs."""
    group = models_factory.GroupFactory.create(name="active_group", is_active=True)
    models_factory.ItemFactory.create(group=group)
    models_factory.GroupFactory.create(is_active=False, with_item=True)

    results = (
        item_sql_query_manager.query_manager.where(group__is_active=True)
        .prefetch_related("group")
        .all()
    )

    assert len(results) == 1
    assert results[0].group.name == "active_group"


def test_prefetch_related__detached_attributes_accessible__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    group = models_factory.GroupFactory.create(name="prefetch_detach")
    models_factory.ItemFactory.create(group=group)

    results = item_sql_query_manager.query_manager.prefetch_related("group").all()

    assert results[0].group.name == "prefetch_detach"


# ──────────────────────────────────────────────────────────────────────────────
# async versions
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_async_select_related__group__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    group = models_factory.GroupFactory.create(name="async_group")
    models_factory.ItemFactory.create(group=group)

    results = await async_item_sql_query_manager.query_manager.select_related(
        "group"
    ).all()

    assert len(results) == 1
    assert results[0].group.name == "async_group"


@pytest.mark.asyncio
async def test_async_select_related__nested__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    owner = models_factory.OwnerFactory.create(first_name="Bob")
    group = models_factory.GroupFactory.create(owner=owner)
    models_factory.ItemFactory.create(group=group)

    results = await async_item_sql_query_manager.query_manager.select_related(
        "group__owner"
    ).all()

    assert len(results) == 1
    assert results[0].group.owner.first_name == "Bob"


@pytest.mark.asyncio
async def test_async_select_related__with_fk_filter__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    group = models_factory.GroupFactory.create(name="async_target")
    models_factory.ItemFactory.create(group=group)
    models_factory.GroupFactory.create(name="async_other", with_item=True)

    results = await (
        async_item_sql_query_manager.query_manager.where(group__name="async_target")
        .select_related("group")
        .all()
    )

    assert len(results) == 1
    assert results[0].group.name == "async_target"


@pytest.mark.asyncio
async def test_async_prefetch_related__group__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    group = models_factory.GroupFactory.create(name="async_prefetch")
    models_factory.ItemFactory.create(group=group)

    results = await async_item_sql_query_manager.query_manager.prefetch_related(
        "group"
    ).all()

    assert len(results) == 1
    assert results[0].group.name == "async_prefetch"


@pytest.mark.asyncio
async def test_async_prefetch_related__nested__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    owner = models_factory.OwnerFactory.create(first_name="Carol")
    group = models_factory.GroupFactory.create(owner=owner)
    models_factory.ItemFactory.create(group=group)

    results = await async_item_sql_query_manager.query_manager.prefetch_related(
        "group__owner"
    ).all()

    assert len(results) == 1
    assert results[0].group.owner.first_name == "Carol"


# ──────────────────────────────────────────────────────────────────────────────
# Combined select_related + prefetch_related (no strategy conflict)
# ──────────────────────────────────────────────────────────────────────────────


def test_select_related_and_prefetch_related__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    """
    select_related('group') + prefetch_related('group__owner') must not conflict.
    group loaded via joinedload, owner loaded via selectinload chained from it.
    """
    owner = models_factory.OwnerFactory.create(first_name="Dave")
    group = models_factory.GroupFactory.create(name="combo_group", owner=owner)
    models_factory.ItemFactory.create(group=group)

    results = (
        item_sql_query_manager.query_manager.select_related("group")
        .prefetch_related("group__owner")
        .all()
    )

    assert len(results) == 1
    assert results[0].group.name == "combo_group"
    assert results[0].group.owner.first_name == "Dave"


# ──────────────────────────────────────────────────────────────────────────────
# Q filter + select_related (Q filter adds to models_to_join → contains_eager)
# ──────────────────────────────────────────────────────────────────────────────


def test_select_related__with_q_filter__uses_contains_eager__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    from sqlalchemy_query_manager.core.helpers import Q

    active_group = models_factory.GroupFactory.create(name="active", is_active=True)
    models_factory.ItemFactory.create(group=active_group)

    inactive_group = models_factory.GroupFactory.create(is_active=False)
    models_factory.ItemFactory.create(name="standalone", group=inactive_group)

    results = (
        item_sql_query_manager.query_manager.where(
            Q(group__is_active=True) | Q(name="standalone")
        )
        .select_related("group")
        .all()
    )

    assert len(results) == 2
    for obj in results:
        # group loaded — no DetachedInstanceError
        assert obj.group is not None


# ──────────────────────────────────────────────────────────────────────────────
# select_related / prefetch_related with filters + ordering + limit
# ──────────────────────────────────────────────────────────────────────────────


def test_prefetch_related__with_filters_and_limit__ok(
    db_session,
    sync_db_engine,
    item_sql_query_manager,
):
    group = models_factory.GroupFactory.create(name="limited_group", is_active=True)
    for i in range(5):
        models_factory.ItemFactory.create(number=i, is_valid=True, group=group)
    # Unexpected item
    models_factory.ItemFactory.create(is_valid=False, group=group)

    results = (
        item_sql_query_manager.query_manager.where(is_valid=True)
        .order_by("number")
        .limit(3)
        .prefetch_related("group")
        .all()
    )

    assert len(results) == 3
    for obj in results:
        assert obj.group.name == "limited_group"


@pytest.mark.asyncio
async def test_async_select_related_and_prefetch_related__ok(
    db_session,
    sync_db_engine,
    async_item_sql_query_manager,
):
    owner = models_factory.OwnerFactory.create(first_name="Eve")
    group = models_factory.GroupFactory.create(name="async_combo", owner=owner)
    models_factory.ItemFactory.create(group=group)

    results = await (
        async_item_sql_query_manager.query_manager.select_related("group")
        .prefetch_related("group__owner")
        .all()
    )

    assert len(results) == 1
    assert results[0].group.name == "async_combo"
    assert results[0].group.owner.first_name == "Eve"
