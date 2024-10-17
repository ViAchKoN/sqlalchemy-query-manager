import pytest
import datetime

from tests import models_factory, models
from tests.models import Item


@pytest.mark.asyncio
async def test_async_all__filter__eq__ok(
    db_session,
    async_item_sql_query_manager,
):
    expected_item_name = "expected_item_name"

    expected_item = models_factory.ItemFactory.create(name=expected_item_name)

    # Create unexpected items
    models_factory.ItemFactory.create_batch(size=4)

    results = await async_item_sql_query_manager.query_manager.where(name=expected_item_name).all()

    assert len(results) == 1

    result = results[0]

    assert result.as_dict() == expected_item.as_dict()


@pytest.mark.asyncio
async def test_async_all__filter__multiple_filters__ok(
    db_session,
    async_item_sql_query_manager,
):
    expected_item_name = "expected_item_name"
    expected_item_number = 1

    expected_item = models_factory.ItemFactory.create(
        name=expected_item_name,
        number=expected_item_number,
    )

    # Create unexpected items
    models_factory.ItemFactory.create_batch(number=999, size=4)

    query = async_item_sql_query_manager.query_manager

    query = query.where(name=expected_item_name)
    query = query.where(number=1)
    query = query.where(number__not_in=[999, ])

    results = await query.all()

    assert len(results) == 1

    result = results[0]

    assert result.as_dict() == expected_item.as_dict()


@pytest.mark.asyncio
async def test_async_all__filter__in_not_in__ok(
    db_session,
    async_item_sql_query_manager,
):
    first_number = 1
    first_item = models_factory.ItemFactory.create(
        number=first_number,
    )

    second_number = 2
    second_item = models_factory.ItemFactory.create(
        number=second_number,
    )

    third_number = 3
    third_item = models_factory.ItemFactory.create(
        number=third_number,
    )

    fourth_number = 4
    fourth_item = models_factory.ItemFactory.create(
        number=fourth_number,
    )

    fifth_number = 5
    fifth_item = models_factory.ItemFactory.create(
        number=fifth_number,
    )

    assert db_session.query(models.Item).count() == 5

    for field, expected_items in (
        ("number__in", [fourth_item, fifth_item]),
        ("number__not_in", [first_item, second_item, third_item]),
    ):
        results = await async_item_sql_query_manager.query_manager.where(**{field: [fourth_number, fifth_number]}).all()

        assert len(results) == len(expected_items)


@pytest.mark.asyncio
async def test_async_all__filter__in_not_in__dates__ok(
    db_session,
    async_item_sql_query_manager,
):
    now = datetime.datetime.now()

    first_date = now + datetime.timedelta(days=1)
    first_item = models_factory.ItemFactory.create(
        created_at=first_date,
    )

    second_date = now + datetime.timedelta(days=2)
    second_item = models_factory.ItemFactory.create(
        created_at=second_date,
    )

    third_date = now + datetime.timedelta(days=3)
    third_item = models_factory.ItemFactory.create(
        created_at=third_date,
    )

    fourth_date = now + datetime.timedelta(days=4)
    fourth_item = models_factory.ItemFactory.create(
        created_at=fourth_date,
    )

    fifth_date = now + datetime.timedelta(days=5)
    fifth_item = models_factory.ItemFactory.create(
        created_at=fifth_date,
    )

    assert db_session.query(models.Item).count() == 5

    for field, expected_items in (
        ("created_at__in", [fourth_item, fifth_item]),
        ("created_at__not_in", [first_item, second_item, third_item]),
    ):
        results = await async_item_sql_query_manager.query_manager.where(**{field: [fourth_date, fifth_date]}).all()

        assert len(results) == len(expected_items)

        for expected_item, result in zip(expected_items, results):
            assert result.as_dict() == expected_item.as_dict()


@pytest.mark.asyncio
async def test_async_all__filter__gt_lt_gte_lte__ok(
    db_session,
    async_item_sql_query_manager,
):
    for i in range(1, 6):
        models_factory.ItemFactory.create(
            number=i,
        )

    assert db_session.query(models.Item).count() == 5

    for field, expected_item_numbers in (
        ("number__gt", [4, 5]),
        ("number__lt", [1, 2]),
        ("number__gte", [3, 4, 5]),
        ("number__lte", [1, 2, 3]),
    ):
        results = await async_item_sql_query_manager.query_manager.where(**{field: 3}).all()

        assert len(results) == len(expected_item_numbers)

        for expected_item_number, result in zip(expected_item_numbers, results):
            assert result.number == expected_item_number


@pytest.mark.asyncio
async def test_async_all__filter__not__ok(
    db_session,
    async_item_sql_query_manager,
):
    expected_item_names = [
        "expected_item_name_1",
        "expected_item_name_2",
        "expected_item_name_3",
    ]

    not_expected_item_name = "not_expected_item_name"

    expected_items = []
    for item_name in [*expected_item_names, not_expected_item_name]:
        expected_items.append(
            models_factory.ItemFactory.create(
                name=item_name,
            )
        )

    assert db_session.query(models.Item).count() == 4

    results = await async_item_sql_query_manager.query_manager.where(name__not=not_expected_item_name).all()

    assert len(results) == len(expected_item_names)

    for expected_item, result in zip(expected_items, results):
        assert result.as_dict() == expected_item.as_dict()


@pytest.mark.asyncio
async def test_async_all__filter__is_is_not__ok(
    db_session,
    async_item_sql_query_manager,
):
    valid_item = models_factory.ItemFactory.create(
        is_valid=True,
    )
    not_valid_item = models_factory.ItemFactory.create(is_valid=False)

    assert db_session.query(models.Item).count() == 2

    for is_valid, expected_items in (
        (
            False,
            [
                not_valid_item,
            ],
        ),
        (
            True,
            [
                valid_item,
            ],
        ),
    ):

        results = await async_item_sql_query_manager.query_manager.where(is_valid__is=is_valid).all()

        assert len(results) == len(expected_items)

        for expected_item, result in zip(expected_items, results):
            assert result.as_dict() == expected_item.as_dict()

    for is_valid, expected_items in (
        (
            False,
            [
                valid_item,
            ],
        ),
        (
            True,
            [
                not_valid_item,
            ],
        ),
    ):
        results = await async_item_sql_query_manager.query_manager.where(is_valid__is_not=is_valid).all()

        assert len(results) == len(expected_items)

        for expected_item, result in zip(expected_items, results):
            assert result.as_dict() == expected_item.as_dict()


@pytest.mark.asyncio
async def test_async_all__filter__like_ilike__ok(
    db_session,
    async_item_sql_query_manager,
):
    first_item = models_factory.ItemFactory.create(
        name="first name",
    )
    second_item = models_factory.ItemFactory.create(name="some oThEr NaMe")

    assert db_session.query(models.Item).count() == 2

    for field, filter_value, expected_items in (
        (
            "name__like",
            "%name%",
            [
                first_item,
            ],
        ),
        (
            "name__ilike",
            "%other name%",
            [
                second_item,
            ],
        ),
    ):
        results = await async_item_sql_query_manager.query_manager.where(**{field: filter_value}).all()

        assert len(results) == len(expected_items)

        for expected_item, result in zip(expected_items, results):
            assert result.as_dict() == expected_item.as_dict()
