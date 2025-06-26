from tests import models, models_factory


def test_left_join__only__star__ok(
    db_session,
    item_sql_query_manager,
):
    expected_names_with_group = [
        ("expected_item_name_1", "expected_group_name_1"),
        ("expected_item_name_2", "expected_group_name_2"),
        ("expected_item_name_3", "expected_group_name_3"),
    ]

    expected_names_without_group = [
        ("expected_item_name_4", None),
        ("expected_item_name_5", None),
        ("expected_item_name_6", None),
    ]

    expected_names = expected_names_with_group + expected_names_without_group

    for item_name, group_name in expected_names:
        models_factory.ItemFactory.create(
            name=item_name,
            group=(
                models_factory.GroupFactory.create(
                    name=group_name,
                    owner=None,
                )
                if group_name
                else None
            ),
        )

    assert db_session.query(models.Item).count() == 6

    results = item_sql_query_manager.query_manager.left_join("group").only("*").all()

    assert len(results) == len(expected_names)

    for expected_name, result in zip(expected_names, results):
        assert result.name == expected_name[0]
        assert result.name_1 == expected_name[1]


def test_left_join__nested__only__star__ok(
    db_session,
    item_sql_query_manager,
):
    expected_names_with_group_owner = [
        ("expected_item_name_1", "expected_group_name_1", "expected_owner_name_1"),
        ("expected_item_name_2", "expected_group_name_2", "expected_owner_name_2"),
        ("expected_item_name_3", "expected_group_name_3", "expected_owner_name_3"),
    ]

    expected_names_without_group = [
        ("expected_item_name_4", None, None),
        ("expected_item_name_5", None, None),
        ("expected_item_name_6", None, None),
    ]

    expected_names_without_owner = [
        ("expected_item_name_7", "expected_group_name_7", None),
        ("expected_item_name_8", "expected_group_name_8", None),
        ("expected_item_name_9", "expected_group_name_9", None),
    ]

    expected_names = (
        expected_names_with_group_owner
        + expected_names_without_group
        + expected_names_without_owner
    )

    for item_name, group_name, owner_name in expected_names:
        models_factory.ItemFactory.create(
            name=item_name,
            group=(
                models_factory.GroupFactory.create(
                    name=group_name,
                    **(
                        {"owner__first_name": owner_name}
                        if owner_name
                        else {"owner": None}
                    ),
                )
                if group_name
                else None
            ),
        )

    assert db_session.query(models.Item).count() == 9

    results = (
        item_sql_query_manager.query_manager.left_join("group__owner").only("*").all()
    )

    assert len(results) == len(expected_names)

    for expected_name, result in zip(expected_names, results):
        assert result.name == expected_name[0]
        assert result.name_1 == expected_name[1]
        assert result.first_name == expected_name[2]


def test_left_join__chain_nested__only__star__ok(
    db_session,
    item_sql_query_manager,
):
    expected_names_with_group_owner = [
        ("expected_item_name_1", "expected_group_name_1", "expected_owner_name_1"),
        ("expected_item_name_2", "expected_group_name_2", "expected_owner_name_2"),
        ("expected_item_name_3", "expected_group_name_3", "expected_owner_name_3"),
    ]

    expected_names_without_group = [
        ("expected_item_name_4", "expected_group_name_4", None),
        ("expected_item_name_5", "expected_group_name_5", None),
        ("expected_item_name_6", "expected_group_name_6", None),
    ]

    expected_names_without_owner = [
        ("expected_item_name_7", None, "expected_owner_name_7"),
        ("expected_item_name_8", None, "expected_owner_name_8"),
        ("expected_item_name_9", None, "expected_owner_name_9"),
    ]

    expected_names = expected_names_with_group_owner + expected_names_without_group

    for item_name, group_name, owner_name in (
        expected_names + expected_names_without_owner
    ):
        models_factory.ItemFactory.create(
            name=item_name,
            group=(
                models_factory.GroupFactory.create(
                    name=group_name,
                    **(
                        {"owner__first_name": owner_name}
                        if owner_name
                        else {"owner": None}
                    ),
                )
                if group_name
                else None
            ),
        )

    assert db_session.query(models.Item).count() == 9

    results = (
        item_sql_query_manager.query_manager.inner_join("group")
        .left_join("group__owner")
        .only("*")
        .all()
    )

    assert len(results) == len(expected_names)

    for expected_name, result in zip(expected_names, results):
        assert result.name == expected_name[0]
        assert result.name_1 == expected_name[1]
        assert result.first_name == expected_name[2]
