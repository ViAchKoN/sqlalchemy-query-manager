from tests import models, models_factory


def test_inner_join__only__star__ok(
    db_session,
    item_sql_query_manager,
):
    expected_names = [
        ("expected_item_name_1", "expected_group_name_1"),
        ("expected_item_name_2", "expected_group_name_2"),
        ("expected_item_name_3", "expected_group_name_3"),
    ]

    for item_name, group_name in expected_names:
        models_factory.ItemFactory.create(
            name=item_name,
            group=models_factory.GroupFactory.create(
                name=group_name,
                owner=None,
            ),
        )

    assert db_session.query(models.Item).count() == 3

    results = item_sql_query_manager.query_manager.inner_join("group").only("*").all()

    assert len(results) == len(expected_names)

    for expected_name, result in zip(expected_names, results):
        assert result.name == expected_name[0]
        assert result.name_1 == expected_name[1]


def test_inner_join__nested__only_star__ok(
    db_session,
    item_sql_query_manager,
):
    expected_names = [
        ("expected_item_name_1", "expected_group_name_1", "expected_owner_name_1"),
        ("expected_item_name_2", "expected_group_name_2", "expected_owner_name_2"),
        ("expected_item_name_3", "expected_group_name_3", "expected_owner_name_3"),
    ]

    for item_name, group_name, owner_name in expected_names:
        models_factory.ItemFactory.create(
            name=item_name,
            group=models_factory.GroupFactory.create(
                name=group_name,
                owner__first_name=owner_name,
            ),
        )

    assert db_session.query(models.Item).count() == 3

    results = (
        item_sql_query_manager.query_manager.inner_join("group", "group__owner")
        .only("*")
        .all()
    )

    assert len(results) == len(expected_names)

    for expected_name, result in zip(expected_names, results):
        assert result.name == expected_name[0]
        assert result.name_1 == expected_name[1]
        assert result.first_name == expected_name[2]
