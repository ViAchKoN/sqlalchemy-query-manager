from tests import models, models_factory


def test_left_join__only__star__ok(
    db_session,
    item_sql_query_manager,
):
    expected_names_with_group = [
        ("expected_item_name_1", "expected_group_name_1"),
        ("expected_item_name_2", "expected_group_name_2"),
    ]

    expected_names_without_group = [
        ("expected_item_name_3", None),
    ]

    expected_names_without_item = [
        (None, "expected_group_name_4"),
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

    models_factory.GroupFactory.create(name=expected_names_without_item[0][1])

    expected_names += expected_names_without_item

    assert db_session.query(models.Item).count() == 3

    results = item_sql_query_manager.query_manager.full_join("group").only("*").all()

    assert len(results) == len(expected_names)

    for expected_name, result in zip(expected_names, results):
        assert result.name == expected_name[0]
        assert result.name_1 == expected_name[1]
