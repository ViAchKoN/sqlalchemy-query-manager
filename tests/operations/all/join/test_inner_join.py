#
#
#
# def test_inner_join__ok(
#     db_session,
#     item_sql_query_manager,
# ):
#     expected_item_names = [
#         "expected_item_name_1",
#         "expected_item_name_2",
#         "expected_item_name_3",
#     ]
#
#     not_expected_item_name = "not_expected_item_name"
#
#     expected_items = []
#     for item_name in [*expected_item_names, not_expected_item_name]:
#         expected_items.append(
#             models_factory.ItemFactory.create(
#                 name=item_name,
#             )
#         )
#
#     assert db_session.query(models.Item).count() == 4
#
#     results = item_sql_query_manager.query_manager.where(
#         name__not=not_expected_item_name
#     ).all()
#
#     assert len(results) == len(expected_item_names)
#
#     for expected_item, result in zip(expected_items, results):
#         assert result.as_dict() == expected_item.as_dict()
