import pytest


def test_c_finding_lowest_common_ancestor(table_for_merge):
    result = table_for_merge.version_graph._find_path_to_common_ancestor(2, 3)
    expected = ([1, 2], [1, 3])
    assert result == expected


def test_c_version_graph_attrs(table_with_data):
    version_count = table_with_data.version_graph.version_count
    expected = 3
    assert version_count == expected

    head = table_with_data.version_graph.head
    expected = 3
    assert head == expected
