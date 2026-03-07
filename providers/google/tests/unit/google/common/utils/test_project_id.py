import pytest

from airflow.providers.google.common.utils import is_valid_gcp_project_id


@pytest.mark.parametrize(
    "project_id",
    [
        "abcde1",
        "a12345",
        "myproject123",
        "a" + "b" * 4,
        "a" + "b" * 28,
    ],
)
def test_is_valid_gcp_project_id_valid(project_id):
    assert is_valid_gcp_project_id(project_id)


@pytest.mark.parametrize(
    "project_id",
    [
        "Abcde1",
        "abc",
        "a" * 31,
        "abcde-",
        "abc_de",
        "1abcde",
        "",
        "     ",
    ],
)
def test_is_valid_gcp_project_id_invalid(project_id):
    assert not is_valid_gcp_project_id(project_id)
