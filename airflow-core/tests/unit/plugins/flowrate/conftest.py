from unittest import mock
import pytest

@pytest.fixture(autouse=True)
def skip_clear_all():
    with mock.patch("tests_common.test_utils.db.clear_all"):
        yield
