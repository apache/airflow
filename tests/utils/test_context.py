import pytest

from airflow.exceptions import DeserializingResultError
from airflow.utils.context import AirflowContextDeprecationWarning, suppress_and_warn


def method_which_raises_an_airflow_deprecation_warning():
    raise AirflowContextDeprecationWarning()


class TestContext:
    def test_suppress_and_warn_when_raised_exception_is_suppressed(self):
        with suppress_and_warn(AirflowContextDeprecationWarning):
            method_which_raises_an_airflow_deprecation_warning()

    def test_suppress_and_warn_when_raised_exception_is_not_suppressed(self):
        with pytest.raises(AirflowContextDeprecationWarning):
            with suppress_and_warn(DeserializingResultError):
                method_which_raises_an_airflow_deprecation_warning()
