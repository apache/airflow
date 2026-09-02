import pytest
from airflow.providers.google.cloud.utils.bigquery import bq_cast, convert_job_id

class TestBqCast:
    def test_bq_cast_none(self):
        """bq_cast returns None for a None input regardless of the declared type."""
        assert bq_cast(None, "INTEGER") is None

    def test_bq_cast_integer_and_float(self):
        """INTEGER casts to int, and FLOAT casts to float."""
        assert bq_cast("123", "INTEGER") == 123
        assert bq_cast("123.45", "FLOAT") == 123.45

    def test_bq_cast_timestamp_returns_float(self):
        """TIMESTAMP also casts to float (pinned behavior)."""
        assert bq_cast("123.45", "TIMESTAMP") == 123.45

    @pytest.mark.parametrize("value, expected", [("true", True), ("false", False)])
    def test_bq_cast_boolean_valid(self, value, expected):
        """BOOLEAN maps the strings true and false to real booleans."""
        assert bq_cast(value, "BOOLEAN") is expected

    def test_bq_cast_boolean_invalid_raises_value_error(self):
        """BOOLEAN raises ValueError for anything else with the expected message."""
        with pytest.raises(ValueError, match="invalid must have value 'true' or 'false'"):
            bq_cast("invalid", "BOOLEAN")

    def test_bq_cast_unrecognized_type(self):
        """An unrecognised type falls through and returns the string unchanged."""
        assert bq_cast("raw_string", "UNKNOWN_TYPE") == "raw_string"


class TestConvertJobId:
    def test_convert_single_job_id(self):
        """Builds project_id:location:job_id for a single id."""
        result = convert_job_id(job_id="job123", project_id="proj", location="EU")
        assert result == "proj:EU:job123"

    def test_convert_job_id_defaults_to_us(self):
        """Defaults location to US when it is None."""
        result = convert_job_id(job_id="job123", project_id="proj", location=None)
        assert result == "proj:US:job123"

    def test_convert_list_of_job_ids(self):
        """Maps over the list form and returns a list."""
        result = convert_job_id(job_id=["job1", "job2"], project_id="proj", location="EU")
        assert result == ["proj:EU:job1", "proj:EU:job2"]
