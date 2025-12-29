def test_serialize_template_field_with_very_small_max_length(monkeypatch):
    monkeypatch.setenv("AIRFLOW__CORE__MAX_TEMPLATED_FIELD_LENGTH", "1")

    from airflow.serialization.helpers import serialize_template_field

    result = serialize_template_field("This is a long string", "test")

    assert result
    assert len(result) <= 1
