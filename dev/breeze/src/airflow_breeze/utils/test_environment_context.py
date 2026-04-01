from airflow_breeze.utils.environment_context import detect_environment_context


def test_detect_host_environment():
    context = detect_environment_context()
    assert context.recommended_command in ["breeze shell", "pytest"]