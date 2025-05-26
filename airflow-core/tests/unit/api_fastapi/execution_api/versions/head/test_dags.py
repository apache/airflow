from unittest import mock

import pytest
from fastapi import HTTPException

from airflow.api_fastapi.execution_api.routes.dags import update_dags
from airflow.dag_processing.processor import DagFileParsingResult


@pytest.fixture
def mock_session():
    return mock.MagicMock()


@pytest.fixture
def sample_parsing_result():
    return DagFileParsingResult(
        fileloc="/path/to/dag.py",
        serialized_dags=[
            {
                "dag_id": "test_dag",
                "schedule": "@daily",
                "tasks": [],
                "timezone": "UTC",
                "catchup": False,
                "max_active_runs": 1,
                "timetable": None,
            }
        ],
        warnings=["Test warning"],
        import_errors={"test_dag": "Test error"},
    )


def test_update_dags_success(mock_session, sample_parsing_result):
    """Test successful DAG update."""
    with mock.patch("airflow.dag_processing.collection.update_dag_parsing_results_in_db") as mock_update:
        response = update_dags(
            bundle_name="test_bundle",
            bundle_version="1.0.0",
            parsing_result=sample_parsing_result,
            session=mock_session,
        )

        mock_update.assert_called_once_with(
            bundle_name="test_bundle",
            bundle_version="1.0.0",
            dags=sample_parsing_result.serialized_dags,
            import_errors=sample_parsing_result.import_errors,
            warnings=set(sample_parsing_result.warnings),
            session=mock_session,
        )
        assert response == {"message": "DAGs updated successfully"}


def test_update_dags_retry_success(mock_session, sample_parsing_result):
    """Test successful DAG update after retries."""
    with mock.patch(
        "airflow.dag_processing.collection.update_dag_parsing_results_in_db",
        side_effect=[Exception("Temporary error"), None],
    ) as mock_update:
        response = update_dags(
            bundle_name="test_bundle",
            bundle_version="1.0.0",
            parsing_result=sample_parsing_result,
            session=mock_session,
        )

        assert mock_update.call_count == 2
        assert response == {"message": "DAGs updated successfully"}


def test_update_dags_max_retries_exceeded(mock_session, sample_parsing_result):
    """Test that max retries are enforced."""
    with mock.patch(
        "airflow.dag_processing.collection.update_dag_parsing_results_in_db",
        side_effect=Exception("Persistent error"),
    ) as mock_update:
        with pytest.raises(HTTPException) as exc_info:
            update_dags(
                bundle_name="test_bundle",
                bundle_version="1.0.0",
                parsing_result=sample_parsing_result,
                session=mock_session,
            )

        assert exc_info.value.status_code == 400
        assert "Something went wrong while updating DAGs" in str(exc_info.value.detail)
        assert mock_update.call_count == 5  # Max retries reached


def test_update_dags_empty_warnings_and_errors(mock_session):
    """Test DAG update with empty warnings and errors."""
    parsing_result = DagFileParsingResult(
        fileloc="/path/to/dag.py",
        serialized_dags=[
            {
                "dag_id": "test_dag",
                "schedule": "@daily",
                "tasks": [],
                "timezone": "UTC",
                "catchup": False,
                "max_active_runs": 1,
                "timetable": None,
            }
        ],
        warnings=None,
        import_errors=None,
    )

    with mock.patch("airflow.dag_processing.collection.update_dag_parsing_results_in_db") as mock_update:
        response = update_dags(
            bundle_name="test_bundle",
            bundle_version="1.0.0",
            parsing_result=parsing_result,
            session=mock_session,
        )

        mock_update.assert_called_once_with(
            bundle_name="test_bundle",
            bundle_version="1.0.0",
            dags=parsing_result.serialized_dags,
            import_errors={},
            warnings=set(),
            session=mock_session,
        )
        assert response == {"message": "DAGs updated successfully"}


def test_update_dags_invalid_parsing_result(mock_session):
    """Test DAG update with invalid parsing result."""
    with pytest.raises(HTTPException) as exc_info:
        update_dags(
            bundle_name="test_bundle",
            bundle_version="1.0.0",
            parsing_result=None,  # Invalid parsing result
            session=mock_session,
        )

    assert exc_info.value.status_code == 400
    assert "Something went wrong while updating DAGs" in str(exc_info.value.detail)
