import pytest
from unittest.mock import MagicMock, patch

from airflow.providers.google.cloud.operators.gen_ai import (
    GenAIGenerateContentOperator,
    GenAIGenerateEmbeddingsOperator,
    GenAICountTokensOperator,
)


# ---------------------------
# Fixtures
# ---------------------------

@pytest.fixture
def mock_context():
    return {"ti": MagicMock()}


# ---------------------------
# Execution Tests
# ---------------------------

@patch("airflow.providers.google.cloud.operators.gen_ai.GenAIGenerativeModelHook")
def test_generate_embeddings_execute(mock_hook_class, mock_context):
    mock_hook = MagicMock()
    mock_hook.embed_content.return_value = {"embedding": [0.1, 0.2]}
    mock_hook_class.return_value = mock_hook

    op = GenAIGenerateEmbeddingsOperator(
        task_id="test",
        project_id="test-project",
        location="us-central1",
        model="gemini-pro",
        contents=["hello"],
    )

    result = op.execute(mock_context)

    assert result == {"embedding": [0.1, 0.2]}


@patch("airflow.providers.google.cloud.operators.gen_ai.GenAIGenerativeModelHook")
def test_generate_content_execute(mock_hook_class, mock_context):
    mock_hook = MagicMock()
    mock_hook.generate_content.return_value = {"text": "Hello world"}
    mock_hook_class.return_value = mock_hook

    op = GenAIGenerateContentOperator(
        task_id="test",
        project_id="test-project",
        location="us-central1",
        model="gemini-pro",
        contents={"text": "Hello"},
    )

    result = op.execute(mock_context)

    assert result == {"text": "Hello world"}


@patch("airflow.providers.google.cloud.operators.gen_ai.GenAIGenerativeModelHook")
def test_count_tokens_execute(mock_hook_class, mock_context):
    mock_hook = MagicMock()
    mock_response = MagicMock()
    mock_response.total_tokens = 42
    mock_hook.count_tokens.return_value = mock_response
    mock_hook_class.return_value = mock_hook

    op = GenAICountTokensOperator(
        task_id="test",
        project_id="test-project",
        location="us-central1",
        model="gemini-pro",
        contents={"text": "Hello"},
    )

    op.execute(mock_context)

    mock_context["ti"].xcom_push.assert_called_once_with(
        key="total_tokens", value=42
    )


# ---------------------------
# Exception Tests
# ---------------------------

def test_generate_embeddings_missing_project_id():
    with pytest.raises(ValueError):
        GenAIGenerateEmbeddingsOperator(
            task_id="test",
            location="us-central1",
            contents=["hello"],
            model="gemini-pro",
        )


def test_generate_embeddings_missing_location():
    with pytest.raises(ValueError):
        GenAIGenerateEmbeddingsOperator(
            task_id="test",
            project_id="test-project",
            contents=["hello"],
            model="gemini-pro",
        )


def test_generate_content_missing_project_id():
    with pytest.raises(ValueError):
        GenAIGenerateContentOperator(
            task_id="test",
            location="us-central1",
            contents={"text": "Hello"},
            model="gemini-pro",
        )


def test_count_tokens_missing_location():
    with pytest.raises(ValueError):
        GenAICountTokensOperator(
            task_id="test",
            project_id="test-project",
            contents={"text": "Hello"},
            model="gemini-pro",
        )