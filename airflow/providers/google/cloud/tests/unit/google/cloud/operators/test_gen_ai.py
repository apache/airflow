from __future__ import annotations

from unittest import mock

import pytest
from google.genai.errors import ClientError

from airflow.providers.google.cloud.operators.gen_ai import (
    GenAICountTokensOperator,
)

GEN_AI_PATH = "airflow.providers.google.cloud.operators.gen_ai.{}"

TASK_ID = "test_task_id"
GCP_PROJECT = "test-project"
GCP_LOCATION = "test-location"
GCP_CONN_ID = "test-conn"
IMPERSONATION_CHAIN = ["ACCOUNT_1"]

CONTENTS = ["Test content"]
MODEL = "gemini-pro"


class TestGenAICountTokensOperator:
    @mock.patch(GEN_AI_PATH.format("GenAIGenerativeModelHook"))
    def test_execute(self, mock_hook):
        op = GenAICountTokensOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            contents=CONTENTS,
            model=MODEL,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        op.execute(context={"ti": mock.MagicMock()})

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_hook.return_value.count_tokens.assert_called_once()

    @mock.patch(GEN_AI_PATH.format("GenAIGenerativeModelHook"))
    def test_execute_propagates_client_error(self, mock_hook):
        mock_hook.return_value.count_tokens.side_effect = ClientError.__new__(ClientError)

        op = GenAICountTokensOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            contents=CONTENTS,
            model=MODEL,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        with pytest.raises(ClientError):
            op.execute(context={"ti": mock.MagicMock()})