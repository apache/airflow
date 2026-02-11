import pytest
from pydantic import BaseModel
from pydantic_evals.evaluators import EvaluatorContext
from pydantic_evals.otel.span_tree import SpanTree

from airflow.providers.common.ai.evals.llm_sql import ValidateSQL
from airflow.providers.common.ai.evals.llm_sql import build_test_case


class TaskInput(BaseModel):
    query: str


class TaskMetadata(BaseModel):
    difficulty: str = 'easy'

@pytest.fixture
def test_context() -> EvaluatorContext[TaskInput, str, TaskMetadata]:
    return EvaluatorContext[TaskInput, str, TaskMetadata](
        name='test_case',
        inputs=TaskInput(query='Select * from table'),
        output='Select * from table',
        expected_output='Select * from table',
        metadata=TaskMetadata(difficulty='easy'),
        duration=0.1,
        _span_tree=SpanTree(),
        attributes={},
        metrics={},
    )

class TestValidateSQL:

    def test_validate_sql_with_blocked_keywords_sql_valid(self, test_context):
        evaluator = ValidateSQL(BLOCKED_KEYWORDS=["DROP"])
        result = evaluator.evaluate(test_context)
        assert result.value is True
        assert result.reason == 'SQL is valid'

    def test_validate_sql_with_blocked_keywords_sql_invalid(self, test_context):
        evaluator = ValidateSQL(BLOCKED_KEYWORDS=["DROP"])
        test_context.output = "DROP TABLE table"
        result = evaluator.evaluate(test_context)
        assert result.value is False
        assert result.reason == 'SQL contains blocked keyword: DROP'

    def test_validate_sql_error_sql_parsing(self, test_context):
        evaluator = ValidateSQL()
        test_context.output = "Invalid SQL"
        result = evaluator.evaluate(test_context)
        print(result)
        assert result.value is False
        assert "Error while parsing SQL" in result.reason

def test_build_test_case():
    tc = build_test_case("Select * from table", "test_case")
    assert tc.name == "test_case"
    assert tc.inputs == "Select * from table"




