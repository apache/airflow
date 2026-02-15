from types import SimpleNamespace
import pytest

from airflowctl.cli.cli_config import FieldValidationError
from airflowctl.cli import dag_command
from airflowctl.api.datamodels.generated.dag_patch_body import DAGPatchBody


# --- Test pause command --- #
def test_pause_success():
    args = SimpleNamespace(dag_id="example_dag", output="json")
    # Should not raise error
    try:
        dag_command.validate_required_fields_from_model(args, DAGPatchBody)
    except FieldValidationError:
        pytest.fail("Validation failed when it should pass")


def test_pause_missing_dag_id():
    args = SimpleNamespace(output="json")  # dag_id missing
    with pytest.raises(FieldValidationError) as exc_info:
        dag_command.validate_required_fields_from_model(args, DAGPatchBody)
    assert "dag_id" in str(exc_info.value)


# --- Test unpause command --- #
def test_unpause_success():
    args = SimpleNamespace(dag_id="example_dag", output="json")
    # Should not raise error
    try:
        dag_command.validate_required_fields_from_model(args, DAGPatchBody)
    except FieldValidationError:
        pytest.fail("Validation failed when it should pass")


def test_unpause_missing_dag_id():
    args = SimpleNamespace(output="json")  # dag_id missing
    with pytest.raises(FieldValidationError) as exc_info:
        dag_command.validate_required_fields_from_model(args, DAGPatchBody)
    assert "dag_id" in str(exc_info.value)
