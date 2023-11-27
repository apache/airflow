#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.papermill.hooks.kernel import (
    JUPYTER_KERNEL_CONTROL_PORT,
    JUPYTER_KERNEL_HB_PORT,
    JUPYTER_KERNEL_IOPUB_PORT,
    JUPYTER_KERNEL_SHELL_PORT,
    JUPYTER_KERNEL_STDIN_PORT,
)
from airflow.providers.papermill.operators.papermill import REMOTE_KERNEL_ENGINE, NoteBook, PapermillOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2021, 1, 1)
TEST_INPUT_URL = "/foo/bar"
TEST_OUTPUT_URL = "/spam/egg"


class TestNoteBook:
    """Test NoteBook object."""

    def test_templated_fields(self):
        assert hasattr(NoteBook, "template_fields")
        assert "parameters" in NoteBook.template_fields


class TestPapermillOperator:
    """Test PapermillOperator."""

    def test_mandatory_attributes(self):
        """Test missing Input or Output notebooks."""
        with pytest.raises(ValueError, match="Input notebook is not specified"):
            PapermillOperator(task_id="missing_input_nb", output_nb="foo-bar")

        with pytest.raises(ValueError, match="Output notebook is not specified"):
            PapermillOperator(task_id="missing_input_nb", input_nb="foo-bar")

    @pytest.mark.parametrize(
        "output_nb",
        [
            pytest.param(TEST_OUTPUT_URL, id="output-as-string"),
            pytest.param(NoteBook(TEST_OUTPUT_URL), id="output-as-notebook-object"),
        ],
    )
    @pytest.mark.parametrize(
        "input_nb",
        [
            pytest.param(TEST_INPUT_URL, id="input-as-string"),
            pytest.param(NoteBook(TEST_INPUT_URL), id="input-as-notebook-object"),
        ],
    )
    def test_notebooks_objects(self, input_nb, output_nb):
        """Test different type of Input/Output notebooks arguments."""
        op = PapermillOperator(task_id="test_notebooks_objects", input_nb=input_nb, output_nb=output_nb)
        assert op.input_nb.url == TEST_INPUT_URL
        assert op.output_nb.url == TEST_OUTPUT_URL

    @patch("airflow.providers.papermill.operators.papermill.pm")
    def test_execute(self, mock_papermill):
        in_nb = "/tmp/does_not_exist"
        out_nb = "/tmp/will_not_exist"
        kernel_name = "python3"
        language_name = "python"
        parameters = {"msg": "hello_world", "train": 1}

        op = PapermillOperator(
            input_nb=in_nb,
            output_nb=out_nb,
            parameters=parameters,
            task_id="papermill_operator_test",
            kernel_name=kernel_name,
            language_name=language_name,
            dag=None,
        )

        op.execute(context={})

        mock_papermill.execute_notebook.assert_called_once_with(
            in_nb,
            out_nb,
            parameters=parameters,
            kernel_name=kernel_name,
            language=language_name,
            progress_bar=False,
            report_mode=True,
            engine_name=None,
        )

    @patch("airflow.providers.papermill.hooks.kernel.KernelHook.get_connection")
    @patch("airflow.providers.papermill.operators.papermill.pm")
    def test_execute_remote_kernel(self, mock_papermill, kernel_hook):
        in_nb = "/tmp/does_not_exist"
        out_nb = "/tmp/will_not_exist"
        kernel_name = "python3"
        language_name = "python"
        parameters = {"msg": "hello_world", "train": 1}
        conn = MagicMock()
        conn.host = "127.0.0.1"
        conn.extra_dejson = {"session_key": "notebooks"}
        kernel_hook.return_value = conn

        op = PapermillOperator(
            input_nb=in_nb,
            output_nb=out_nb,
            parameters=parameters,
            task_id="papermill_operator_test",
            kernel_name=kernel_name,
            language_name=language_name,
            kernel_conn_id="jupyter_kernel_default",
            dag=None,
        )

        op.execute(context={})

        mock_papermill.execute_notebook.assert_called_once_with(
            in_nb,
            out_nb,
            parameters=parameters,
            kernel_name=kernel_name,
            language=language_name,
            progress_bar=False,
            report_mode=True,
            engine_name=REMOTE_KERNEL_ENGINE,
            kernel_session_key="notebooks",
            kernel_shell_port=JUPYTER_KERNEL_SHELL_PORT,
            kernel_iopub_port=JUPYTER_KERNEL_IOPUB_PORT,
            kernel_stdin_port=JUPYTER_KERNEL_STDIN_PORT,
            kernel_control_port=JUPYTER_KERNEL_CONTROL_PORT,
            kernel_hb_port=JUPYTER_KERNEL_HB_PORT,
            kernel_ip="127.0.0.1",
        )

    @pytest.mark.db_test
    def test_render_template(self, create_task_instance_of_operator):
        """Test rendering fields."""
        ti = create_task_instance_of_operator(
            PapermillOperator,
            input_nb="/tmp/{{ dag.dag_id }}.ipynb",
            output_nb="/tmp/out-{{ dag.dag_id }}.ipynb",
            parameters={"msgs": "dag id is {{ dag.dag_id }}!", "test_dt": "{{ ds }}"},
            kernel_name="{{ params.kernel_name }}",
            language_name="{{ params.language_name }}",
            # Additional parameters for render fields
            params={
                "kernel_name": "python3",
                "language_name": "python",
            },
            # TI Settings
            dag_id="test_render_template",
            task_id="render_dag_test",
            execution_date=DEFAULT_DATE,
        )
        task = ti.render_templates()

        # Test render Input/Output notebook attributes
        assert task.input_nb.url == "/tmp/test_render_template.ipynb"
        assert task.input_nb.parameters == {
            "msgs": "dag id is test_render_template!",
            "test_dt": DEFAULT_DATE.date().isoformat(),
        }
        assert task.output_nb.url == "/tmp/out-test_render_template.ipynb"
        assert task.output_nb.parameters == {}

        # Test render other templated attributes
        assert task.parameters == task.input_nb.parameters
        assert "python3" == task.kernel_name
        assert "python" == task.language_name

        # Test render Lineage inlets/outlets
        assert task.inlets[0] == task.input_nb
        assert task.outlets[0] == task.output_nb
