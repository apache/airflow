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

import pytest
from check_example_dag_shell_commands import check_file


class TestCheckFilePasses:
    """Constant command text, with runtime values passed through ``env``, produces no errors."""

    @pytest.mark.parametrize(
        "code",
        [
            pytest.param('BashOperator(task_id="t", bash_command="echo hello")\n', id="literal"),
            pytest.param(
                'BashOperator(task_id="t", bash_command=\'echo "$X"\', env={"X": result["x"]})\n',
                id="value-through-env",
            ),
            pytest.param(
                'BashOperator(task_id="t", bash_command=\'echo "$X"\', env={"X": "{{ params.x }}"})\n',
                id="params-template-through-env",
            ),
            pytest.param('BashOperator(task_id="t", bash_command="echo {{ ds }}")\n', id="context-template"),
            pytest.param(
                'CMD = "echo hi"\nBashOperator(task_id="t", bash_command=CMD)\n',
                id="module-constant",
            ),
            pytest.param(
                'BASE = "/tmp"\nBashOperator(task_id="t", bash_command=f"ls {BASE}")\n',
                id="f-string-of-upper-case-constant",
            ),
            pytest.param(
                "import textwrap\n"
                "def f():\n"
                '    cmd = textwrap.dedent("""\n        echo {{ ds }}\n    """)\n'
                '    BashOperator(task_id="t", bash_command=cmd)\n',
                id="local-dedented-literal",
            ),
            pytest.param(
                '@task.bash\ndef run(seconds):\n    return f"sleep {seconds}"\nrun(seconds=3)\n',
                id="task-bash-fed-a-literal",
            ),
            pytest.param(
                '@task.bash\ndef run(ti_key):\n    return f"echo {ti_key}"\nrun()\n',
                id="task-bash-context-value",
            ),
        ],
    )
    def test_no_errors(self, write_python_file, code: str):
        assert check_file(write_python_file(code)) == []


class TestCheckFileFails:
    """A shell command built from a runtime value produces exactly one error."""

    @pytest.mark.parametrize(
        ("code", "expected"),
        [
            pytest.param(
                'BashOperator(task_id="t", bash_command=result["command"])\n',
                "command comes from",
                id="task-output-subscript",
            ),
            pytest.param(
                'BashOperator(task_id="t", bash_command=f"echo {ip}")\n',
                "f-string interpolates 'ip'",
                id="f-string-of-runtime-value",
            ),
            pytest.param(
                'BashOperator(task_id="t", bash_command="echo {}".format(ip))\n',
                "str.format",
                id="str-format",
            ),
            pytest.param(
                'BashOperator(task_id="t", bash_command="echo %s" % ip)\n',
                "concatenation or % formatting",
                id="percent-format",
            ),
            pytest.param(
                'BashOperator(task_id="t", bash_command="echo {{ params.x }}")\n',
                "renders params",
                id="params-template-in-command",
            ),
            pytest.param(
                'BashOperator(task_id="t", bash_command="echo {{ ti.xcom_pull(task_ids=\'a\') }}")\n',
                "renders xcom_pull",
                id="xcom-template-in-command",
            ),
            pytest.param(
                'BashOperator(task_id="t", bash_command="echo {{ dag_run.conf[\'x\'] }}")\n',
                "renders dag_run.conf",
                id="conf-template-in-command",
            ),
            pytest.param(
                'CMD = "echo {{ params.x }}"\nBashOperator(task_id="t", bash_command=CMD)\n',
                "renders params",
                id="params-template-through-constant",
            ),
            pytest.param(
                '@task.bash\ndef show(folder):\n    return f"echo {folder}"\nshow(folder=stats["folder"])\n',
                "interpolates folder",
                id="task-bash-fed-task-output",
            ),
            pytest.param(
                "@task.bash\n"
                "def show(folder):\n"
                '    return f"echo {folder}"\n'
                'show.override(task_id="x")(folder=stats["folder"])\n',
                "interpolates folder",
                id="task-bash-override-fed-task-output",
            ),
            pytest.param(
                '@task.bash\ndef show():\n    return "echo {{ params.x }}"\nshow()\n',
                "renders params",
                id="task-bash-returns-params-template",
            ),
        ],
    )
    def test_single_error(self, write_python_file, code: str, expected: str):
        errors = check_file(write_python_file(code))
        assert len(errors) == 1
        assert expected in errors[0]
