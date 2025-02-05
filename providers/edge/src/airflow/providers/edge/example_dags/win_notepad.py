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
"""
In this DAG is a demonstrator how to interact with a Windows Worker via Notepad.

The DAG is created in conjunction with the documentation in
https://github.com/apache/airflow/blob/main/docs/apache-airflow-providers-edge/install_on_windows.rst
and serves as a PoC test for the Windows worker.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from subprocess import check_call
from tempfile import gettempdir
from typing import TYPE_CHECKING, Any

from airflow.models import BaseOperator
from airflow.models.dag import DAG
from airflow.sdk import Param

if TYPE_CHECKING:
    from airflow.utils.context import Context


class NotepadOperator(BaseOperator):
    """Example Operator Implementation which starts a Notepod.exe on WIndows."""

    template_fields: Sequence[str] = "text"

    def __init__(self, text: str, **kwargs):
        self.text = text
        super().__init__(**kwargs)

    def execute(self, context: Context) -> Any:
        tmp_file = Path(gettempdir()) / "airflow_test.txt"
        with open(tmp_file, "w", encoding="utf8") as textfile:
            textfile.write(self.text)
        check_call(["notepad.exe", tmp_file])
        with open(tmp_file, encoding="utf8") as textfile:
            return textfile.read()


with DAG(
    dag_id="win_notepad",
    dag_display_name="Windows Notepad",
    description=__doc__.partition(".")[0],
    doc_md=__doc__,
    schedule=None,
    start_date=datetime(2024, 7, 1),
    tags=["edge", "Windows"],
    default_args={"queue": "windows"},
    params={
        "notepad_text": Param(
            "This is a text as proposal generated by Airflow DAG. Change it and save and it will get to XCom.",
            title="Notepad Text",
            description="Add some text that should be filled into Notepad at start.",
            type="string",
            format="multiline",
        ),
    },
) as dag:
    npo = NotepadOperator(
        task_id="notepad",
        text="{{ params.notepad_text }}",
    )
