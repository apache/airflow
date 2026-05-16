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

import os
from pathlib import Path

from sphinx.util import logging

from airflow_shared.exceptions.common import get_error_meta_list

AIRFLOW_ROOT_PATH = Path(os.path.abspath(__file__)).parents[3]
GENERATED_PATH = AIRFLOW_ROOT_PATH / "airflow-core" / "docs" / "error-codes"
AIRFLOW_CORE_SRC_PATH = AIRFLOW_ROOT_PATH / "airflow-core" / "src"
TASK_SDK_SRC_PATH = AIRFLOW_ROOT_PATH / "task-sdk" / "src"
PROVIDERS_SRC_PATH = AIRFLOW_ROOT_PATH / "providers"
EXCEPTION_CLASS_PATHS = [AIRFLOW_CORE_SRC_PATH, TASK_SDK_SRC_PATH, PROVIDERS_SRC_PATH]


def get_license_header():
    return """
    .. Licensed to the Apache Software Foundation (ASF) under one
        or more contributor license agreements.  See the NOTICE file
        distributed with this work for additional information
        regarding copyright ownership.  The ASF licenses this file
        to you under the Apache License, Version 2.0 (the
        "License"); you may not use this file except in compliance
        with the License.  You may obtain a copy of the License at

    ..   http://www.apache.org/licenses/LICENSE-2.0

    .. Unless required by applicable law or agreed to in writing,
        software distributed under the License is distributed on an
        "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
        KIND, either express or implied.  See the License for the
        specific language governing permissions and limitations
        under the License.
    """


class ErrorGuideIndexHandler:
    """File handler class for error guide index file."""

    def __init__(self, file_path):
        self.file_path = file_path
        self.lines = [
            get_license_header(),
            "",
            "Error Codes Guide",
            "=================",
            "",
            "Quick debug guide for common problems when using Airflow, with error code as a starting point. "
            "In your Airflow logs, look for the error code in beginning of exception traceback. "
            "e.g: ``[AERR-TIMETABLE-INVALID] schedule interval must be positive...``",
            "",
            "List of documented error codes",
            "------------------------------",
            "",
        ]
        self._toc_tree_lines = [
            ".. toctree::",
            "   :maxdepth: 1",
            "",
        ]
        self.error_codes = []

    def add_error_code(self, error_code):
        self.error_codes.append(error_code)

    def commit(self):
        if self.error_codes:
            self.lines += self._toc_tree_lines + [f"   {error_code}" for error_code in self.error_codes]
        else:
            self.lines.append(
                "There are no error codes listed as of now. If you'd like a new error code listed, "
                "please raise an issue on `GitHub <https://github.com/apache/airflow/issues>`_."
            )
        self.lines.append("")
        self.file_path.write_text(
            "\n".join(self.lines),
            encoding="utf-8",
        )


class ErrorGuideDocHandler:
    """File handler class for error code rst file."""

    def __init__(self, file_path):
        self.file_path = file_path
        self.lines = []

    def prepare(self, error_code_info: dict[str, str]):
        code = error_code_info["error_code"]
        title = f"{code}: {error_code_info['user_facing_error_message']}"
        underline = "=" * len(title)
        self.lines = [
            get_license_header(),
            "",
            title,
            underline,
            "",
            f"**Exception:** ``{error_code_info['exception_type']}``",
            "",
            "Description",
            "-----------",
            error_code_info["description"],
            "",
            "First Steps",
            "-----------",
            error_code_info["first_steps"],
            "",
            "Documentation to refer",
            "-----------------------",
            error_code_info["documentation"],
            "",
        ]

    def commit(self):
        self.file_path.write_text(
            "\n".join(self.lines) + "\n",
            encoding="utf-8",
        )


def generate_error_docs(app):
    """Generates .rst files for each error code in the YAML mapping."""

    GENERATED_PATH.mkdir(parents=True, exist_ok=True)
    index_handler = ErrorGuideIndexHandler(GENERATED_PATH / "index.rst")
    logger = logging.getLogger(__name__)

    error_list: list[dict[str, str]] = get_error_meta_list(EXCEPTION_CLASS_PATHS)
    if not error_list:
        logger.error("Error mapping is empty.")
        index_handler.commit()
        return

    for entry in error_list:
        code = entry["error_code"]

        # add error code to index (toctree) page
        index_handler.add_error_code(code)

        # prep and commit error page doc to disk
        error_doc = ErrorGuideDocHandler(GENERATED_PATH / f"{code}.rst")
        error_doc.prepare(entry)
        error_doc.commit()

    index_handler.commit()


def setup(app):
    app.connect("builder-inited", generate_error_docs)
