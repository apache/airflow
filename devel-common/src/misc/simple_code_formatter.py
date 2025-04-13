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

from pathlib import Path
from typing import Any

from datamodel_code_generator.format import CustomCodeFormatter

AIRFLOW_ROOT_PATH = Path(__file__).parents[3].resolve()


def license_text() -> str:
    license = (AIRFLOW_ROOT_PATH / "scripts" / "ci" / "license-templates" / "LICENSE.txt").read_text()
    return "\n".join(f"# {line}" if line else "#" for line in license.splitlines()) + "\n"


class CodeFormatter(CustomCodeFormatter):
    def __init__(self, formatter_kwargs: dict[str, Any]) -> None:
        super().__init__(formatter_kwargs)

        self.args = formatter_kwargs

    def apply(self, code: str) -> str:
        code = license_text() + self.args.get("ignores", "") + code
        return code
