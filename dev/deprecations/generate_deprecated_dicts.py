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

import ast
import os
from collections import defaultdict
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, NamedTuple

from jinja2 import BaseLoader, Environment
from rich.console import Console

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module_path."
        f"To run this script, run the ./{__file__} command [FILE] ..."
    )

AIRFLOW_SOURCES_ROOT = Path(__file__).parents[2].resolve()
CONTRIB_DIR = AIRFLOW_SOURCES_ROOT / "airflow" / "contrib"


@lru_cache(maxsize=None)
def black_mode():
    from black import Mode, parse_pyproject_toml, target_version_option_callback

    config = parse_pyproject_toml(os.path.join(AIRFLOW_SOURCES_ROOT, "pyproject.toml"))

    target_versions = set(
        target_version_option_callback(None, None, tuple(config.get('target_version', ()))),
    )

    return Mode(
        target_versions=target_versions,
        line_length=config.get('line_length', Mode.line_length),
        is_pyi=bool(config.get('is_pyi', Mode.is_pyi)),
        string_normalization=not bool(config.get('skip_string_normalization', not Mode.string_normalization)),
        experimental_string_processing=bool(
            config.get('experimental_string_processing', Mode.experimental_string_processing)
        ),
    )


def black_format(content) -> str:
    from black import format_str

    return format_str(content, mode=black_mode())


class Import(NamedTuple):
    module_path: str
    name: str
    alias: str


class ImportedClass(NamedTuple):
    module_path: str
    name: str


def get_imports(path: Path):
    root = ast.parse(path.read_text())
    imports: Dict[str, ImportedClass] = {}
    for node in ast.iter_child_nodes(root):
        if isinstance(node, ast.Import):
            module_array: List[str] = []
        elif isinstance(node, ast.ImportFrom) and node.module:
            module_array = node.module.split('.')
        elif isinstance(node, ast.ClassDef):
            for base in node.bases:
                res = imports.get(base.id)  # type: ignore[attr-defined]
                if res:
                    yield Import(module_path=res.module_path, name=res.name, alias=node.name)
            continue
        else:
            continue
        for n in node.names:  # type: ignore[attr-defined]
            imported_as = n.asname if n.asname else n.name
            module_path = ".".join(module_array)
            imports[imported_as] = ImportedClass(module_path=module_path, name=n.name)
            yield Import(module_path, n.name, imported_as)


DEPRECATED_CLASSES_TEMPLATE = """
__deprecated_classes = {
{%- for module_path, package_imports in package_imports.items() %}
    '{{module_path}}': {
{%- for import_item in package_imports %}
        '{{import_item.alias}}': '{{import_item.module_path}}.{{import_item.name}}',
{%- endfor %}
    },
{%- endfor %}
}
"""

DEPRECATED_MODULES = [
    'airflow/hooks/base_hook.py',
    'airflow/hooks/dbapi_hook.py',
    'airflow/hooks/docker_hook.py',
    'airflow/hooks/druid_hook.py',
    'airflow/hooks/hdfs_hook.py',
    'airflow/hooks/hive_hooks.py',
    'airflow/hooks/http_hook.py',
    'airflow/hooks/jdbc_hook.py',
    'airflow/hooks/mssql_hook.py',
    'airflow/hooks/mysql_hook.py',
    'airflow/hooks/oracle_hook.py',
    'airflow/hooks/pig_hook.py',
    'airflow/hooks/postgres_hook.py',
    'airflow/hooks/presto_hook.py',
    'airflow/hooks/S3_hook.py',
    'airflow/hooks/samba_hook.py',
    'airflow/hooks/slack_hook.py',
    'airflow/hooks/sqlite_hook.py',
    'airflow/hooks/webhdfs_hook.py',
    'airflow/hooks/zendesk_hook.py',
    'airflow/operators/bash_operator.py',
    'airflow/operators/branch_operator.py',
    'airflow/operators/check_operator.py',
    'airflow/operators/dagrun_operator.py',
    'airflow/operators/docker_operator.py',
    'airflow/operators/druid_check_operator.py',
    'airflow/operators/dummy.py',
    'airflow/operators/dummy_operator.py',
    'airflow/operators/email_operator.py',
    'airflow/operators/gcs_to_s3.py',
    'airflow/operators/google_api_to_s3_transfer.py',
    'airflow/operators/hive_operator.py',
    'airflow/operators/hive_stats_operator.py',
    'airflow/operators/hive_to_druid.py',
    'airflow/operators/hive_to_mysql.py',
    'airflow/operators/hive_to_samba_operator.py',
    'airflow/operators/http_operator.py',
    'airflow/operators/jdbc_operator.py',
    'airflow/operators/latest_only_operator.py',
    'airflow/operators/mssql_operator.py',
    'airflow/operators/mssql_to_hive.py',
    'airflow/operators/mysql_operator.py',
    'airflow/operators/mysql_to_hive.py',
    'airflow/operators/oracle_operator.py',
    'airflow/operators/papermill_operator.py',
    'airflow/operators/pig_operator.py',
    'airflow/operators/postgres_operator.py',
    'airflow/operators/presto_check_operator.py',
    'airflow/operators/presto_to_mysql.py',
    'airflow/operators/python_operator.py',
    'airflow/operators/redshift_to_s3_operator.py',
    'airflow/operators/s3_file_transform_operator.py',
    'airflow/operators/s3_to_hive_operator.py',
    'airflow/operators/s3_to_redshift_operator.py',
    'airflow/operators/slack_operator.py',
    'airflow/operators/sql.py',
    'airflow/operators/sql_branch_operator.py',
    'airflow/operators/sqlite_operator.py',
    'airflow/operators/subdag_operator.py',
    'airflow/sensors/base_sensor_operator.py',
    'airflow/sensors/date_time_sensor.py',
    'airflow/sensors/external_task_sensor.py',
    'airflow/sensors/hdfs_sensor.py',
    'airflow/sensors/hive_partition_sensor.py',
    'airflow/sensors/http_sensor.py',
    'airflow/sensors/metastore_partition_sensor.py',
    'airflow/sensors/named_hive_partition_sensor.py',
    'airflow/sensors/s3_key_sensor.py',
    'airflow/sensors/sql.py',
    'airflow/sensors/sql_sensor.py',
    'airflow/sensors/time_delta_sensor.py',
    'airflow/sensors/web_hdfs_sensor.py',
    'airflow/utils/log/cloudwatch_task_handler.py',
    'airflow/utils/log/es_task_handler.py',
    'airflow/utils/log/gcs_task_handler.py',
    'airflow/utils/log/s3_task_handler.py',
    'airflow/utils/log/stackdriver_task_handler.py',
    'airflow/utils/log/wasb_task_handler.py',
]

CONTRIB_FILES = (AIRFLOW_SOURCES_ROOT / "airflow" / "contrib").rglob("*.py")


if __name__ == '__main__':
    console = Console(color_system="standard", width=300)
    all_deprecated_imports: Dict[str, Dict[str, List[Import]]] = defaultdict(lambda: defaultdict(list))
    # delete = True
    delete = False
    # for file in DEPRECATED_MODULES:
    for file in CONTRIB_FILES:
        file_path = AIRFLOW_SOURCES_ROOT / file
        if not file_path.exists() or file.name == "__init__.py":
            continue
        original_module = os.fspath(file_path.parent.relative_to(AIRFLOW_SOURCES_ROOT)).replace(os.sep, ".")
        for _import in get_imports(file_path):
            module_name = file_path.name[: -len(".py")]
            if _import.name not in ['warnings', 'RemovedInAirflow3Warning']:
                all_deprecated_imports[original_module][module_name].append(_import)
        if delete:
            file_path.unlink()

    for module_path, package_imports in all_deprecated_imports.items():
        console.print(f"[yellow]Import dictionary for {module_path}:\n")
        template = Environment(loader=BaseLoader()).from_string(DEPRECATED_CLASSES_TEMPLATE)
        print(black_format(template.render(package_imports=dict(sorted(package_imports.items())))))
