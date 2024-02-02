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

import glob
import os
import re
import sys
from functools import lru_cache

import pytest
from coverage import Coverage
from coverage.exceptions import NoSource
from rich.console import Console
from rich.theme import Theme

recording_width = os.environ.get("RECORD_BREEZE_WIDTH")
recording_file = os.environ.get("RECORD_BREEZE_OUTPUT_FILE")

command_list = sys.argv[1:]

# Select test types and the sources they cover
TEST_TYPE_MAP_TO_SOURCES_FOR_COVERAGE = {
    "API": ["airflow/api", "airflow/api_connexion", "airflow/api_internal"],
    "CLI": ["airflow/cli"],
    "Core": [
        "airflow/executors",
        "airflow/jobs",
        "airflow/models",
        "airflow/ti_deps",
        "airflow/utils",
    ],
    "Serialization": [
        "airflow/serialization",
    ],
    "WWW": [
        "airflow/www",
    ],
}

files_not_fully_covered = [
    # executors
    "airflow/executors/executor_loader.py",
    "airflow/executors/local_executor.py",
    "airflow/executors/sequential_executor.py",
    # jobs
    "airflow/jobs/backfill_job_runner.py",
    "airflow/jobs/base_job_runner.py",
    "airflow/jobs/dag_processor_job_runner.py",
    "airflow/jobs/job.py",
    "airflow/jobs/local_task_job_runner.py",
    "airflow/jobs/scheduler_job_runner.py",
    "airflow/jobs/triggerer_job_runner.py",
    # models
    "airflow/models/abstractoperator.py",
    "airflow/models/base.py",
    "airflow/models/baseoperator.py",
    "airflow/models/connection.py",
    "airflow/models/crypto.py",
    "airflow/models/dag.py",
    "airflow/models/dagbag.py",
    "airflow/models/dagcode.py",
    "airflow/models/dagpickle.py",
    "airflow/models/dagrun.py",
    "airflow/models/dagwarning.py",
    "airflow/models/dataset.py",
    "airflow/models/expandinput.py",
    "airflow/models/log.py",
    "airflow/models/mappedoperator.py",
    "airflow/models/param.py",
    "airflow/models/pool.py",
    "airflow/models/renderedtifields.py",
    "airflow/models/serialized_dag.py",
    "airflow/models/skipmixin.py",
    "airflow/models/taskinstance.py",
    "airflow/models/taskinstancekey.py",
    "airflow/models/taskmap.py",
    "airflow/models/taskmixin.py",
    "airflow/models/taskreschedule.py",
    "airflow/models/trigger.py",
    "airflow/models/variable.py",
    "airflow/models/xcom.py",
    "airflow/models/xcom_arg.py",
    # serialization
    "airflow/serialization/json_schema.py",
    "airflow/serialization/pydantic/dag_run.py",
    "airflow/serialization/pydantic/job.py",
    "airflow/serialization/pydantic/taskinstance.py",
    "airflow/serialization/pydantic/dag.py",
    "airflow/serialization/serde.py",
    "airflow/serialization/serialized_objects.py",
    "airflow/serialization/serializers/bignum.py",
    "airflow/serialization/serializers/builtin.py",
    "airflow/serialization/serializers/datetime.py",
    "airflow/serialization/serializers/deltalake.py",
    "airflow/serialization/serializers/iceberg.py",
    "airflow/serialization/serializers/kubernetes.py",
    "airflow/serialization/serializers/numpy.py",
    "airflow/serialization/serializers/pandas.py",
    "airflow/serialization/serializers/timezone.py",
    # ti_deps
    "airflow/ti_deps/deps/base_ti_dep.py",
    "airflow/ti_deps/deps/exec_date_after_start_date_dep.py",
    "airflow/ti_deps/deps/not_previously_skipped_dep.py",
    "airflow/ti_deps/deps/pool_slots_available_dep.py",
    "airflow/ti_deps/deps/prev_dagrun_dep.py",
    "airflow/ti_deps/deps/task_not_running_dep.py",
    "airflow/ti_deps/deps/trigger_rule_dep.py",
    "airflow/ti_deps/deps/valid_state_dep.py",
    # utils
    "airflow/utils/airflow_flask_app.py",
    "airflow/utils/cli.py",
    "airflow/utils/cli_action_loggers.py",
    "airflow/utils/code_utils.py",
    "airflow/utils/context.py",
    "airflow/utils/dag_cycle_tester.py",
    "airflow/utils/dag_parsing_context.py",
    "airflow/utils/dates.py",
    "airflow/utils/db.py",
    "airflow/utils/db_cleanup.py",
    "airflow/utils/decorators.py",
    "airflow/utils/deprecation_tools.py",
    "airflow/utils/docs.py",
    "airflow/utils/dot_renderer.py",
    "airflow/utils/edgemodifier.py",
    "airflow/utils/email.py",
    "airflow/utils/empty_set.py",
    "airflow/utils/entry_points.py",
    "airflow/utils/file.py",
    "airflow/utils/hashlib_wrapper.py",
    "airflow/utils/helpers.py",
    "airflow/utils/json.py",
    "airflow/utils/log/action_logger.py",
    "airflow/utils/log/colored_log.py",
    "airflow/utils/log/file_processor_handler.py",
    "airflow/utils/log/file_task_handler.py",
    "airflow/utils/log/json_formatter.py",
    "airflow/utils/log/log_reader.py",
    "airflow/utils/log/logging_mixin.py",
    "airflow/utils/log/non_caching_file_handler.py",
    "airflow/utils/log/secrets_masker.py",
    "airflow/utils/sensor_helper.py",
    "airflow/utils/log/task_context_logger.py",
    "airflow/utils/log/task_handler_with_custom_formatter.py",
    "airflow/utils/log/trigger_handler.py",
    "airflow/utils/mixins.py",
    "airflow/utils/module_loading.py",
    "airflow/utils/net.py",
    "airflow/utils/operator_helpers.py",
    "airflow/utils/operator_resources.py",
    "airflow/utils/orm_event_handlers.py",
    "airflow/utils/platform.py",
    "airflow/utils/process_utils.py",
    "airflow/utils/pydantic.py",
    "airflow/utils/python_virtualenv.py",
    "airflow/utils/scheduler_health.py",
    "airflow/utils/serve_logs.py",
    "airflow/utils/session.py",
    "airflow/utils/setup_teardown.py",
    "airflow/utils/sqlalchemy.py",
    "airflow/utils/state.py",
    "airflow/utils/strings.py",
    "airflow/utils/task_group.py",
    "airflow/utils/task_instance_session.py",
    "airflow/utils/timeout.py",
    "airflow/utils/timezone.py",
    "airflow/utils/yaml.py",
    # API
    "airflow/api/__init__.py",
    "airflow/api/auth/backend/basic_auth.py",
    "airflow/api/auth/backend/deny_all.py",
    "airflow/api/auth/backend/kerberos_auth.py",
    "airflow/api/client/api_client.py",
    "airflow/api/client/json_client.py",
    "airflow/api/client/local_client.py",
    "airflow/api/common/delete_dag.py",
    "airflow/api/common/experimental/__init__.py",
    "airflow/api/common/experimental/get_code.py",
    "airflow/api/common/experimental/get_dag_run_state.py",
    "airflow/api/common/experimental/get_dag_runs.py",
    "airflow/api/common/experimental/get_lineage.py",
    "airflow/api/common/experimental/get_task.py",
    "airflow/api/common/experimental/get_task_instance.py",
    "airflow/api/common/experimental/mark_tasks.py",
    "airflow/api/common/mark_tasks.py",
    "airflow/api/common/trigger_dag.py",
    "airflow/api_connexion/endpoints/dataset_endpoint.py",
    "airflow/api_connexion/endpoints/task_instance_endpoint.py",
    "airflow/api_connexion/exceptions.py",
    "airflow/api_connexion/schemas/common_schema.py",
    "airflow/api_connexion/schemas/dag_schema.py",
    "airflow/api_connexion/security.py",
    "airflow/api_connexion/types.py",
    "airflow/api_internal/endpoints/rpc_api_endpoint.py",
    "airflow/api_internal/internal_api_call.py",
    ## CLI
    "airflow/cli/cli_config.py",
    "airflow/cli/cli_parser.py",
    "airflow/cli/commands/celery_command.py",
    "airflow/cli/commands/config_command.py",
    "airflow/cli/commands/connection_command.py",
    "airflow/cli/commands/daemon_utils.py",
    "airflow/cli/commands/dag_command.py",
    "airflow/cli/commands/dag_processor_command.py",
    "airflow/cli/commands/db_command.py",
    "airflow/cli/commands/info_command.py",
    "airflow/cli/commands/internal_api_command.py",
    "airflow/cli/commands/jobs_command.py",
    "airflow/cli/commands/kubernetes_command.py",
    "airflow/cli/commands/plugins_command.py",
    "airflow/cli/commands/pool_command.py",
    "airflow/cli/commands/provider_command.py",
    "airflow/cli/commands/standalone_command.py",
    "airflow/cli/commands/task_command.py",
    "airflow/cli/commands/variable_command.py",
    "airflow/cli/commands/webserver_command.py",
    "airflow/cli/simple_table.py",
    "airflow/cli/utils.py",
    # WWW
    "airflow/www/api/experimental/endpoints.py",
    "airflow/www/app.py",
    "airflow/www/auth.py",
    "airflow/www/decorators.py",
    "airflow/www/extensions/init_appbuilder.py",
    "airflow/www/extensions/init_auth_manager.py",
    "airflow/www/extensions/init_dagbag.py",
    "airflow/www/extensions/init_jinja_globals.py",
    "airflow/www/extensions/init_manifest_files.py",
    "airflow/www/extensions/init_security.py",
    "airflow/www/extensions/init_session.py",
    "airflow/www/extensions/init_views.py",
    "airflow/www/fab_security/manager.py",
    "airflow/www/forms.py",
    "airflow/www/gunicorn_config.py",
    "airflow/www/security_manager.py",
    "airflow/www/session.py",
    "airflow/www/utils.py",
    "airflow/www/validators.py",
    "airflow/www/views.py",
    "airflow/www/widgets.py",
    "airflow/www/security.py",
]
non_db_test_files = [
    "airflow/api/client/__init__.py",
    "airflow/api_connexion/parameters.py",
    "airflow/api_connexion/schemas/task_instance_schema.py",
    "airflow/executors/base_executor.py",
    "airflow/utils/compression.py",
    "airflow/utils/event_scheduler.py",
    "airflow/utils/weekday.py",
    "airflow/utils/dag_edges.py",
    "airflow/executors/debug_executor.py",
    "airflow/utils/retries.py",
    "airflow/utils/jwt_signer.py",
    "airflow/cli/commands/version_command.py",
    "airflow/cli/commands/cheat_sheet_command.py",
    "airflow/cli/commands/legacy_commands.py",
]

test_type = os.environ["TEST_TYPE"]


def file_name_from_test_type(testtype: str):
    test_type_no_brackets = testtype.lower().replace("[", "_").replace("]", "")
    return re.sub("[,.]", "_", test_type_no_brackets)[:30]


def coverage_filepath(testtype: str, backend: str) -> str:
    file_friendly_test_type = file_name_from_test_type(testtype)
    random_suffix = os.urandom(4).hex()
    return f"/files/coverage-{file_friendly_test_type}-{backend}-{random_suffix}.xml"


def get_theme() -> Theme:
    return Theme(
        {
            "success": "green",
            "info": "bright_blue",
            "warning": "bright_yellow",
            "error": "red",
            "special": "magenta",
        }
    )


@lru_cache(maxsize=None)
def get_console() -> Console:
    return Console(
        force_terminal=True,
        color_system="standard",
        width=202 if not recording_width else int(recording_width),
        file=None,
        theme=get_theme(),
        record=True if recording_file else False,
    )


if __name__ == "__main__":
    source = ["airflow"]
    if test_type and TEST_TYPE_MAP_TO_SOURCES_FOR_COVERAGE.get(test_type):
        source = TEST_TYPE_MAP_TO_SOURCES_FOR_COVERAGE[test_type]
    cov = Coverage(
        config_file="pyproject.toml",
        source=source,
        concurrency="multiprocessing",
    )
    coverage_file = coverage_filepath(test_type, os.environ["BACKEND"])
    if test_type == "Helm":
        pytest.main(command_list)
    else:
        with cov.collect():
            pytest.main(command_list)
        # Combine the coverage and report
        cov.combine()
        cov.xml_report(outfile=coverage_file)
    # Analyze the coverage
    if source != ["airflow"] and test_type in TEST_TYPE_MAP_TO_SOURCES_FOR_COVERAGE:
        covered = sorted(
            {path for item in source for path in glob.glob(item + "/**/*.py", recursive=True)}
            - {path for path in files_not_fully_covered}
        )
        failed = False
        if os.environ.get("_AIRFLOW_RUN_DB_TESTS_ONLY") == "true":
            covered = [path for path in covered if path not in non_db_test_files]
        for path in covered:
            missing_lines = cov.analysis2(path)[3]
            if len(missing_lines) > 0:
                get_console().print(f"[error] Error: {path} has dropped in coverage. Please update tests")
                failed = True
        for path in files_not_fully_covered:
            try:
                missing_lines = cov.analysis2(path)[3]
                if not missing_lines:
                    get_console().print(
                        f"[error] Error: {path} now has full coverage. "
                        "Please remove from files_not_fully_covered at scripts/in_container/run_ci_tests.py"
                    )
                    failed = True
            except NoSource:
                continue
        if failed:
            get_console().print("[error]There are some coverage errors. Please fix them")
