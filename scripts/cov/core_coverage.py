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

import sys
from pathlib import Path

from cov_runner import run_tests

sys.path.insert(0, str(Path(__file__).parent.resolve()))

source_files = [
    "airflow/executors",
    "airflow/jobs",
    "airflow/models",
    "airflow/serialization",
    "airflow/ti_deps",
    "airflow/utils",
]

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
    "airflow/models/asset.py",
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
    "airflow/serialization/serde.py",
    "airflow/serialization/serialized_objects.py",
    "airflow/serialization/serializers/bignum.py",
    "airflow/serialization/serializers/builtin.py",
    "airflow/serialization/serializers/datetime.py",
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
]
core_files = [
    "tests/core",
    "tests/executors",
    "tests/jobs",
    "tests/models",
    "tests/serialization",
    "tests/ti_deps",
    "tests/utils",
]


if __name__ == "__main__":
    args = ["-qq"] + core_files
    run_tests(args, source_files, files_not_fully_covered)
