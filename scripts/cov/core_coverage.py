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
    "airflow-core/src/airflow/executors",
    "airflow-core/src/airflow/jobs",
    "airflow-core/src/airflow/models",
    "airflow-core/src/airflow/serialization",
    "airflow-core/src/airflow/ti_deps",
    "airflow-core/src/airflow/utils",
]

files_not_fully_covered = [
    # executors
    "airflow-core/src/airflow/executors/base_executor.py",
    "airflow-core/src/airflow/executors/executor_loader.py",
    "airflow-core/src/airflow/executors/local_executor.py",
    # jobs
    "airflow-core/src/airflow/jobs/base_job_runner.py",
    "airflow-core/src/airflow/jobs/dag_processor_job_runner.py",
    "airflow-core/src/airflow/jobs/job.py",
    "airflow-core/src/airflow/jobs/scheduler_job_runner.py",
    "airflow-core/src/airflow/jobs/triggerer_job_runner.py",
    # models
    "airflow-core/src/airflow/models/asset.py",
    "airflow-core/src/airflow/models/backfill.py",
    "airflow-core/src/airflow/models/base.py",
    "airflow-core/src/airflow/models/baseoperator.py",
    "airflow-core/src/airflow/models/callback.py",
    "airflow-core/src/airflow/models/connection.py",
    "airflow-core/src/airflow/models/crypto.py",
    "airflow-core/src/airflow/models/dag.py",
    "airflow-core/src/airflow/models/dag_version.py",
    "airflow-core/src/airflow/models/dagbag.py",
    "airflow-core/src/airflow/models/dagcode.py",
    "airflow-core/src/airflow/models/dagrun.py",
    "airflow-core/src/airflow/models/dagwarning.py",
    "airflow-core/src/airflow/models/deadline.py",
    "airflow-core/src/airflow/models/expandinput.py",
    "airflow-core/src/airflow/models/log.py",
    "airflow-core/src/airflow/models/mappedoperator.py",
    "airflow-core/src/airflow/models/param.py",
    "airflow-core/src/airflow/models/pool.py",
    "airflow-core/src/airflow/models/renderedtifields.py",
    "airflow-core/src/airflow/models/serialized_dag.py",
    "airflow-core/src/airflow/models/taskinstance.py",
    "airflow-core/src/airflow/models/taskinstancehistory.py",
    "airflow-core/src/airflow/models/taskinstancekey.py",
    "airflow-core/src/airflow/models/taskmap.py",
    "airflow-core/src/airflow/models/taskmixin.py",
    "airflow-core/src/airflow/models/trigger.py",
    "airflow-core/src/airflow/models/variable.py",
    "airflow-core/src/airflow/models/xcom.py",
    "airflow-core/src/airflow/models/xcom_arg.py",
    # serialization
    "airflow-core/src/airflow/serialization/json_schema.py",
    "airflow-core/src/airflow/serialization/serde.py",
    "airflow-core/src/airflow/serialization/serialized_objects.py",
    "airflow-core/src/airflow/serialization/serializers/bignum.py",
    "airflow-core/src/airflow/serialization/serializers/builtin.py",
    "airflow-core/src/airflow/serialization/serializers/datetime.py",
    "airflow-core/src/airflow/serialization/serializers/deltalake.py",
    "airflow-core/src/airflow/serialization/serializers/iceberg.py",
    "airflow-core/src/airflow/serialization/serializers/kubernetes.py",
    "airflow-core/src/airflow/serialization/serializers/numpy.py",
    "airflow-core/src/airflow/serialization/serializers/pandas.py",
    "airflow-core/src/airflow/serialization/serializers/timezone.py",
    # ti_deps
    "airflow-core/src/airflow/ti_deps/deps/base_ti_dep.py",
    "airflow-core/src/airflow/ti_deps/deps/exec_date_after_start_date_dep.py",
    "airflow-core/src/airflow/ti_deps/deps/mapped_task_expanded.py",
    "airflow-core/src/airflow/ti_deps/deps/not_previously_skipped_dep.py",
    "airflow-core/src/airflow/ti_deps/deps/pool_slots_available_dep.py",
    "airflow-core/src/airflow/ti_deps/deps/prev_dagrun_dep.py",
    "airflow-core/src/airflow/ti_deps/deps/ready_to_reschedule.py",
    "airflow-core/src/airflow/ti_deps/deps/runnable_exec_date_dep.py",
    "airflow-core/src/airflow/ti_deps/deps/task_not_running_dep.py",
    "airflow-core/src/airflow/ti_deps/deps/trigger_rule_dep.py",
    "airflow-core/src/airflow/ti_deps/deps/valid_state_dep.py",
    # utils
    "airflow-core/src/airflow/utils/cli.py",
    "airflow-core/src/airflow/utils/cli_action_loggers.py",
    "airflow-core/src/airflow/utils/code_utils.py",
    "airflow-core/src/airflow/utils/context.py",
    "airflow-core/src/airflow/utils/dag_parsing_context.py",
    "airflow-core/src/airflow/utils/db.py",
    "airflow-core/src/airflow/utils/db_cleanup.py",
    "airflow-core/src/airflow/utils/db_manager.py",
    "airflow-core/src/airflow/utils/decorators.py",
    "airflow-core/src/airflow/utils/deprecation_tools.py",
    "airflow-core/src/airflow/utils/dot_renderer.py",
    "airflow-core/src/airflow/utils/edgemodifier.py",
    "airflow-core/src/airflow/utils/email.py",
    "airflow-core/src/airflow/utils/entry_points.py",
    "airflow-core/src/airflow/utils/file.py",
    "airflow-core/src/airflow/utils/helpers.py",
    "airflow-core/src/airflow/utils/json.py",
    "airflow-core/src/airflow/utils/log/action_logger.py",
    "airflow-core/src/airflow/utils/log/colored_log.py",
    "airflow-core/src/airflow/utils/log/file_processor_handler.py",
    "airflow-core/src/airflow/utils/log/file_task_handler.py",
    "airflow-core/src/airflow/utils/log/json_formatter.py",
    "airflow-core/src/airflow/utils/log/log_reader.py",
    "airflow-core/src/airflow/utils/log/logging_mixin.py",
    "airflow-core/src/airflow/utils/log/non_caching_file_handler.py",
    "airflow-core/src/airflow/utils/log/task_handler_with_custom_formatter.py",
    "airflow-core/src/airflow/utils/module_loading.py",
    "airflow-core/src/airflow/utils/net.py",
    "airflow-core/src/airflow/utils/operator_resources.py",
    "airflow-core/src/airflow/utils/orm_event_handlers.py",
    "airflow-core/src/airflow/utils/platform.py",
    "airflow-core/src/airflow/utils/process_utils.py",
    "airflow-core/src/airflow/utils/retries.py",
    "airflow-core/src/airflow/utils/scheduler_health.py",
    "airflow-core/src/airflow/utils/serve_logs.py",
    "airflow-core/src/airflow/utils/session.py",
    "airflow-core/src/airflow/utils/setup_teardown.py",
    "airflow-core/src/airflow/utils/span_status.py",
    "airflow-core/src/airflow/utils/sqlalchemy.py",
    "airflow-core/src/airflow/utils/state.py",
    "airflow-core/src/airflow/utils/strings.py",
    "airflow-core/src/airflow/utils/task_group.py",
    "airflow-core/src/airflow/utils/task_instance_session.py",
    "airflow-core/src/airflow/utils/timeout.py",
    "airflow-core/src/airflow/utils/weight_rule.py",
    "airflow-core/src/airflow/utils/yaml.py",
]
core_files = [
    "airflow-core/tests/unit/core",
    "airflow-core/tests/unit/executors",
    "airflow-core/tests/unit/jobs",
    "airflow-core/tests/unit/models",
    "airflow-core/tests/unit/serialization",
    "airflow-core/tests/unit/ti_deps",
    "airflow-core/tests/unit/utils",
]


if __name__ == "__main__":
    args = ["-qq"] + core_files
    run_tests(args, source_files, files_not_fully_covered)
