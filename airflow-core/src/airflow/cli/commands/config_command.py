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
"""Config sub-commands."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from io import StringIO
from typing import Any, NamedTuple

import pygments
from pygments.lexers.configs import IniLexer

from airflow.cli.simple_table import AirflowConsole
from airflow.configuration import AIRFLOW_CONFIG, ConfigModifications, conf
from airflow.exceptions import AirflowConfigException
from airflow.utils.cli import should_use_colors
from airflow.utils.code_utils import get_terminal_formatter
from airflow.utils.providers_configuration_loader import providers_configuration_loaded


@providers_configuration_loaded
def show_config(args):
    """Show current application configuration."""
    with StringIO() as output:
        conf.write(
            output,
            section=args.section,
            include_examples=args.include_examples or args.defaults,
            include_descriptions=args.include_descriptions or args.defaults,
            include_sources=args.include_sources and not args.defaults,
            include_env_vars=args.include_env_vars or args.defaults,
            include_providers=not args.exclude_providers,
            comment_out_everything=args.comment_out_everything or args.defaults,
            only_defaults=args.defaults,
        )
        code = output.getvalue()
    if should_use_colors(args):
        code = pygments.highlight(code=code, formatter=get_terminal_formatter(), lexer=IniLexer())
    print(code)


@providers_configuration_loaded
def get_value(args):
    """Get one value from configuration."""
    # while this will make get_value quite a bit slower we must initialize configuration
    # for providers because we do not know what sections and options will be available after
    # providers are initialized. Theoretically Providers might add new sections and options
    # but also override defaults for existing options, so without loading all providers we
    # cannot be sure what is the final value of the option.
    try:
        value = conf.get(args.section, args.option)
        print(value)
    except AirflowConfigException:
        pass


class ConfigParameter(NamedTuple):
    """Represents a configuration parameter."""

    section: str
    option: str


@dataclass
class ConfigChange:
    """
    Class representing the configuration changes in Airflow 3.0.

    :param config: The configuration parameter being changed.
    :param default_change: If the change is a default value change.
    :param old_default: The old default value (valid only if default_change is True).
    :param new_default: The new default value for the configuration parameter.
    :param suggestion: A suggestion for replacing or handling the removed configuration.
    :param renamed_to: The new section and option if the configuration is renamed.
    :param was_deprecated: If the config is removed, whether the old config was deprecated.
    :param was_removed: If the config is removed.
    :param is_invalid_if: If the current config value is invalid in the future.
    :param breaking: Mark if this change is known to be breaking and causing errors/ warnings / deprecations.
    :param remove_if_equals: For removal rules, remove the option only if its current value equals this value.
    """

    config: ConfigParameter
    default_change: bool = False
    old_default: str | bool | int | float | None = None
    new_default: str | bool | int | float | None = None
    suggestion: str = ""
    renamed_to: ConfigParameter | None = None
    was_deprecated: bool = True
    was_removed: bool = True
    is_invalid_if: Any = None
    breaking: bool = False
    remove_if_equals: str | bool | int | float | None = None

    @property
    def message(self) -> str | None:
        """Generate a message for this configuration change."""
        if self.default_change:
            value = conf.get(self.config.section, self.config.option)
            if value != self.new_default:
                return (
                    f"Changed default value of `{self.config.option}` in `{self.config.section}` "
                    f"from `{self.old_default}` to `{self.new_default}`. "
                    f"You currently have `{value}` set. {self.suggestion}"
                )
        if self.renamed_to:
            if self.config.section != self.renamed_to.section:
                return (
                    f"`{self.config.option}` configuration parameter moved from `{self.config.section}` section to "
                    f"`{self.renamed_to.section}` section as `{self.renamed_to.option}`."
                )
            return (
                f"`{self.config.option}` configuration parameter renamed to `{self.renamed_to.option}` "
                f"in the `{self.config.section}` section."
            )
        if self.was_removed and not self.remove_if_equals:
            return (
                f"Removed{' deprecated' if self.was_deprecated else ''} `{self.config.option}` configuration parameter "
                f"from `{self.config.section}` section. "
                f"{self.suggestion}"
            )
        if self.is_invalid_if is not None:
            value = conf.get(self.config.section, self.config.option)
            if value == self.is_invalid_if:
                return (
                    f"Invalid value `{self.is_invalid_if}` set for `{self.config.option}` configuration parameter "
                    f"in `{self.config.section}` section. {self.suggestion}"
                )
        return None


CONFIGS_CHANGES = [
    # admin
    ConfigChange(
        config=ConfigParameter("admin", "hide_sensitive_variable_fields"),
        renamed_to=ConfigParameter("core", "hide_sensitive_var_conn_fields"),
    ),
    ConfigChange(
        config=ConfigParameter("admin", "sensitive_variable_fields"),
        renamed_to=ConfigParameter("core", "sensitive_var_conn_names"),
    ),
    # core
    ConfigChange(
        config=ConfigParameter("core", "executor"),
        default_change=True,
        old_default="SequentialExecutor",
        new_default="LocalExecutor",
        was_removed=False,
    ),
    ConfigChange(
        config=ConfigParameter("core", "hostname"),
        was_removed=True,
        remove_if_equals=":",
    ),
    ConfigChange(
        config=ConfigParameter("core", "check_slas"),
        suggestion="The SLA feature is removed in Airflow 3.0, to be replaced with Airflow Alerts in future",
    ),
    ConfigChange(
        config=ConfigParameter("core", "strict_dataset_uri_validation"),
        suggestion="Dataset URI with a defined scheme will now always be validated strictly, "
        "raising a hard error on validation failure.",
    ),
    ConfigChange(
        config=ConfigParameter("core", "dag_default_view"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("core", "dag_orientation"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("core", "dataset_manager_class"),
        renamed_to=ConfigParameter("core", "asset_manager_class"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "dataset_manager_kwargs"),
        renamed_to=ConfigParameter("core", "asset_manager_kwargs"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "worker_precheck"),
        renamed_to=ConfigParameter("celery", "worker_precheck"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "non_pooled_task_slot_count"),
        renamed_to=ConfigParameter("core", "default_pool_task_slot_count"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "dag_concurrency"),
        renamed_to=ConfigParameter("core", "max_active_tasks_per_dag"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "sql_alchemy_conn"),
        renamed_to=ConfigParameter("database", "sql_alchemy_conn"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "sql_engine_encoding"),
        renamed_to=ConfigParameter("database", "sql_engine_encoding"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "sql_engine_collation_for_ids"),
        renamed_to=ConfigParameter("database", "sql_engine_collation_for_ids"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "sql_alchemy_pool_enabled"),
        renamed_to=ConfigParameter("database", "sql_alchemy_pool_enabled"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "sql_alchemy_pool_size"),
        renamed_to=ConfigParameter("database", "sql_alchemy_pool_size"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "sql_alchemy_max_overflow"),
        renamed_to=ConfigParameter("database", "sql_alchemy_max_overflow"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "sql_alchemy_pool_recycle"),
        renamed_to=ConfigParameter("database", "sql_alchemy_pool_recycle"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "sql_alchemy_pool_pre_ping"),
        renamed_to=ConfigParameter("database", "sql_alchemy_pool_pre_ping"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "sql_alchemy_schema"),
        renamed_to=ConfigParameter("database", "sql_alchemy_schema"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "sql_alchemy_connect_args"),
        renamed_to=ConfigParameter("database", "sql_alchemy_connect_args"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "load_default_connections"),
        renamed_to=ConfigParameter("database", "load_default_connections"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "max_db_retries"),
        renamed_to=ConfigParameter("database", "max_db_retries"),
    ),
    ConfigChange(config=ConfigParameter("core", "task_runner")),
    ConfigChange(config=ConfigParameter("core", "enable_xcom_pickling")),
    ConfigChange(
        config=ConfigParameter("core", "dag_file_processor_timeout"),
        renamed_to=ConfigParameter("dag_processor", "dag_file_processor_timeout"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "dag_processor_manager_log_location"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "log_processor_filename_template"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "parallelism"),
        was_removed=False,
        is_invalid_if="0",
        suggestion="Please set the `parallelism` configuration parameter to a value greater than 0.",
    ),
    # api
    ConfigChange(
        config=ConfigParameter("api", "access_control_allow_origin"),
        renamed_to=ConfigParameter("api", "access_control_allow_origins"),
    ),
    ConfigChange(
        config=ConfigParameter("api", "auth_backend"),
        renamed_to=ConfigParameter("fab", "auth_backends"),
    ),
    ConfigChange(
        config=ConfigParameter("api", "auth_backends"),
        renamed_to=ConfigParameter("fab", "auth_backends"),
    ),
    # logging
    ConfigChange(
        config=ConfigParameter("logging", "enable_task_context_logger"),
        suggestion="Remove TaskContextLogger: Replaced by the Log table for better handling of task log "
        "messages outside the execution context.",
    ),
    ConfigChange(
        config=ConfigParameter("logging", "dag_processor_manager_log_location"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("logging", "dag_processor_manager_log_stdout"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("logging", "log_processor_filename_template"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("logging", "log_filename_template"),
        was_removed=True,
        remove_if_equals="{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log",
        breaking=True,
    ),
    ConfigChange(
        config=ConfigParameter("logging", "log_filename_template"),
        was_removed=True,
        remove_if_equals="dag_id={{ ti.dag_id }}/run_id={{ ti.run_id }}/task_id={{ ti.task_id }}/{% if ti.map_index >= 0 %}map_index={{ ti.map_index }}/{% endif %}attempt={{ try_number }}.log",
        breaking=True,
    ),
    # metrics
    ConfigChange(
        config=ConfigParameter("metrics", "metrics_use_pattern_match"),
    ),
    ConfigChange(
        config=ConfigParameter("metrics", "timer_unit_consistency"),
        suggestion="In Airflow 3.0, the `timer_unit_consistency` setting in the `metrics` section is "
        "removed as it is now the default behaviour. This is done to standardize all timer and "
        "timing metrics to milliseconds across all metric loggers",
    ),
    ConfigChange(
        config=ConfigParameter("metrics", "statsd_allow_list"),
        renamed_to=ConfigParameter("metrics", "metrics_allow_list"),
    ),
    ConfigChange(
        config=ConfigParameter("metrics", "statsd_block_list"),
        renamed_to=ConfigParameter("metrics", "metrics_block_list"),
    ),
    # traces
    ConfigChange(
        config=ConfigParameter("traces", "otel_task_log_event"),
    ),
    # operators
    ConfigChange(
        config=ConfigParameter("operators", "allow_illegal_arguments"),
    ),
    # webserver
    ConfigChange(
        config=ConfigParameter("webserver", "allow_raw_html_descriptions"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "cookie_samesite"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "update_fab_perms"),
        renamed_to=ConfigParameter("fab", "update_fab_perms"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "auth_rate_limited"),
        renamed_to=ConfigParameter("fab", "auth_rate_limited"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", option="auth_rate_limit"),
        renamed_to=ConfigParameter("fab", "auth_rate_limit"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "config_file"),
        renamed_to=ConfigParameter("fab", "config_file"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "session_backend"),
        renamed_to=ConfigParameter("fab", "session_backend"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "session_lifetime_days"),
        renamed_to=ConfigParameter("fab", "session_lifetime_minutes"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "force_log_out_after"),
        renamed_to=ConfigParameter("fab", "session_lifetime_minutes"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "session_lifetime_minutes"),
        renamed_to=ConfigParameter("fab", "session_lifetime_minutes"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "access_denied_message"),
        renamed_to=ConfigParameter("fab", "access_denied_message"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "expose_hostname"),
        renamed_to=ConfigParameter("fab", "expose_hostname"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "navbar_color"),
        renamed_to=ConfigParameter("fab", "navbar_color"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "navbar_text_color"),
        renamed_to=ConfigParameter("fab", "navbar_text_color"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "navbar_hover_color"),
        renamed_to=ConfigParameter("fab", "navbar_hover_color"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "navbar_text_hover_color"),
        renamed_to=ConfigParameter("fab", "navbar_text_hover_color"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "x_frame_enabled"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "base_url"),
        renamed_to=ConfigParameter("api", "base_url"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "secret_key"),
        renamed_to=ConfigParameter("api", "secret_key"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "web_server_host"),
        renamed_to=ConfigParameter("api", "host"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "web_server_port"),
        renamed_to=ConfigParameter("api", "port"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "workers"),
        renamed_to=ConfigParameter("api", "workers"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "web_server_worker_timeout"),
        renamed_to=ConfigParameter("api", "worker_timeout"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "web_server_ssl_cert"),
        renamed_to=ConfigParameter("api", "ssl_cert"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "web_server_ssl_key"),
        renamed_to=ConfigParameter("api", "ssl_key"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "access_logfile"),
        renamed_to=ConfigParameter("api", "access_logfile"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "enable_swagger_ui"),
        renamed_to=ConfigParameter("api", "enable_swagger_ui"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "error_logfile"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "access_logformat"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "web_server_master_timeout"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "worker_refresh_batch_size"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "worker_refresh_interval"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "reload_on_plugin_change"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "worker_class"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "expose_stacktrace"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "log_fetch_delay_sec"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "log_auto_tailing_offset"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "log_animation_speed"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "default_dag_run_display_number"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "enable_proxy_fix"),
        renamed_to=ConfigParameter("fab", "enable_proxy_fix"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "proxy_fix_x_for"),
        renamed_to=ConfigParameter("fab", "proxy_fix_x_for"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "proxy_fix_x_proto"),
        renamed_to=ConfigParameter("fab", "proxy_fix_x_proto"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "proxy_fix_x_host"),
        renamed_to=ConfigParameter("fab", "proxy_fix_x_host"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "proxy_fix_x_port"),
        renamed_to=ConfigParameter("fab", "proxy_fix_x_port"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "proxy_fix_x_prefix"),
        renamed_to=ConfigParameter("fab", "proxy_fix_x_prefix"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "expose_config"),
        renamed_to=ConfigParameter("api", "expose_config"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "cookie_secure"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "analytics_tool"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "analytics_id"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "analytics_url"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "show_recent_stats_for_completed_runs"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "run_internal_api"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "caching_hash_method"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "show_trigger_form_if_no_params"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "num_recent_configurations_for_trigger"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "allowed_payload_size"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "max_form_memory_size"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "max_form_parts"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "default_ui_timezone"),
        was_deprecated=False,
    ),
    # policy
    ConfigChange(
        config=ConfigParameter("policy", "airflow_local_settings"),
        renamed_to=ConfigParameter("policy", "task_policy"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "navbar_logo_text_color"),
        was_deprecated=False,
    ),
    # scheduler
    ConfigChange(
        config=ConfigParameter("scheduler", "dependency_detector"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "allow_trigger_in_future"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "catchup_by_default"),
        default_change=True,
        old_default="True",
        was_removed=False,
        new_default="False",
        suggestion="In Airflow 3.0 the default value for `catchup_by_default` is set to `False`. "
        "This means that DAGs without explicit definition of the `catchup` parameter will not "
        "catchup by default. "
        "If your DAGs rely on catchup behavior, not explicitly defined in the DAG definition, "
        "set this configuration parameter to `True` in the `scheduler` section of your `airflow.cfg` "
        "to enable the behavior from Airflow 2.x.",
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "create_cron_data_intervals"),
        default_change=True,
        old_default="True",
        new_default="False",
        was_removed=False,
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "create_delta_data_intervals"),
        default_change=True,
        old_default="True",
        new_default="False",
        was_removed=False,
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "processor_poll_interval"),
        renamed_to=ConfigParameter("scheduler", "scheduler_idle_sleep_time"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "deactivate_stale_dags_interval"),
        renamed_to=ConfigParameter("scheduler", "parsing_cleanup_interval"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "statsd_on"), renamed_to=ConfigParameter("metrics", "statsd_on")
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "max_threads"),
        renamed_to=ConfigParameter("dag_processor", "parsing_processes"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "statsd_host"),
        renamed_to=ConfigParameter("metrics", "statsd_host"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "statsd_port"),
        renamed_to=ConfigParameter("metrics", "statsd_port"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "statsd_prefix"),
        renamed_to=ConfigParameter("metrics", "statsd_prefix"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "statsd_allow_list"),
        renamed_to=ConfigParameter("metrics", "statsd_allow_list"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "stat_name_handler"),
        renamed_to=ConfigParameter("metrics", "stat_name_handler"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "statsd_datadog_enabled"),
        renamed_to=ConfigParameter("metrics", "statsd_datadog_enabled"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "statsd_datadog_tags"),
        renamed_to=ConfigParameter("metrics", "statsd_datadog_tags"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "statsd_datadog_metrics_tags"),
        renamed_to=ConfigParameter("metrics", "statsd_datadog_metrics_tags"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "statsd_custom_client_path"),
        renamed_to=ConfigParameter("metrics", "statsd_custom_client_path"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "parsing_processes"),
        renamed_to=ConfigParameter("dag_processor", "parsing_processes"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "file_parsing_sort_mode"),
        renamed_to=ConfigParameter("dag_processor", "file_parsing_sort_mode"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "max_callbacks_per_loop"),
        renamed_to=ConfigParameter("dag_processor", "max_callbacks_per_loop"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "min_file_process_interval"),
        renamed_to=ConfigParameter("dag_processor", "min_file_process_interval"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "stale_dag_threshold"),
        renamed_to=ConfigParameter("dag_processor", "stale_dag_threshold"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "print_stats_interval"),
        renamed_to=ConfigParameter("dag_processor", "print_stats_interval"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "dag_dir_list_interval"),
        renamed_to=ConfigParameter("dag_processor", "refresh_interval"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "local_task_job_heartbeat_sec"),
        renamed_to=ConfigParameter("scheduler", "task_instance_heartbeat_sec"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "scheduler_zombie_task_threshold"),
        renamed_to=ConfigParameter("scheduler", "task_instance_heartbeat_timeout"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "zombie_detection_interval"),
        renamed_to=ConfigParameter("scheduler", "task_instance_heartbeat_timeout_detection_interval"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "child_process_log_directory"),
        renamed_to=ConfigParameter("logging", "dag_processor_child_process_log_directory"),
    ),
    # celery
    ConfigChange(
        config=ConfigParameter("celery", "stalled_task_timeout"),
        renamed_to=ConfigParameter("scheduler", "task_queued_timeout"),
    ),
    ConfigChange(
        config=ConfigParameter("celery", "default_queue"),
        renamed_to=ConfigParameter("operators", "default_queue"),
    ),
    ConfigChange(
        config=ConfigParameter("celery", "task_adoption_timeout"),
        renamed_to=ConfigParameter("scheduler", "task_queued_timeout"),
    ),
    # kubernetes_executor
    ConfigChange(
        config=ConfigParameter("kubernetes_executor", "worker_pods_pending_timeout"),
        renamed_to=ConfigParameter("scheduler", "task_queued_timeout"),
    ),
    ConfigChange(
        config=ConfigParameter("kubernetes_executor", "worker_pods_pending_timeout_check_interval"),
        renamed_to=ConfigParameter("scheduler", "task_queued_timeout_check_interval"),
    ),
    # smtp
    ConfigChange(
        config=ConfigParameter("smtp", "smtp_user"),
        suggestion="Please use the SMTP connection (`smtp_default`).",
    ),
    ConfigChange(
        config=ConfigParameter("smtp", "smtp_password"),
        suggestion="Please use the SMTP connection (`smtp_default`).",
    ),
    # database
    ConfigChange(
        config=ConfigParameter("database", "load_default_connections"),
    ),
    # triggerer
    ConfigChange(
        config=ConfigParameter("triggerer", "default_capacity"),
        renamed_to=ConfigParameter("triggerer", "capacity"),
    ),
    # email
    ConfigChange(
        config=ConfigParameter("email", "email_backend"),
        was_removed=True,
        remove_if_equals="airflow.contrib.utils.sendgrid.send_email",
    ),
    # elasticsearch
    ConfigChange(
        config=ConfigParameter("elasticsearch", "log_id_template"),
        was_removed=True,
        remove_if_equals="{dag_id}-{task_id}-{logical_date}-{try_number}",
        breaking=True,
    ),
]


@providers_configuration_loaded
def lint_config(args) -> None:
    """
    Lint the airflow.cfg file for removed, or renamed configurations.

    This function scans the Airflow configuration file for parameters that are removed or renamed in
    Airflow 3.0. It provides suggestions for alternative parameters or  settings where applicable.
    CLI Arguments:
        --section: str (optional)
            The specific section of the configuration to lint.
            Example: --section core

        --option: str (optional)
            The specific option within a section to lint.
            Example: --option check_slas

        --ignore-section: str (optional)
            A section to ignore during linting.
            Example: --ignore-section webserver

        --ignore-option: str (optional)
            An option to ignore during linting.
            Example: --ignore-option smtp_user

        --verbose: flag (optional)
            Enables detailed output, including the list of ignored sections and options.
            Example: --verbose

    Examples:
        1. Lint all sections and options:
            airflow config lint

        2. Lint a specific section:
            airflow config lint --section core,webserver

        3. Lint specific sections and options:
            airflow config lint --section smtp --option smtp_user

        4. Ignore a section:
            airflow config lint --ignore-section webserver,api

        5. Ignore an options:
            airflow config lint --ignore-option smtp_user,session_lifetime_days

        6. Enable verbose output:
            airflow config lint --verbose

    :param args: The CLI arguments for linting configurations.
    """
    console = AirflowConsole()
    lint_issues = []

    section_to_check_if_provided = args.section or []
    option_to_check_if_provided = args.option or []

    ignore_sections = args.ignore_section or []
    ignore_options = args.ignore_option or []

    for configuration in CONFIGS_CHANGES:
        if section_to_check_if_provided and configuration.config.section not in section_to_check_if_provided:
            continue

        if option_to_check_if_provided and configuration.config.option not in option_to_check_if_provided:
            continue

        if configuration.config.section in ignore_sections or configuration.config.option in ignore_options:
            continue

        if conf.has_option(
            configuration.config.section, configuration.config.option, lookup_from_deprecated=False
        ):
            if configuration.message is not None:
                lint_issues.append(configuration.message)

    if lint_issues:
        console.print("[red]Found issues in your airflow.cfg:[/red]")
        for issue in lint_issues:
            console.print(f"  - [yellow]{issue}[/yellow]")
        if args.verbose:
            console.print("\n[blue]Detailed Information:[/blue]")
            console.print(f"Ignored sections: [green]{', '.join(ignore_sections)}[/green]")
            console.print(f"Ignored options: [green]{', '.join(ignore_options)}[/green]")
        console.print("\n[red]Please update your configuration file accordingly.[/red]")
    else:
        console.print("[green]No issues found in your airflow.cfg. It is ready for Airflow 3![/green]")


@providers_configuration_loaded
def update_config(args) -> None:
    """
    Update the airflow.cfg file to migrate configuration changes from Airflow 2.x to Airflow 3.

    By default, this command will perform a dry-run (showing the changes only) and list only
    the breaking configuration changes by scanning the current configuration file for parameters that have
    been renamed, removed, or had their default values changed in Airflow 3.0. To see or fix all recommended
    changes, use the --all-recommendations argument. To automatically update your airflow.cfg file, use
    the --fix argument. This command cleans up the existing comments in airflow.cfg but creates a backup of
    the old airflow.cfg file.

    CLI Arguments:
        --fix: flag (optional)
            Automatically fix/apply the breaking changes (or all changes if --all-recommendations is also
            specified)
            Example: --fix

        --all-recommendations: flag (optional)
            Include non-breaking (recommended) changes as well as breaking ones.
            Can be used with --fix.
            Example: --all-recommendations

        --section: str (optional)
            Comma-separated list of configuration sections to update.
            Example: --section core,database

        --option: str (optional)
            Comma-separated list of configuration options to update.
            Example: --option sql_alchemy_conn,dag_concurrency

        --ignore-section: str (optional)
            Comma-separated list of configuration sections to ignore during update.
            Example: --ignore-section webserver

        --ignore-option: str (optional)
            Comma-separated list of configuration options to ignore during update.
            Example: --ignore-option check_slas

    Examples:
        1. Dry-run mode (print the changes in modified airflow.cfg) showing only breaking changes:
            airflow config update

        2. Dry-run mode showing all recommendations:
            airflow config update --all-recommendations

        3. Apply (fix) only breaking changes:
            airflow config update --fix

        4. Apply (fix) all recommended changes:
            airflow config update --fix --all-recommendations

        5. Show changes only the specific sections:
            airflow config update --section core,database

        6.Show changes only the specific options:
            airflow config update --option sql_alchemy_conn,dag_concurrency

        7. Ignores the specific section:
            airflow config update --ignore-section webserver

    :param args: The CLI arguments for updating configuration.
    """
    console = AirflowConsole()
    changes_applied: list[str] = []
    modifications = ConfigModifications()

    include_all = args.all_recommendations if args.all_recommendations else False
    apply_fix = args.fix if args.fix else False
    dry_run = not apply_fix
    update_sections = args.section if args.section else None
    update_options = args.option if args.option else None
    ignore_sections = args.ignore_section if args.ignore_section else []
    ignore_options = args.ignore_option if args.ignore_option else []

    config_dict = conf.as_dict(
        display_source=True,
        include_env=False,
        include_cmds=False,
        include_secret=True,
        display_sensitive=True,
    )
    for change in CONFIGS_CHANGES:
        if not include_all and not change.breaking:
            continue
        conf_section = change.config.section.lower()
        conf_option = change.config.option.lower()
        full_key = f"{conf_section}.{conf_option}"

        if update_sections is not None and conf_section not in [s.lower() for s in update_sections]:
            continue
        if update_options is not None and full_key not in [opt.lower() for opt in update_options]:
            continue
        if conf_section in [s.lower() for s in ignore_sections] or full_key in [
            opt.lower() for opt in ignore_options
        ]:
            continue

        if conf_section not in config_dict or conf_option not in config_dict[conf_section]:
            continue
        value_data = config_dict[conf_section][conf_option]
        if not (isinstance(value_data, tuple) and value_data[1] == "airflow.cfg"):
            continue

        current_value = value_data[0]
        prefix = "[[red]BREAKING[/red]]" if change.breaking else "[[yellow]Recommended[/yellow]]"
        if change.default_change:
            if str(current_value) != str(change.new_default):
                modifications.add_default_update(conf_section, conf_option, str(change.new_default))
                changes_applied.append(
                    f"{prefix} Updated default value of '{conf_section}/{conf_option}' from "
                    f"'{current_value}' to '{change.new_default}'."
                )
        if change.renamed_to:
            modifications.add_rename(
                conf_section, conf_option, change.renamed_to.section, change.renamed_to.option
            )
            changes_applied.append(
                f"{prefix} Renamed '{conf_section}/{conf_option}' to "
                f"'{change.renamed_to.section.lower()}/{change.renamed_to.option.lower()}'."
            )
        elif change.was_removed:
            if change.remove_if_equals is not None:
                if str(current_value) == str(change.remove_if_equals):
                    modifications.add_remove(conf_section, conf_option)
                    changes_applied.append(
                        f"{prefix} Removed '{conf_section}/{conf_option}' from configuration."
                    )
            else:
                modifications.add_remove(conf_section, conf_option)
                changes_applied.append(f"{prefix} Removed '{conf_section}/{conf_option}' from configuration.")

    backup_path = f"{AIRFLOW_CONFIG}.bak"
    try:
        shutil.copy2(AIRFLOW_CONFIG, backup_path)
        console.print(f"Backup saved as '{backup_path}'.")
    except Exception as e:
        console.print(f"Failed to create backup: {e}")
        raise AirflowConfigException("Backup creation failed. Aborting update_config operation.")

    if dry_run:
        console.print("[blue]Dry-run mode enabled. No changes will be written to airflow.cfg.[/blue]")
        with StringIO() as config_output:
            conf.write_custom_config(
                file=config_output,
                comment_out_defaults=True,
                include_descriptions=True,
                modifications=modifications,
            )
            new_config = config_output.getvalue()
        console.print(new_config)
    else:
        with open(AIRFLOW_CONFIG, "w") as config_file:
            conf.write_custom_config(
                file=config_file,
                comment_out_defaults=True,
                include_descriptions=True,
                modifications=modifications,
            )

    if changes_applied:
        console.print("[green]The following are the changes in airflow config:[/green]")
        for change_msg in changes_applied:
            console.print(f"  - {change_msg}")
        if dry_run:
            console.print(
                "[blue]Dry-run is mode enabled. To apply above airflow.cfg run the command "
                "with `--fix`.[/blue]"
            )
    else:
        console.print("[green]No updates needed. Your configuration is already up-to-date.[/green]")

    if args.verbose:
        console.print("[blue]Configuration update completed with verbose output enabled.[/blue]")
