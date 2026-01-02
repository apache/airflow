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
"""This module contains Google BigQuery operators."""

from __future__ import annotations

import enum
import json
import re
import warnings
from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any, SupportsAbs

from google.api_core.exceptions import Conflict
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud.bigquery import DEFAULT_RETRY, CopyJob, ExtractJob, LoadJob, QueryJob, Row
from google.cloud.bigquery.table import RowIterator, Table, TableListItem, TableReference

from airflow.configuration import conf
from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.common.compat.sdk import AirflowException, AirflowSkipException
from airflow.providers.common.sql.operators.sql import (  # for _parse_boolean
    SQLCheckOperator,
    SQLColumnCheckOperator,
    SQLIntervalCheckOperator,
    SQLTableCheckOperator,
    SQLValueCheckOperator,
    _parse_boolean,
)
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook, BigQueryJob
from airflow.providers.google.cloud.hooks.gcs import GCSHook, _parse_gcs_url
from airflow.providers.google.cloud.links.bigquery import (
    BigQueryDatasetLink,
    BigQueryJobDetailLink,
    BigQueryTableLink,
)
from airflow.providers.google.cloud.openlineage.mixins import _BigQueryInsertJobOperatorOpenLineageMixin
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.cloud.triggers.bigquery import (
    BigQueryCheckTrigger,
    BigQueryGetDataTrigger,
    BigQueryInsertJobTrigger,
    BigQueryIntervalCheckTrigger,
    BigQueryValueCheckTrigger,
)
from airflow.providers.google.cloud.utils.bigquery import convert_job_id
from airflow.providers.google.common.deprecated import deprecated
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID
from airflow.utils.helpers import exactly_one

if TYPE_CHECKING:
    from google.api_core.retry import Retry
    from google.cloud.bigquery import UnknownJob

    from airflow.providers.common.compat.sdk import Context


BIGQUERY_JOB_DETAILS_LINK_FMT = "https://console.cloud.google.com/bigquery?j={job_id}"

LABEL_REGEX = re.compile(r"^[\w-]{0,63}$")


class BigQueryUIColors(enum.Enum):
    """Hex colors for BigQuery operators."""

    CHECK = "#C0D7FF"
    QUERY = "#A1BBFF"
    TABLE = "#81A0FF"
    DATASET = "#5F86FF"


class IfExistAction(enum.Enum):
    """Action to take if the resource exist."""

    IGNORE = "ignore"
    LOG = "log"
    FAIL = "fail"
    SKIP = "skip"


class _BigQueryHookWithFlexibleProjectId(BigQueryHook):
    @property
    def project_id(self) -> str:
        _, project_id = self.get_credentials_and_project_id()
        return project_id or PROVIDE_PROJECT_ID

    @project_id.setter
    def project_id(self, value: str) -> None:
        cached_creds, _ = self.get_credentials_and_project_id()
        self._cached_project_id = value or PROVIDE_PROJECT_ID
        self._cached_credntials = cached_creds


class _BigQueryDbHookMixin:
    def get_db_hook(self: BigQueryCheckOperator) -> _BigQueryHookWithFlexibleProjectId:  # type:ignore[misc]
        """Get BigQuery DB Hook."""
        hook = _BigQueryHookWithFlexibleProjectId(
            gcp_conn_id=self.gcp_conn_id,
            use_legacy_sql=self.use_legacy_sql,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
            labels=self.labels,
        )

        # mypy assuming project_id is read only, as project_id is a property in GoogleBaseHook.
        if self.project_id:
            hook.project_id = self.project_id  # type:ignore[misc]
        return hook


class _BigQueryOperatorsEncryptionConfigurationMixin:
    """A class to handle the configuration for BigQueryHook.insert_job method."""

    # Note: If you want to add this feature to a new operator you can include the class name in the type
    # annotation of the `self`. Then you can inherit this class in the target operator.
    # e.g: BigQueryCheckOperator, BigQueryTableCheckOperator
    def include_encryption_configuration(  # type:ignore[misc]
        self: BigQueryCheckOperator
        | BigQueryTableCheckOperator
        | BigQueryValueCheckOperator
        | BigQueryColumnCheckOperator
        | BigQueryGetDataOperator
        | BigQueryIntervalCheckOperator,
        configuration: dict,
        config_key: str,
    ) -> None:
        """Add encryption_configuration to destinationEncryptionConfiguration key if it is not None."""
        if self.encryption_configuration is not None:
            configuration[config_key]["destinationEncryptionConfiguration"] = self.encryption_configuration


class BigQueryCheckOperator(
    _BigQueryDbHookMixin, SQLCheckOperator, _BigQueryOperatorsEncryptionConfigurationMixin
):
    """
    Performs checks against BigQuery.

    This operator expects a SQL query that returns a single row. Each value on
    that row is evaluated using a Python ``bool`` cast. If any of the values
    is falsy, the check errors out.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryCheckOperator`

    Note that Python bool casting evals the following as *False*:

    * ``False``
    * ``0``
    * Empty string (``""``)
    * Empty list (``[]``)
    * Empty dictionary or set (``{}``)

    Given a query like ``SELECT COUNT(*) FROM foo``, it will fail only if
    the count equals to zero. You can craft much more complex query that could,
    for instance, check that the table has the same number of rows as the source
    table upstream, or that the count of today's partition is greater than
    yesterday's partition, or that a set of metrics are less than three standard
    deviation for the 7-day average.

    This operator can be used as a data quality check in your pipeline.
    Depending on where you put it in your DAG, you have the choice to stop the
    critical path, preventing from publishing dubious data, or on the side and
    receive email alerts without stopping the progress of the DAG.

    :param sql: SQL to execute.
    :param gcp_conn_id: Connection ID for Google Cloud.
    :param use_legacy_sql: Whether to use legacy SQL (true) or standard SQL (false).
    :param location: The geographic location of the job. See details at:
        https://cloud.google.com/bigquery/docs/locations#specifying_your_location
    :param impersonation_chain: Optional service account to impersonate using
        short-term credentials, or chained list of accounts required to get the
        access token of the last account in the list, which will be impersonated
        in the request. If set as a string, the account must grant the
        originating account the Service Account Token Creator IAM role. If set
        as a sequence, the identities from the list must grant Service Account
        Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account. (templated)
    :param labels: a dictionary containing labels for the table, passed to BigQuery.
    :param encryption_configuration: (Optional) Custom encryption configuration (e.g., Cloud KMS keys).

        .. code-block:: python

            encryption_configuration = {
                "kmsKeyName": "projects/PROJECT/locations/LOCATION/keyRings/KEY_RING/cryptoKeys/KEY",
            }
    :param deferrable: Run operator in the deferrable mode.
    :param poll_interval: (Deferrable mode only) polling period in seconds to
        check for the status of job.
    :param query_params: a list of dictionary containing query parameter types and
        values, passed to BigQuery. The structure of dictionary should look like
        'queryParameters' in Google BigQuery Jobs API:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs.
        For example, [{ 'name': 'corpus', 'parameterType': { 'type': 'STRING' },
        'parameterValue': { 'value': 'romeoandjuliet' } }]. (templated)
    :param project_id: Google Cloud Project where the job is running
    """

    template_fields: Sequence[str] = (
        "sql",
        "gcp_conn_id",
        "impersonation_chain",
        "labels",
        "query_params",
    )
    template_ext: Sequence[str] = (".sql",)
    ui_color = BigQueryUIColors.CHECK.value
    conn_id_field = "gcp_conn_id"

    def __init__(
        self,
        *,
        sql: str,
        gcp_conn_id: str = "google_cloud_default",
        project_id: str = PROVIDE_PROJECT_ID,
        use_legacy_sql: bool = True,
        location: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        labels: dict | None = None,
        encryption_configuration: dict | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poll_interval: float = 4.0,
        query_params: list | None = None,
        **kwargs,
    ) -> None:
        super().__init__(sql=sql, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.use_legacy_sql = use_legacy_sql
        self.location = location
        self.impersonation_chain = impersonation_chain
        self.labels = labels
        self.encryption_configuration = encryption_configuration
        self.deferrable = deferrable
        self.poll_interval = poll_interval
        self.query_params = query_params
        self.project_id = project_id

    def _submit_job(
        self,
        hook: BigQueryHook,
        job_id: str,
    ) -> BigQueryJob:
        """Submit a new job and get the job id for polling the status using Trigger."""
        configuration = {"query": {"query": self.sql, "useLegacySql": self.use_legacy_sql}}
        if self.query_params:
            configuration["query"]["queryParameters"] = self.query_params

        self.include_encryption_configuration(configuration, "query")

        return hook.insert_job(
            configuration=configuration,
            project_id=self.project_id,
            location=self.location,
            job_id=job_id,
            nowait=True,
        )

    def execute(self, context: Context):
        if not self.deferrable:
            super().execute(context=context)
        else:
            hook = BigQueryHook(
                gcp_conn_id=self.gcp_conn_id,
                impersonation_chain=self.impersonation_chain,
            )
            if self.project_id is None:
                self.project_id = hook.project_id
            job = self._submit_job(hook, job_id="")
            context["ti"].xcom_push(key="job_id", value=job.job_id)
            if job.running():
                self.defer(
                    timeout=self.execution_timeout,
                    trigger=BigQueryCheckTrigger(
                        conn_id=self.gcp_conn_id,
                        job_id=job.job_id,
                        project_id=self.project_id,
                        location=self.location or hook.location,
                        poll_interval=self.poll_interval,
                        impersonation_chain=self.impersonation_chain,
                    ),
                    method_name="execute_complete",
                )
            self._handle_job_error(job)
            # job.result() returns a RowIterator. Mypy expects an instance of SupportsNext[Any] for
            # the next() call which the RowIterator does not resemble to. Hence, ignore the arg-type error.
            # Row passed to _validate_records is a collection of values only, without column names.
            self._validate_records(next(iter(job.result()), []))  # type: ignore[arg-type]
            self.log.info("Current state of job %s is %s", job.job_id, job.state)

    @staticmethod
    def _handle_job_error(job: BigQueryJob | UnknownJob) -> None:
        if job.error_result:
            raise AirflowException(f"BigQuery job {job.job_id} failed: {job.error_result}")

    def _validate_records(self, records) -> None:
        if not records:
            raise AirflowException(f"The following query returned zero rows: {self.sql}")
        if not all(records):
            self._raise_exception(f"Test failed.\nQuery:\n{self.sql}\nResults:\n{records!s}")

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        """
        Act as a callback for when the trigger fires.

        This returns immediately. It relies on trigger to throw an exception,
        otherwise it assumes execution was successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])

        self._validate_records(event["records"])
        self.log.info("Record: %s", event["records"])
        self.log.info("Success.")


class BigQueryValueCheckOperator(
    _BigQueryDbHookMixin, SQLValueCheckOperator, _BigQueryOperatorsEncryptionConfigurationMixin
):
    """
    Perform a simple value check using sql code.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryValueCheckOperator`

    :param sql: SQL to execute.
    :param use_legacy_sql: Whether to use legacy SQL (true)
        or standard SQL (false).
    :param encryption_configuration: (Optional) Custom encryption configuration (e.g., Cloud KMS keys).

        .. code-block:: python

            encryption_configuration = {
                "kmsKeyName": "projects/PROJECT/locations/LOCATION/keyRings/KEY_RING/cryptoKeys/KEY",
            }
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param location: The geographic location of the job. See details at:
        https://cloud.google.com/bigquery/docs/locations#specifying_your_location
    :param impersonation_chain: Optional service account to impersonate using
        short-term credentials, or chained list of accounts required to get the
        access token of the last account in the list, which will be impersonated
        in the request. If set as a string, the account must grant the
        originating account the Service Account Token Creator IAM role. If set
        as a sequence, the identities from the list must grant Service Account
        Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account. (templated)
    :param labels: a dictionary containing labels for the table, passed to BigQuery.
    :param deferrable: Run operator in the deferrable mode.
    :param poll_interval: (Deferrable mode only) polling period in seconds to
        check for the status of job.
    :param project_id: Google Cloud Project where the job is running
    """

    template_fields: Sequence[str] = (
        "sql",
        "gcp_conn_id",
        "pass_value",
        "impersonation_chain",
        "labels",
    )
    template_ext: Sequence[str] = (".sql",)
    ui_color = BigQueryUIColors.CHECK.value
    conn_id_field = "gcp_conn_id"

    def __init__(
        self,
        *,
        sql: str,
        pass_value: Any,
        tolerance: Any = None,
        encryption_configuration: dict | None = None,
        gcp_conn_id: str = "google_cloud_default",
        project_id: str = PROVIDE_PROJECT_ID,
        use_legacy_sql: bool = True,
        location: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        labels: dict | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poll_interval: float = 4.0,
        **kwargs,
    ) -> None:
        super().__init__(sql=sql, pass_value=pass_value, tolerance=tolerance, **kwargs)
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.use_legacy_sql = use_legacy_sql
        self.encryption_configuration = encryption_configuration
        self.impersonation_chain = impersonation_chain
        self.labels = labels
        self.deferrable = deferrable
        self.poll_interval = poll_interval
        self.project_id = project_id

    def _submit_job(
        self,
        hook: BigQueryHook,
        job_id: str,
    ) -> BigQueryJob:
        """Submit a new job and get the job id for polling the status using Triggerer."""
        configuration = {
            "query": {
                "query": self.sql,
                "useLegacySql": self.use_legacy_sql,
            },
        }

        self.include_encryption_configuration(configuration, "query")

        return hook.insert_job(
            configuration=configuration,
            project_id=self.project_id,
            location=self.location,
            job_id=job_id,
            nowait=True,
        )

    def execute(self, context: Context) -> None:
        if not self.deferrable:
            super().execute(context=context)
        else:
            hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
            if self.project_id is None:
                self.project_id = hook.project_id
            job = self._submit_job(hook, job_id="")
            context["ti"].xcom_push(key="job_id", value=job.job_id)
            if job.running():
                self.defer(
                    timeout=self.execution_timeout,
                    trigger=BigQueryValueCheckTrigger(
                        conn_id=self.gcp_conn_id,
                        job_id=job.job_id,
                        project_id=self.project_id,
                        location=self.location or hook.location,
                        sql=self.sql,
                        pass_value=self.pass_value,
                        tolerance=self.tol,
                        poll_interval=self.poll_interval,
                        impersonation_chain=self.impersonation_chain,
                    ),
                    method_name="execute_complete",
                )
            self._handle_job_error(job)
            # job.result() returns a RowIterator. Mypy expects an instance of SupportsNext[Any] for
            # the next() call which the RowIterator does not resemble to. Hence, ignore the arg-type error.
            # Row passed to check_value is a collection of values only, without column names.
            self.check_value(next(iter(job.result()), []))  # type: ignore[arg-type]
            self.log.info("Current state of job %s is %s", job.job_id, job.state)

    @staticmethod
    def _handle_job_error(job: BigQueryJob | UnknownJob) -> None:
        if job.error_result:
            raise AirflowException(f"BigQuery job {job.job_id} failed: {job.error_result}")

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        """
        Act as a callback for when the trigger fires.

        This returns immediately. It relies on trigger to throw an exception,
        otherwise it assumes execution was successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info(
            "%s completed with response %s ",
            self.task_id,
            event["message"],
        )


class BigQueryIntervalCheckOperator(
    _BigQueryDbHookMixin, SQLIntervalCheckOperator, _BigQueryOperatorsEncryptionConfigurationMixin
):
    """
    Check that the values of metrics given as SQL expressions are within a tolerance of the older ones.

    This method constructs a query like so ::

        SELECT {metrics_threshold_dict_key} FROM {table}
        WHERE {date_filter_column}=<date>

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryIntervalCheckOperator`

    :param table: the table name
    :param days_back: number of days between ds and the ds we want to check
        against. Defaults to 7 days
    :param metrics_thresholds: a dictionary of ratios indexed by metrics, for
        example 'COUNT(*)': 1.5 would require a 50 percent or less difference
        between the current day, and the prior days_back.
    :param use_legacy_sql: Whether to use legacy SQL (true)
        or standard SQL (false).
    :param encryption_configuration: (Optional) Custom encryption configuration (e.g., Cloud KMS keys).

        .. code-block:: python

            encryption_configuration = {
                "kmsKeyName": "projects/PROJECT/locations/LOCATION/keyRings/KEY_RING/cryptoKeys/KEY",
            }
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param location: The geographic location of the job. See details at:
        https://cloud.google.com/bigquery/docs/locations#specifying_your_location
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param labels: a dictionary containing labels for the table, passed to BigQuery
    :param deferrable: Run operator in the deferrable mode
    :param poll_interval: (Deferrable mode only) polling period in seconds to check for the status of job.
        Defaults to 4 seconds.
    :param project_id: a string represents the BigQuery projectId
    """

    template_fields: Sequence[str] = (
        "table",
        "gcp_conn_id",
        "sql1",
        "sql2",
        "impersonation_chain",
        "labels",
    )
    ui_color = BigQueryUIColors.CHECK.value
    conn_id_field = "gcp_conn_id"

    def __init__(
        self,
        *,
        table: str,
        metrics_thresholds: dict,
        date_filter_column: str = "ds",
        days_back: SupportsAbs[int] = -7,
        gcp_conn_id: str = "google_cloud_default",
        use_legacy_sql: bool = True,
        location: str | None = None,
        encryption_configuration: dict | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        labels: dict | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poll_interval: float = 4.0,
        project_id: str = PROVIDE_PROJECT_ID,
        **kwargs,
    ) -> None:
        super().__init__(
            table=table,
            metrics_thresholds=metrics_thresholds,
            date_filter_column=date_filter_column,
            days_back=days_back,
            **kwargs,
        )

        self.gcp_conn_id = gcp_conn_id
        self.use_legacy_sql = use_legacy_sql
        self.location = location
        self.encryption_configuration = encryption_configuration
        self.impersonation_chain = impersonation_chain
        self.labels = labels
        self.project_id = project_id
        self.deferrable = deferrable
        self.poll_interval = poll_interval

    def _submit_job(
        self,
        hook: BigQueryHook,
        sql: str,
        job_id: str,
    ) -> BigQueryJob:
        """Submit a new job and get the job id for polling the status using Triggerer."""
        configuration = {"query": {"query": sql, "useLegacySql": self.use_legacy_sql}}
        self.include_encryption_configuration(configuration, "query")
        return hook.insert_job(
            configuration=configuration,
            project_id=self.project_id or hook.project_id,
            location=self.location,
            job_id=job_id,
            nowait=True,
        )

    def execute(self, context: Context):
        if not self.deferrable:
            super().execute(context)
        else:
            hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
            self.log.info("Using ratio formula: %s", self.ratio_formula)

            if self.project_id is None:
                self.project_id = hook.project_id

            self.log.info("Executing SQL check: %s", self.sql1)
            job_1 = self._submit_job(hook, sql=self.sql1, job_id="")
            context["ti"].xcom_push(key="job_id", value=job_1.job_id)

            self.log.info("Executing SQL check: %s", self.sql2)
            job_2 = self._submit_job(hook, sql=self.sql2, job_id="")
            self.defer(
                timeout=self.execution_timeout,
                trigger=BigQueryIntervalCheckTrigger(
                    conn_id=self.gcp_conn_id,
                    first_job_id=job_1.job_id,
                    second_job_id=job_2.job_id,
                    project_id=self.project_id,
                    table=self.table,
                    location=self.location or hook.location,
                    metrics_thresholds=self.metrics_thresholds,
                    date_filter_column=self.date_filter_column,
                    days_back=self.days_back,
                    ratio_formula=self.ratio_formula,
                    ignore_zero=self.ignore_zero,
                    poll_interval=self.poll_interval,
                    impersonation_chain=self.impersonation_chain,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        """
        Act as a callback for when the trigger fires.

        This returns immediately. It relies on trigger to throw an exception,
        otherwise it assumes execution was successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info(
            "%s completed with response %s ",
            self.task_id,
            event["message"],
        )


class BigQueryColumnCheckOperator(
    _BigQueryDbHookMixin, SQLColumnCheckOperator, _BigQueryOperatorsEncryptionConfigurationMixin
):
    """
    Subclasses the SQLColumnCheckOperator in order to provide a job id for OpenLineage to parse.

    See base class docstring for usage.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryColumnCheckOperator`

    :param table: the table name
    :param column_mapping: a dictionary relating columns to their checks
    :param partition_clause: a string SQL statement added to a WHERE clause
        to partition data
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param encryption_configuration: (Optional) Custom encryption configuration (e.g., Cloud KMS keys).

        .. code-block:: python

            encryption_configuration = {
                "kmsKeyName": "projects/PROJECT/locations/LOCATION/keyRings/KEY_RING/cryptoKeys/KEY",
            }
    :param use_legacy_sql: Whether to use legacy SQL (true)
        or standard SQL (false).
    :param location: The geographic location of the job. See details at:
        https://cloud.google.com/bigquery/docs/locations#specifying_your_location
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param labels: a dictionary containing labels for the table, passed to BigQuery
    :param project_id: Google Cloud Project where the job is running
    """

    template_fields: Sequence[str] = tuple(set(SQLColumnCheckOperator.template_fields) | {"gcp_conn_id"})

    conn_id_field = "gcp_conn_id"

    def __init__(
        self,
        *,
        table: str,
        column_mapping: dict,
        partition_clause: str | None = None,
        database: str | None = None,
        accept_none: bool = True,
        encryption_configuration: dict | None = None,
        gcp_conn_id: str = "google_cloud_default",
        project_id: str = PROVIDE_PROJECT_ID,
        use_legacy_sql: bool = True,
        location: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        labels: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            table=table,
            column_mapping=column_mapping,
            partition_clause=partition_clause,
            database=database,
            accept_none=accept_none,
            **kwargs,
        )
        self.table = table
        self.column_mapping = column_mapping
        self.partition_clause = partition_clause
        self.database = database
        self.accept_none = accept_none
        self.gcp_conn_id = gcp_conn_id
        self.encryption_configuration = encryption_configuration
        self.use_legacy_sql = use_legacy_sql
        self.location = location
        self.impersonation_chain = impersonation_chain
        self.labels = labels
        self.project_id = project_id

    def _submit_job(
        self,
        hook: BigQueryHook,
        job_id: str,
    ) -> BigQueryJob:
        """Submit a new job and get the job id for polling the status using Trigger."""
        configuration = {"query": {"query": self.sql, "useLegacySql": self.use_legacy_sql}}
        self.include_encryption_configuration(configuration, "query")
        return hook.insert_job(
            configuration=configuration,
            project_id=self.project_id,
            location=self.location,
            job_id=job_id,
            nowait=False,
        )

    def execute(self, context=None):
        """Perform checks on the given columns."""
        hook = self.get_db_hook()

        if self.project_id is None:
            self.project_id = hook.project_id
        failed_tests = []

        job = self._submit_job(hook, job_id="")
        context["ti"].xcom_push(key="job_id", value=job.job_id)
        records = job.result().to_dataframe()

        if records.empty:
            raise AirflowException(f"The following query returned zero rows: {self.sql}")

        records.columns = records.columns.str.lower()
        self.log.info("Record: %s", records)

        for row in records.iterrows():
            column = row[1].get("col_name")
            check = row[1].get("check_type")
            result = row[1].get("check_result")
            tolerance = self.column_mapping[column][check].get("tolerance")

            self.column_mapping[column][check]["result"] = result
            self.column_mapping[column][check]["success"] = self._get_match(
                self.column_mapping[column][check], result, tolerance
            )

        failed_tests.extend(
            f"Column: {col}\n\tCheck: {check},\n\tCheck Values: {check_values}\n"
            for col, checks in self.column_mapping.items()
            for check, check_values in checks.items()
            if not check_values["success"]
        )
        if failed_tests:
            exception_string = (
                f"Test failed.\nResults:\n{records!s}\n"
                f"The following tests have failed:"
                f"\n{''.join(failed_tests)}"
            )
            self._raise_exception(exception_string)

        self.log.info("All tests have passed")


class BigQueryTableCheckOperator(
    _BigQueryDbHookMixin, SQLTableCheckOperator, _BigQueryOperatorsEncryptionConfigurationMixin
):
    """
    Subclasses the SQLTableCheckOperator in order to provide a job id for OpenLineage to parse.

    See base class for usage.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryTableCheckOperator`

    :param table: the table name
    :param checks: a dictionary of check names and boolean SQL statements
    :param partition_clause: a string SQL statement added to a WHERE clause
        to partition data
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param use_legacy_sql: Whether to use legacy SQL (true)
        or standard SQL (false).
    :param location: The geographic location of the job. See details at:
        https://cloud.google.com/bigquery/docs/locations#specifying_your_location
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param labels: a dictionary containing labels for the table, passed to BigQuery
    :param encryption_configuration: (Optional) Custom encryption configuration (e.g., Cloud KMS keys).
    :param project_id: Google Cloud Project where the job is running

        .. code-block:: python

            encryption_configuration = {
                "kmsKeyName": "projects/PROJECT/locations/LOCATION/keyRings/KEY_RING/cryptoKeys/KEY",
            }
    """

    template_fields: Sequence[str] = tuple(set(SQLTableCheckOperator.template_fields) | {"gcp_conn_id"})

    conn_id_field = "gcp_conn_id"

    def __init__(
        self,
        *,
        table: str,
        checks: dict,
        partition_clause: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        project_id: str = PROVIDE_PROJECT_ID,
        use_legacy_sql: bool = True,
        location: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        labels: dict | None = None,
        encryption_configuration: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(table=table, checks=checks, partition_clause=partition_clause, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.use_legacy_sql = use_legacy_sql
        self.location = location
        self.impersonation_chain = impersonation_chain
        self.labels = labels
        self.encryption_configuration = encryption_configuration
        self.project_id = project_id

    def _submit_job(
        self,
        hook: BigQueryHook,
        job_id: str,
    ) -> BigQueryJob:
        """Submit a new job and get the job id for polling the status using Trigger."""
        configuration = {"query": {"query": self.sql, "useLegacySql": self.use_legacy_sql}}

        self.include_encryption_configuration(configuration, "query")

        return hook.insert_job(
            configuration=configuration,
            project_id=self.project_id,
            location=self.location,
            job_id=job_id,
            nowait=False,
        )

    def execute(self, context=None):
        """Execute the given checks on the table."""
        hook = self.get_db_hook()
        if self.project_id is None:
            self.project_id = hook.project_id
        job = self._submit_job(hook, job_id="")
        context["ti"].xcom_push(key="job_id", value=job.job_id)
        records = job.result().to_dataframe()

        if records.empty:
            raise AirflowException(f"The following query returned zero rows: {self.sql}")

        records.columns = records.columns.str.lower()
        self.log.info("Record:\n%s", records)

        for row in records.iterrows():
            check = row[1].get("check_name")
            result = row[1].get("check_result")
            self.checks[check]["success"] = _parse_boolean(str(result))

        failed_tests = [
            f"\tCheck: {check},\n\tCheck Values: {check_values}\n"
            for check, check_values in self.checks.items()
            if not check_values["success"]
        ]
        if failed_tests:
            exception_string = (
                f"Test failed.\nQuery:\n{self.sql}\nResults:\n{records!s}\n"
                f"The following tests have failed:\n{', '.join(failed_tests)}"
            )
            self._raise_exception(exception_string)

        self.log.info("All tests have passed")


class BigQueryGetDataOperator(GoogleCloudBaseOperator, _BigQueryOperatorsEncryptionConfigurationMixin):
    """
    Fetch data and return it, either from a BigQuery table, or results of a query job.

    Data could be narrowed down by specific columns or retrieved as a whole.
    It is returned in either of the following two formats, based on "as_dict" value:
    1. False (Default) - A Python list of lists, with the number of nested lists equal to the number of rows
    fetched. Each nested list represents a row, where the elements within it correspond to the column values
    for that particular row.

    **Example Result**: ``[['Tony', 10], ['Mike', 20]``


    2. True - A Python list of dictionaries, where each dictionary represents a row. In each dictionary,
    the keys are the column names and the values are the corresponding values for those columns.

    **Example Result**: ``[{'name': 'Tony', 'age': 10}, {'name': 'Mike', 'age': 20}]``

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryGetDataOperator`

    .. note::
        If you pass fields to ``selected_fields`` which are in different order than the
        order of columns already in
        BQ table/job, the data will still be in the order of BQ table.
        For example if the BQ table has 3 columns as
        ``[A,B,C]`` and you pass 'B,A' in the ``selected_fields``
        the data would still be of the form ``'A,B'``.

    .. note::
        When utilizing job id not in deferrable mode, the job should be in DONE state.

    **Example - Retrieve data from BigQuery using table**::

        get_data = BigQueryGetDataOperator(
            task_id="get_data_from_bq",
            dataset_id="test_dataset",
            table_id="Transaction_partitions",
            table_project_id="internal-gcp-project",
            max_results=100,
            selected_fields="DATE",
            gcp_conn_id="airflow-conn-id",
        )

    **Example - Retrieve data from BigQuery using a job id**::

        get_data = BigQueryGetDataOperator(
            job_id="airflow_8999918812727394_86a1cecc69c5e3028d28247affd7563",
            job_project_id="internal-gcp-project",
            max_results=100,
            selected_fields="DATE",
            gcp_conn_id="airflow-conn-id",
        )

    :param dataset_id: The dataset ID of the requested table. (templated)
    :param table_id: The table ID of the requested table. Mutually exclusive with job_id. (templated)
    :param table_project_id: (Optional) The project ID of the requested table.
        If None, it will be derived from the hook's project ID. (templated)
    :param job_id: The job ID from which query results are retrieved.
        Mutually exclusive with table_id. (templated)
    :param job_project_id: (Optional) Google Cloud Project where the job is running.
        If None, it will be derived from the hook's project ID. (templated)
    :param project_id: (Deprecated) (Optional) The name of the project where the data
        will be returned from. If None, it will be derived from the hook's project ID. (templated)
    :param max_results: The maximum number of records (rows) to be fetched
        from the table. (templated)
    :param selected_fields: List of fields to return (comma-separated). If
        unspecified, all fields are returned.
    :param encryption_configuration: (Optional) Custom encryption configuration (e.g., Cloud KMS keys).

        .. code-block:: python

            encryption_configuration = {
                "kmsKeyName": "projects/PROJECT/locations/LOCATION/keyRings/KEY_RING/cryptoKeys/KEY",
            }
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param location: The location used for the operation.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param deferrable: Run operator in the deferrable mode
    :param poll_interval: (Deferrable mode only) polling period in seconds to check for the status of job.
        Defaults to 4 seconds.
    :param as_dict: if True returns the result as a list of dictionaries, otherwise as list of lists
        (default: False).
    :param use_legacy_sql: Whether to use legacy SQL (true) or standard SQL (false).
    """

    template_fields: Sequence[str] = (
        "dataset_id",
        "table_id",
        "table_project_id",
        "job_id",
        "job_project_id",
        "project_id",
        "max_results",
        "selected_fields",
        "gcp_conn_id",
        "impersonation_chain",
    )
    ui_color = BigQueryUIColors.QUERY.value

    def __init__(
        self,
        *,
        dataset_id: str | None = None,
        table_id: str | None = None,
        table_project_id: str | None = None,
        job_id: str | None = None,
        job_project_id: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        max_results: int = 100,
        selected_fields: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        location: str | None = None,
        encryption_configuration: dict | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poll_interval: float = 4.0,
        as_dict: bool = False,
        use_legacy_sql: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.table_project_id = table_project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.job_project_id = job_project_id
        self.job_id = job_id
        self.max_results = max_results
        self.selected_fields = selected_fields
        self.gcp_conn_id = gcp_conn_id
        self.location = location
        self.impersonation_chain = impersonation_chain
        self.encryption_configuration = encryption_configuration
        self.project_id = project_id
        self.deferrable = deferrable
        self.poll_interval = poll_interval
        self.as_dict = as_dict
        self.use_legacy_sql = use_legacy_sql

    def _submit_job(
        self,
        hook: BigQueryHook,
        job_id: str,
    ) -> BigQueryJob:
        get_query = self.generate_query(hook=hook)
        configuration = {"query": {"query": get_query, "useLegacySql": self.use_legacy_sql}}
        self.include_encryption_configuration(configuration, "query")

        """Submit a new job and get the job id for polling the status using Triggerer."""
        return hook.insert_job(
            configuration=configuration,
            location=self.location,
            project_id=self.job_project_id or hook.project_id,
            job_id=job_id,
            nowait=True,
        )

    def generate_query(self, hook: BigQueryHook) -> str:
        """Generate a SELECT query if for the given dataset and table ID."""
        query = "select "
        if self.selected_fields:
            query += self.selected_fields
        else:
            query += "*"
        query += (
            f" from `{self.table_project_id or hook.project_id}.{self.dataset_id}"
            f".{self.table_id}` limit {self.max_results}"
        )
        return query

    """Deprecated method to assign project_id to table_project_id."""

    @deprecated(
        planned_removal_date="June 30, 2026",
        use_instead="table_project_id",
        category=AirflowProviderDeprecationWarning,
    )
    def _assign_project_id(self, project_id: str) -> str:
        return project_id

    def execute(self, context: Context):
        if self.project_id != PROVIDE_PROJECT_ID and not self.table_project_id:
            self.table_project_id = self._assign_project_id(self.project_id)
        elif self.project_id != PROVIDE_PROJECT_ID and self.table_project_id:
            self.log.info("Ignoring 'project_id' parameter, as 'table_project_id' is found.")
        if not exactly_one(self.job_id, self.table_id):
            raise AirflowException(
                "'job_id' and 'table_id' parameters are mutually exclusive, "
                "ensure that exactly one of them is specified"
            )

        hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            use_legacy_sql=self.use_legacy_sql,
        )

        if not self.deferrable:
            if not self.job_id:
                self.log.info(
                    "Fetching Data from %s.%s.%s max results: %s",
                    self.table_project_id or hook.project_id,
                    self.dataset_id,
                    self.table_id,
                    self.max_results,
                )
                if not self.selected_fields:
                    schema: dict[str, list] = hook.get_schema(
                        dataset_id=self.dataset_id,
                        table_id=self.table_id,
                        project_id=self.table_project_id or hook.project_id,
                    )
                    if "fields" in schema:
                        self.selected_fields = ",".join([field["name"] for field in schema["fields"]])
                rows: list[Row] | RowIterator | list[dict[str, Any]] = hook.list_rows(
                    dataset_id=self.dataset_id,
                    table_id=self.table_id,
                    max_results=self.max_results,
                    selected_fields=self.selected_fields,
                    location=self.location,
                    project_id=self.table_project_id or hook.project_id,
                )
            else:
                self.log.info(
                    "Fetching data from job '%s:%s.%s' max results: %s",
                    self.job_project_id or hook.project_id,
                    self.location,
                    self.job_id,
                    self.max_results,
                )
                rows = hook.get_query_results(
                    job_id=self.job_id,
                    location=self.location,
                    selected_fields=self.selected_fields,
                    max_results=self.max_results,
                    project_id=self.job_project_id or hook.project_id,
                )
            if isinstance(rows, RowIterator):
                raise TypeError(
                    "BigQueryHook.list_rows() returns iterator when return_iterator is False (default)"
                )
            self.log.info("Total extracted rows: %s", len(rows))
            table_data: list[dict[str, Any]] | list[Any]
            if self.as_dict:
                table_data = [dict(row) for row in rows]
            else:
                table_data = [row.values() if isinstance(row, Row) else list(row.values()) for row in rows]

            return table_data

        if not self.job_id:
            job: BigQueryJob | UnknownJob = self._submit_job(hook, job_id="")
        else:
            job = hook.get_job(
                job_id=self.job_id, project_id=self.job_project_id or hook.project_id, location=self.location
            )

        context["ti"].xcom_push(key="job_id", value=job.job_id)
        self.defer(
            timeout=self.execution_timeout,
            trigger=BigQueryGetDataTrigger(
                conn_id=self.gcp_conn_id,
                job_id=job.job_id,
                dataset_id=self.dataset_id,
                table_id=self.table_id,
                project_id=self.job_project_id or hook.project_id,
                location=self.location or hook.location,
                poll_interval=self.poll_interval,
                as_dict=self.as_dict,
                impersonation_chain=self.impersonation_chain,
                selected_fields=self.selected_fields,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> Any:
        """
        Act as a callback for when the trigger fires.

        This returns immediately. It relies on trigger to throw an exception,
        otherwise it assumes execution was successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])

        self.log.info("Total extracted rows: %s", len(event["records"]))
        return event["records"]


class BigQueryCreateTableOperator(GoogleCloudBaseOperator):
    """
    Creates a new table in the specified BigQuery dataset, optionally with schema.

    The schema to be used for the BigQuery table may be specified in one of
    two ways. You may either directly pass the schema fields in, or you may
    point the operator to a Google Cloud Storage object name. The object in
    Google Cloud Storage must be a JSON file with the schema fields in it.
    You can also create a table without schema.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryCreateTableOperator`

    :param project_id: Optional. The project to create the table into.
    :param dataset_id: Required. The dataset to create the table into.
    :param table_id: Required. The Name of the table to be created.
    :param table_resource: Required. Table resource as described in documentation:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#Table
        If ``table`` is a reference, an empty table is created with the specified ID. The dataset that
        the table belongs to must already exist.
    :param if_exists: Optional. What should Airflow do if the table exists. If set to `log`,
        the TI will be passed to success and an error message will be logged. Set to `ignore` to ignore
        the error, set to `fail` to fail the TI, and set to `skip` to skip it.
    :param gcs_schema_object: Optional. Full path to the JSON file containing schema. For
        example: ``gs://test-bucket/dir1/dir2/employee_schema.json``
    :param gcp_conn_id: Optional. The connection ID used to connect to Google Cloud and
        interact with the Bigquery service.
    :param google_cloud_storage_conn_id: Optional. The connection ID used to connect to Google Cloud.
        and interact with the Google Cloud Storage service.
    :param location: Optional. The location used for the operation.
    :param retry: Optional. A retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    """

    template_fields: Sequence[str] = (
        "dataset_id",
        "table_id",
        "table_resource",
        "project_id",
        "gcs_schema_object",
        "gcp_conn_id",
        "impersonation_chain",
    )
    template_fields_renderers = {"table_resource": "json"}
    ui_color = BigQueryUIColors.TABLE.value
    operator_extra_links = (BigQueryTableLink(),)

    def __init__(
        self,
        *,
        dataset_id: str,
        table_id: str,
        table_resource: dict[str, Any] | Table | TableReference | TableListItem,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str | None = None,
        gcs_schema_object: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        google_cloud_storage_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        if_exists: str = "log",
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.table_resource = table_resource
        self.if_exists = IfExistAction(if_exists)
        self.gcs_schema_object = gcs_schema_object
        self.gcp_conn_id = gcp_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.impersonation_chain = impersonation_chain
        self.retry = retry
        self.timeout = timeout
        self._table: Table | None = None

    def execute(self, context: Context) -> None:
        bq_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )
        if self.gcs_schema_object:
            gcs_bucket, gcs_object = _parse_gcs_url(self.gcs_schema_object)
            gcs_hook = GCSHook(
                gcp_conn_id=self.google_cloud_storage_conn_id,
                impersonation_chain=self.impersonation_chain,
            )
            schema_fields_string = gcs_hook.download_as_byte_array(gcs_bucket, gcs_object).decode("utf-8")
            schema_fields = json.loads(schema_fields_string)
        else:
            schema_fields = None

        try:
            self.log.info("Creating table...")
            self._table = bq_hook.create_table(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                table_id=self.table_id,
                schema_fields=schema_fields,
                table_resource=self.table_resource,
                exists_ok=self.if_exists == IfExistAction.IGNORE,
                timeout=self.timeout,
                location=self.location,
            )
            if self._table:
                persist_kwargs = {
                    "context": context,
                    "project_id": self._table.to_api_repr()["tableReference"]["projectId"],
                    "dataset_id": self._table.to_api_repr()["tableReference"]["datasetId"],
                    "table_id": self._table.to_api_repr()["tableReference"]["tableId"],
                }
                self.log.info(
                    "Table %s.%s.%s created successfully",
                    self._table.project,
                    self._table.dataset_id,
                    self._table.table_id,
                )
            else:
                raise AirflowException("Table creation failed.")
        except Conflict:
            error_msg = f"Table {self.dataset_id}.{self.table_id} already exists."
            if self.if_exists == IfExistAction.LOG:
                self.log.info(error_msg)
                persist_kwargs = {
                    "context": context,
                    "project_id": self.project_id or bq_hook.project_id,
                    "dataset_id": self.dataset_id,
                    "table_id": self.table_id,
                }
            elif self.if_exists == IfExistAction.FAIL:
                raise AirflowException(error_msg)
            else:
                raise AirflowSkipException(error_msg)

        BigQueryTableLink.persist(**persist_kwargs)

    def get_openlineage_facets_on_complete(self, _):
        """Implement _on_complete as we will use table resource returned by create method."""
        from airflow.providers.common.compat.openlineage.facet import Dataset
        from airflow.providers.google.cloud.openlineage.utils import (
            BIGQUERY_NAMESPACE,
            get_facets_from_bq_table,
        )
        from airflow.providers.openlineage.extractors import OperatorLineage

        if not self._table:
            self.log.debug("OpenLineage did not find `self._table` attribute.")
            return OperatorLineage()

        output_dataset = Dataset(
            namespace=BIGQUERY_NAMESPACE,
            name=f"{self._table.project}.{self._table.dataset_id}.{self._table.table_id}",
            facets=get_facets_from_bq_table(self._table),
        )

        return OperatorLineage(outputs=[output_dataset])


class BigQueryDeleteDatasetOperator(GoogleCloudBaseOperator):
    """
    Delete an existing dataset from your Project in BigQuery.

    https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/delete

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryDeleteDatasetOperator`

    :param project_id: The project id of the dataset.
    :param dataset_id: The dataset to be deleted.
    :param delete_contents: (Optional) Whether to force the deletion even if the dataset is not empty.
        Will delete all tables (if any) in the dataset if set to True.
        Will raise HttpError 400: "{dataset_id} is still in use" if set to False and dataset is not empty.
        The default value is False.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    **Example**::

        delete_temp_data = BigQueryDeleteDatasetOperator(
            dataset_id="temp-dataset",
            project_id="temp-project",
            delete_contents=True,  # Force the deletion of the dataset as well as its tables (if any).
            gcp_conn_id="_my_gcp_conn_",
            task_id="Deletetemp",
            dag=dag,
        )
    """

    template_fields: Sequence[str] = (
        "dataset_id",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )
    ui_color = BigQueryUIColors.DATASET.value

    def __init__(
        self,
        *,
        dataset_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        delete_contents: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.delete_contents = delete_contents
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

        super().__init__(**kwargs)

    def execute(self, context: Context) -> None:
        self.log.info("Dataset id: %s Project id: %s", self.dataset_id, self.project_id)

        bq_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        bq_hook.delete_dataset(
            project_id=self.project_id, dataset_id=self.dataset_id, delete_contents=self.delete_contents
        )


class BigQueryCreateEmptyDatasetOperator(GoogleCloudBaseOperator):
    """
    Create a new dataset for your Project in BigQuery.

    https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryCreateEmptyDatasetOperator`

    :param project_id: The name of the project where we want to create the dataset.
    :param dataset_id: The id of dataset. Don't need to provide, if datasetId in dataset_reference.
    :param location: The geographic location where the dataset should reside.
    :param dataset_reference: Dataset reference that could be provided with request body.
        More info:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param if_exists: What should Airflow do if the dataset exists. If set to `log`, the TI will be passed to
        success and an error message will be logged. Set to `ignore` to ignore the error, set to `fail` to
        fail the TI, and set to `skip` to skip it.
        **Example**::

            create_new_dataset = BigQueryCreateEmptyDatasetOperator(
                dataset_id='new-dataset',
                project_id='my-project',
                dataset_reference={"friendlyName": "New Dataset"}
                gcp_conn_id='_my_gcp_conn_',
                task_id='newDatasetCreator',
                dag=dag)
    :param exists_ok: Deprecated - use `if_exists="ignore"` instead.
    """

    template_fields: Sequence[str] = (
        "dataset_id",
        "project_id",
        "dataset_reference",
        "gcp_conn_id",
        "impersonation_chain",
    )
    template_fields_renderers = {"dataset_reference": "json"}
    ui_color = BigQueryUIColors.DATASET.value
    operator_extra_links = (BigQueryDatasetLink(),)

    def __init__(
        self,
        *,
        dataset_id: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        dataset_reference: dict | None = None,
        location: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        if_exists: str = "log",
        exists_ok: bool | None = None,
        **kwargs,
    ) -> None:
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.dataset_reference = dataset_reference or {}
        self.impersonation_chain = impersonation_chain
        if exists_ok is not None:
            warnings.warn(
                "`exists_ok` parameter is deprecated, please use `if_exists`",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            self.if_exists = IfExistAction.IGNORE if exists_ok else IfExistAction.LOG
        else:
            self.if_exists = IfExistAction(if_exists)

        super().__init__(**kwargs)

    def execute(self, context: Context) -> None:
        bq_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )

        try:
            dataset = bq_hook.create_empty_dataset(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                dataset_reference=self.dataset_reference,
                location=self.location,
                exists_ok=self.if_exists == IfExistAction.IGNORE,
            )
            persist_kwargs = {
                "context": context,
                "project_id": dataset["datasetReference"]["projectId"],
                "dataset_id": dataset["datasetReference"]["datasetId"],
            }

        except Conflict:
            dataset_id = self.dataset_reference.get("datasetReference", {}).get("datasetId", self.dataset_id)
            project_id = self.dataset_reference.get("datasetReference", {}).get(
                "projectId", self.project_id or bq_hook.project_id
            )
            persist_kwargs = {
                "context": context,
                "project_id": project_id,
                "dataset_id": dataset_id,
            }
            error_msg = f"Dataset {dataset_id} already exists."
            if self.if_exists == IfExistAction.LOG:
                self.log.info(error_msg)
            elif self.if_exists == IfExistAction.FAIL:
                raise AirflowException(error_msg)
            else:
                raise AirflowSkipException(error_msg)
        BigQueryDatasetLink.persist(**persist_kwargs)


class BigQueryGetDatasetOperator(GoogleCloudBaseOperator):
    """
    Get the dataset specified by ID.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryGetDatasetOperator`

    :param dataset_id: The id of dataset. Don't need to provide,
        if datasetId in dataset_reference.
    :param project_id: The name of the project where we want to create the dataset.
        Don't need to provide, if projectId in dataset_reference.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "dataset_id",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )
    ui_color = BigQueryUIColors.DATASET.value
    operator_extra_links = (BigQueryDatasetLink(),)

    def __init__(
        self,
        *,
        dataset_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        super().__init__(**kwargs)

    def execute(self, context: Context):
        bq_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Start getting dataset: %s:%s", self.project_id, self.dataset_id)

        dataset = bq_hook.get_dataset(dataset_id=self.dataset_id, project_id=self.project_id)
        dataset_api_repr = dataset.to_api_repr()
        BigQueryDatasetLink.persist(
            context=context,
            dataset_id=dataset_api_repr["datasetReference"]["datasetId"],
            project_id=dataset_api_repr["datasetReference"]["projectId"],
        )
        return dataset_api_repr


class BigQueryGetDatasetTablesOperator(GoogleCloudBaseOperator):
    """
    Retrieve the list of tables in the specified dataset.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryGetDatasetTablesOperator`

    :param dataset_id: the dataset ID of the requested dataset.
    :param project_id: (Optional) the project of the requested dataset. If None,
        self.project_id will be used.
    :param max_results: (Optional) the maximum number of tables to return.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "dataset_id",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )
    ui_color = BigQueryUIColors.DATASET.value

    def __init__(
        self,
        *,
        dataset_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        max_results: int | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.max_results = max_results
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        super().__init__(**kwargs)

    def execute(self, context: Context):
        bq_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        return bq_hook.get_dataset_tables(
            dataset_id=self.dataset_id,
            project_id=self.project_id,
            max_results=self.max_results,
        )


class BigQueryUpdateTableOperator(GoogleCloudBaseOperator):
    """
    Update a table for your Project in BigQuery.

    Use ``fields`` to specify which fields of table to update. If a field
    is listed in ``fields`` and is ``None`` in table, it will be deleted.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryUpdateTableOperator`

    :param dataset_id: The id of dataset. Don't need to provide,
        if datasetId in table_reference.
    :param table_id: The id of table. Don't need to provide,
        if tableId in table_reference.
    :param table_resource: Dataset resource that will be provided with request body.
        https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#resource
    :param fields: The fields of ``table`` to change, spelled as the Table
        properties (e.g. "friendly_name").
    :param project_id: The name of the project where we want to create the table.
        Don't need to provide, if projectId in table_reference.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "dataset_id",
        "table_id",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )
    template_fields_renderers = {"table_resource": "json"}
    ui_color = BigQueryUIColors.TABLE.value
    operator_extra_links = (BigQueryTableLink(),)

    def __init__(
        self,
        *,
        table_resource: dict[str, Any],
        fields: list[str] | None = None,
        dataset_id: str | None = None,
        table_id: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.project_id = project_id
        self.fields = fields
        self.gcp_conn_id = gcp_conn_id
        self.table_resource = table_resource
        self.impersonation_chain = impersonation_chain
        self._table: dict | None = None
        super().__init__(**kwargs)

    def execute(self, context: Context):
        bq_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        # Save table as attribute for further use by OpenLineage
        self._table = bq_hook.update_table(
            table_resource=self.table_resource,
            fields=self.fields,
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            project_id=self.project_id,
        )

        if self._table:
            BigQueryTableLink.persist(
                context=context,
                dataset_id=self._table["tableReference"]["datasetId"],
                project_id=self._table["tableReference"]["projectId"],
                table_id=self._table["tableReference"]["tableId"],
            )

        return self._table

    def get_openlineage_facets_on_complete(self, _):
        """Implement _on_complete as we will use table resource returned by update method."""
        from airflow.providers.common.compat.openlineage.facet import Dataset
        from airflow.providers.google.cloud.openlineage.utils import (
            BIGQUERY_NAMESPACE,
            get_facets_from_bq_table,
        )
        from airflow.providers.openlineage.extractors import OperatorLineage

        table = Table.from_api_repr(self._table)
        output_dataset = Dataset(
            namespace=BIGQUERY_NAMESPACE,
            name=f"{table.project}.{table.dataset_id}.{table.table_id}",
            facets=get_facets_from_bq_table(table),
        )

        return OperatorLineage(outputs=[output_dataset])


class BigQueryUpdateDatasetOperator(GoogleCloudBaseOperator):
    """
    Update a dataset for your Project in BigQuery.

    Use ``fields`` to specify which fields of dataset to update. If a field
    is listed in ``fields`` and is ``None`` in dataset, it will be deleted.
    If no ``fields`` are provided then all fields of provided ``dataset_resource``
    will be used.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryUpdateDatasetOperator`

    :param dataset_id: The id of dataset. Don't need to provide,
        if datasetId in dataset_reference.
    :param dataset_resource: Dataset resource that will be provided with request body.
        https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
    :param fields: The properties of dataset to change (e.g. "friendly_name").
    :param project_id: The name of the project where we want to create the dataset.
        Don't need to provide, if projectId in dataset_reference.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "dataset_id",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )
    template_fields_renderers = {"dataset_resource": "json"}
    ui_color = BigQueryUIColors.DATASET.value
    operator_extra_links = (BigQueryDatasetLink(),)

    def __init__(
        self,
        *,
        dataset_resource: dict[str, Any],
        fields: list[str] | None = None,
        dataset_id: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.fields = fields
        self.gcp_conn_id = gcp_conn_id
        self.dataset_resource = dataset_resource
        self.impersonation_chain = impersonation_chain
        super().__init__(**kwargs)

    def execute(self, context: Context):
        bq_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        fields = self.fields or list(self.dataset_resource.keys())

        dataset = bq_hook.update_dataset(
            dataset_resource=self.dataset_resource,
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            fields=fields,
        )

        dataset_api_repr = dataset.to_api_repr()
        BigQueryDatasetLink.persist(
            context=context,
            dataset_id=dataset_api_repr["datasetReference"]["datasetId"],
            project_id=dataset_api_repr["datasetReference"]["projectId"],
        )
        return dataset_api_repr


class BigQueryDeleteTableOperator(GoogleCloudBaseOperator):
    """
    Delete a BigQuery table.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryDeleteTableOperator`

    :param deletion_dataset_table: A dotted
        ``(<project>.|<project>:)<dataset>.<table>`` that indicates which table
        will be deleted. (templated)
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param ignore_if_missing: if True, then return success even if the
        requested table does not exist.
    :param location: The location used for the operation.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "deletion_dataset_table",
        "gcp_conn_id",
        "impersonation_chain",
    )
    ui_color = BigQueryUIColors.TABLE.value

    def __init__(
        self,
        *,
        deletion_dataset_table: str,
        gcp_conn_id: str = "google_cloud_default",
        ignore_if_missing: bool = False,
        location: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.deletion_dataset_table = deletion_dataset_table
        self.gcp_conn_id = gcp_conn_id
        self.ignore_if_missing = ignore_if_missing
        self.location = location
        self.impersonation_chain = impersonation_chain
        self.hook: BigQueryHook | None = None

    def execute(self, context: Context) -> None:
        self.log.info("Deleting: %s", self.deletion_dataset_table)
        # Save hook as attribute for further use by OpenLineage
        self.hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )
        self.hook.delete_table(table_id=self.deletion_dataset_table, not_found_ok=self.ignore_if_missing)

    def get_openlineage_facets_on_complete(self, _):
        """Implement _on_complete as we need default project_id from hook."""
        from airflow.providers.common.compat.openlineage.facet import (
            Dataset,
            LifecycleStateChange,
            LifecycleStateChangeDatasetFacet,
            PreviousIdentifier,
        )
        from airflow.providers.google.cloud.openlineage.utils import BIGQUERY_NAMESPACE
        from airflow.providers.openlineage.extractors import OperatorLineage

        bq_table_id = str(
            TableReference.from_string(self.deletion_dataset_table, default_project=self.hook.project_id)
        )
        ds = Dataset(
            namespace=BIGQUERY_NAMESPACE,
            name=bq_table_id,
            facets={
                "lifecycleStateChange": LifecycleStateChangeDatasetFacet(
                    lifecycleStateChange=LifecycleStateChange.DROP.value,
                    previousIdentifier=PreviousIdentifier(
                        namespace=BIGQUERY_NAMESPACE,
                        name=bq_table_id,
                    ),
                )
            },
        )

        return OperatorLineage(inputs=[ds])


class BigQueryUpsertTableOperator(GoogleCloudBaseOperator):
    """
    Upsert to a BigQuery table.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryUpsertTableOperator`

    :param dataset_id: A dotted
        ``(<project>.|<project>:)<dataset>`` that indicates which dataset
        will be updated. (templated)
    :param table_resource: a table resource. see
        https://cloud.google.com/bigquery/docs/reference/v2/tables#resource
    :param project_id: The name of the project where we want to update the dataset.
        Don't need to provide, if projectId in dataset_reference.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param location: The location used for the operation.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "dataset_id",
        "table_resource",
        "gcp_conn_id",
        "impersonation_chain",
        "project_id",
    )
    template_fields_renderers = {"table_resource": "json"}
    ui_color = BigQueryUIColors.TABLE.value
    operator_extra_links = (BigQueryTableLink(),)

    def __init__(
        self,
        *,
        dataset_id: str,
        table_resource: dict,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        location: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.dataset_id = dataset_id
        self.table_resource = table_resource
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.location = location
        self.impersonation_chain = impersonation_chain
        self._table: dict | None = None

    def execute(self, context: Context) -> None:
        self.log.info("Upserting Dataset: %s with table_resource: %s", self.dataset_id, self.table_resource)
        hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )
        # Save table as attribute for further use by OpenLineage
        self._table = hook.run_table_upsert(
            dataset_id=self.dataset_id,
            table_resource=self.table_resource,
            project_id=self.project_id,
        )
        if self._table:
            BigQueryTableLink.persist(
                context=context,
                dataset_id=self._table["tableReference"]["datasetId"],
                project_id=self._table["tableReference"]["projectId"],
                table_id=self._table["tableReference"]["tableId"],
            )

    def get_openlineage_facets_on_complete(self, _):
        """Implement _on_complete as we will use table resource returned by upsert method."""
        from airflow.providers.common.compat.openlineage.facet import Dataset
        from airflow.providers.google.cloud.openlineage.utils import (
            BIGQUERY_NAMESPACE,
            get_facets_from_bq_table,
        )
        from airflow.providers.openlineage.extractors import OperatorLineage

        table = Table.from_api_repr(self._table)
        output_dataset = Dataset(
            namespace=BIGQUERY_NAMESPACE,
            name=f"{table.project}.{table.dataset_id}.{table.table_id}",
            facets=get_facets_from_bq_table(table),
        )

        return OperatorLineage(outputs=[output_dataset])


class BigQueryUpdateTableSchemaOperator(GoogleCloudBaseOperator):
    """
    Update BigQuery Table Schema.

    Updates fields on a table schema based on contents of the supplied schema_fields_updates
    parameter. The supplied schema does not need to be complete, if the field
    already exists in the schema you only need to supply keys & values for the
    items you want to patch, just ensure the "name" key is set.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryUpdateTableSchemaOperator`

    :param schema_fields_updates: a partial schema resource. see
        https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableSchema

        .. code-block:: python

            schema_fields_updates = [
                {"name": "emp_name", "description": "Some New Description"},
                {
                    "name": "salary",
                    "policyTags": {"names": ["some_new_policy_tag"]},
                },
                {
                    "name": "departments",
                    "fields": [
                        {"name": "name", "description": "Some New Description"},
                        {"name": "type", "description": "Some New Description"},
                    ],
                },
            ]

    :param include_policy_tags: (Optional) If set to True policy tags will be included in
        the update request which requires special permissions even if unchanged (default False)
        see https://cloud.google.com/bigquery/docs/column-level-security#roles
    :param dataset_id: A dotted
        ``(<project>.|<project>:)<dataset>`` that indicates which dataset
        will be updated. (templated)
    :param table_id: The table ID of the requested table. (templated)
    :param project_id: The name of the project where we want to update the dataset.
        Don't need to provide, if projectId in dataset_reference.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param location: The location used for the operation.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "schema_fields_updates",
        "dataset_id",
        "table_id",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )
    template_fields_renderers = {"schema_fields_updates": "json"}
    ui_color = BigQueryUIColors.TABLE.value
    operator_extra_links = (BigQueryTableLink(),)

    def __init__(
        self,
        *,
        schema_fields_updates: list[dict[str, Any]],
        dataset_id: str,
        table_id: str,
        include_policy_tags: bool = False,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        location: str | None = None,
        **kwargs,
    ) -> None:
        self.schema_fields_updates = schema_fields_updates
        self.include_policy_tags = include_policy_tags
        self.table_id = table_id
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.location = location
        self._table: dict | None = None
        super().__init__(**kwargs)

    def execute(self, context: Context):
        bq_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain, location=self.location
        )

        # Save table as attribute for further use by OpenLineage
        self._table = bq_hook.update_table_schema(
            schema_fields_updates=self.schema_fields_updates,
            include_policy_tags=self.include_policy_tags,
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            project_id=self.project_id,
        )
        if self._table:
            BigQueryTableLink.persist(
                context=context,
                dataset_id=self._table["tableReference"]["datasetId"],
                project_id=self._table["tableReference"]["projectId"],
                table_id=self._table["tableReference"]["tableId"],
            )
        return self._table

    def get_openlineage_facets_on_complete(self, _):
        """Implement _on_complete as we will use table resource returned by update method."""
        from airflow.providers.common.compat.openlineage.facet import Dataset
        from airflow.providers.google.cloud.openlineage.utils import (
            BIGQUERY_NAMESPACE,
            get_facets_from_bq_table,
        )
        from airflow.providers.openlineage.extractors import OperatorLineage

        table = Table.from_api_repr(self._table)
        output_dataset = Dataset(
            namespace=BIGQUERY_NAMESPACE,
            name=f"{table.project}.{table.dataset_id}.{table.table_id}",
            facets=get_facets_from_bq_table(table),
        )

        return OperatorLineage(outputs=[output_dataset])


class BigQueryInsertJobOperator(GoogleCloudBaseOperator, _BigQueryInsertJobOperatorOpenLineageMixin):
    """
    Execute a BigQuery job.

    Waits for the job to complete and returns job id.
    This operator work in the following way:

    - it calculates a unique hash of the job using job's configuration or uuid if ``force_rerun`` is True
    - creates ``job_id`` in form of
        ``[provided_job_id | airflow_{dag_id}_{task_id}_{exec_date}]_{uniqueness_suffix}``
    - submits a BigQuery job using the ``job_id``
    - if job with given id already exists then it tries to reattach to the job if its not done and its
        state is in ``reattach_states``. If the job is done the operator will raise ``AirflowException``.

    Using ``force_rerun`` will submit a new job every time without attaching to already existing ones.

    For job definition see here:

        https://cloud.google.com/bigquery/docs/reference/v2/jobs

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryInsertJobOperator`


    :param configuration: The configuration parameter maps directly to BigQuery's
        configuration field in the job object. For more details see
        https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfiguration
    :param job_id: The ID of the job. It will be suffixed with hash of job configuration
        unless ``force_rerun`` is True.
        The ID must contain only letters (a-z, A-Z), numbers (0-9), underscores (_), or
        dashes (-). The maximum length is 1,024 characters. If not provided then uuid will
        be generated.
    :param force_rerun: If True then operator will use hash of uuid as job id suffix
    :param reattach_states: Set of BigQuery job's states in case of which we should reattach
        to the job. Should be other than final states.
    :param project_id: Google Cloud Project where the job is running
    :param location: location the job is running
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param cancel_on_kill: Flag which indicates whether cancel the hook's job or not, when on_kill is called
    :param result_retry: How to retry the `result` call that retrieves rows
    :param result_timeout: The number of seconds to wait for `result` method before using `result_retry`
    :param deferrable: Run operator in the deferrable mode
    :param poll_interval: (Deferrable mode only) polling period in seconds to check for the status of job.
        Defaults to 4 seconds.
    """

    template_fields: Sequence[str] = (
        "configuration",
        "job_id",
        "gcp_conn_id",
        "impersonation_chain",
        "project_id",
    )
    template_ext: Sequence[str] = (
        ".json",
        ".sql",
    )
    template_fields_renderers = {"configuration": "json", "configuration.query.query": "sql"}
    ui_color = BigQueryUIColors.QUERY.value
    operator_extra_links = (BigQueryTableLink(), BigQueryJobDetailLink())

    def __init__(
        self,
        configuration: dict[str, Any],
        project_id: str = PROVIDE_PROJECT_ID,
        location: str | None = None,
        job_id: str | None = None,
        force_rerun: bool = True,
        reattach_states: set[str] | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        cancel_on_kill: bool = True,
        result_retry: Retry = DEFAULT_RETRY,
        result_timeout: float | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poll_interval: float = 4.0,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.configuration = configuration
        self.location = location
        self.job_id = job_id
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.force_rerun = force_rerun
        self.reattach_states: set[str] = reattach_states or set()
        self.impersonation_chain = impersonation_chain
        self.cancel_on_kill = cancel_on_kill
        self.result_retry = result_retry
        self.result_timeout = result_timeout
        self.hook: BigQueryHook | None = None
        self.deferrable = deferrable
        self.poll_interval = poll_interval

    @cached_property
    def sql(self) -> str | None:
        try:
            return self.configuration["query"]["query"]
        except KeyError:
            return None

    def prepare_template(self) -> None:
        # If .json is passed then we have to read the file
        if isinstance(self.configuration, str) and self.configuration.endswith(".json"):
            with open(self.configuration) as file:
                self.configuration = json.loads(file.read())

    def _add_job_labels(self) -> None:
        dag_label = self.dag_id.lower()
        task_label = self.task_id.lower().replace(".", "-")

        if LABEL_REGEX.match(dag_label) and LABEL_REGEX.match(task_label):
            automatic_labels = {"airflow-dag": dag_label, "airflow-task": task_label}
            if isinstance(self.configuration.get("labels"), dict):
                self.configuration["labels"].update(automatic_labels)
            elif "labels" not in self.configuration:
                self.configuration["labels"] = automatic_labels

    def _submit_job(
        self,
        hook: BigQueryHook,
        job_id: str,
    ) -> BigQueryJob:
        # Annotate the job with dag and task id labels
        self._add_job_labels()

        # Submit a new job without waiting for it to complete.
        return hook.insert_job(
            configuration=self.configuration,
            project_id=self.project_id,
            location=self.location,
            job_id=job_id,
            timeout=self.result_timeout,
            retry=self.result_retry,
            nowait=True,
        )

    def _handle_job_error(self, job: BigQueryJob | UnknownJob) -> None:
        self.log.info("Job %s is completed. Checking the job status", self.job_id)
        # Log any transient errors encountered during the job execution
        for error in job.errors or []:
            self.log.error("BigQuery Job Error: %s", error)
        if job.error_result:
            raise AirflowException(f"BigQuery job {job.job_id} failed: {job.error_result}")
        # Check the final state.
        if job.state != "DONE":
            raise AirflowException(f"Job failed with state: {job.state}")

    def execute(self, context: Any):
        hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.hook = hook
        if self.project_id is None:
            self.project_id = hook.project_id

        self.job_id = hook.generate_job_id(
            job_id=self.job_id,
            dag_id=self.dag_id,
            task_id=self.task_id,
            logical_date=None,
            configuration=self.configuration,
            run_after=hook.get_run_after_or_logical_date(context),
            force_rerun=self.force_rerun,
        )

        try:
            self.log.info("Executing: %s'", self.configuration)
            # Create a job
            if self.job_id is None:
                raise ValueError("job_id cannot be None")
            job: BigQueryJob | UnknownJob = self._submit_job(hook, self.job_id)
        except Conflict:
            # If the job already exists retrieve it
            job = hook.get_job(
                project_id=self.project_id,
                location=self.location,
                job_id=self.job_id,
            )

            if job.state not in self.reattach_states:
                # Same job configuration, so we need force_rerun
                raise AirflowException(
                    f"Job with id: {self.job_id} already exists and is in {job.state} state. If you "
                    f"want to force rerun it consider setting `force_rerun=True`."
                    f"Or, if you want to reattach in this scenario add {job.state} to `reattach_states`"
                )

            # Job already reached state DONE
            if job.state == "DONE":
                raise AirflowException("Job is already in state DONE. Can not reattach to this job.")

            # We are reattaching to a job
            self.log.info("Reattaching to existing Job in state %s", job.state)
            self._handle_job_error(job)

        job_types = {
            LoadJob._JOB_TYPE: ["sourceTable", "destinationTable"],
            CopyJob._JOB_TYPE: ["sourceTable", "destinationTable"],
            ExtractJob._JOB_TYPE: ["sourceTable"],
            QueryJob._JOB_TYPE: ["destinationTable"],
        }

        if self.project_id:
            for job_type, tables_prop in job_types.items():
                job_configuration = job.to_api_repr()["configuration"]
                if job_type in job_configuration:
                    for table_prop in tables_prop:
                        if table_prop in job_configuration[job_type]:
                            table = job_configuration[job_type][table_prop]
                            persist_kwargs = {
                                "context": context,
                                "project_id": self.project_id,
                                "table_id": table,
                            }
                            if not isinstance(table, str):
                                persist_kwargs["table_id"] = table["tableId"]
                                persist_kwargs["dataset_id"] = table["datasetId"]
                                persist_kwargs["project_id"] = table["projectId"]
                            BigQueryTableLink.persist(**persist_kwargs)

        self.job_id = job.job_id

        if self.project_id:
            job_id_path = convert_job_id(
                job_id=self.job_id,
                project_id=self.project_id,
                location=self.location,
            )
            context["ti"].xcom_push(key="job_id_path", value=job_id_path)

        persist_kwargs = {
            "context": context,
            "project_id": self.project_id,
            "location": self.location,
            "job_id": self.job_id,
        }
        BigQueryJobDetailLink.persist(**persist_kwargs)

        # Wait for the job to complete
        if not self.deferrable:
            job.result(timeout=self.result_timeout, retry=self.result_retry)
            self._handle_job_error(job)

            return self.job_id
        if job.running():
            self.defer(
                timeout=self.execution_timeout,
                trigger=BigQueryInsertJobTrigger(
                    conn_id=self.gcp_conn_id,
                    job_id=self.job_id,
                    project_id=self.project_id,
                    location=self.location or hook.location,
                    poll_interval=self.poll_interval,
                    impersonation_chain=self.impersonation_chain,
                    cancel_on_kill=self.cancel_on_kill,
                ),
                method_name="execute_complete",
            )
        self.log.info("Current state of job %s is %s", job.job_id, job.state)
        self._handle_job_error(job)
        return self.job_id

    def execute_complete(self, context: Context, event: dict[str, Any]) -> str | None:
        """
        Act as a callback for when the trigger fires.

        This returns immediately. It relies on trigger to throw an exception,
        otherwise it assumes execution was successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info(
            "%s completed with response %s ",
            self.task_id,
            event["message"],
        )
        # Save job_id as an attribute to be later used by listeners
        self.job_id = event.get("job_id")
        return self.job_id

    def on_kill(self) -> None:
        if self.job_id and self.cancel_on_kill:
            self.hook.cancel_job(  # type: ignore[union-attr]
                job_id=self.job_id, project_id=self.project_id, location=self.location
            )
        else:
            self.log.info("Skipping to cancel job: %s:%s.%s", self.project_id, self.location, self.job_id)
