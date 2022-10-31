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
import warnings
from typing import TYPE_CHECKING, Any, Iterable, Sequence, SupportsAbs

import attr
from google.api_core.exceptions import Conflict
from google.api_core.retry import Retry
from google.cloud.bigquery import DEFAULT_RETRY, CopyJob, ExtractJob, LoadJob, QueryJob

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, BaseOperatorLink
from airflow.models.xcom import XCom
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLColumnCheckOperator,
    SQLIntervalCheckOperator,
    SQLTableCheckOperator,
    SQLValueCheckOperator,
    _parse_boolean,
)
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook, BigQueryJob
from airflow.providers.google.cloud.hooks.gcs import GCSHook, _parse_gcs_url
from airflow.providers.google.cloud.links.bigquery import BigQueryDatasetLink, BigQueryTableLink
from airflow.providers.google.cloud.triggers.bigquery import (
    BigQueryCheckTrigger,
    BigQueryGetDataTrigger,
    BigQueryInsertJobTrigger,
    BigQueryIntervalCheckTrigger,
    BigQueryValueCheckTrigger,
)

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstanceKey
    from airflow.utils.context import Context


BIGQUERY_JOB_DETAILS_LINK_FMT = "https://console.cloud.google.com/bigquery?j={job_id}"


class BigQueryUIColors(enum.Enum):
    """Hex colors for BigQuery operators"""

    CHECK = "#C0D7FF"
    QUERY = "#A1BBFF"
    TABLE = "#81A0FF"
    DATASET = "#5F86FF"


class BigQueryConsoleLink(BaseOperatorLink):
    """Helper class for constructing BigQuery link."""

    name = "BigQuery Console"

    def get_link(
        self,
        operator: BaseOperator,
        *,
        ti_key: TaskInstanceKey,
    ):
        job_id = XCom.get_value(key="job_id", ti_key=ti_key)
        return BIGQUERY_JOB_DETAILS_LINK_FMT.format(job_id=job_id) if job_id else ""


@attr.s(auto_attribs=True)
class BigQueryConsoleIndexableLink(BaseOperatorLink):
    """Helper class for constructing BigQuery link."""

    index: int = attr.ib()

    @property
    def name(self) -> str:
        return f"BigQuery Console #{self.index + 1}"

    def get_link(
        self,
        operator: BaseOperator,
        *,
        ti_key: TaskInstanceKey,
    ):
        job_ids = XCom.get_value(key="job_id", ti_key=ti_key)
        if not job_ids:
            return None
        if len(job_ids) < self.index:
            return None
        job_id = job_ids[self.index]
        return BIGQUERY_JOB_DETAILS_LINK_FMT.format(job_id=job_id)


class _BigQueryDbHookMixin:
    def get_db_hook(self: BigQueryCheckOperator) -> BigQueryHook:  # type:ignore[misc]
        """Get BigQuery DB Hook"""
        return BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            use_legacy_sql=self.use_legacy_sql,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
            labels=self.labels,
        )


class BigQueryCheckOperator(_BigQueryDbHookMixin, SQLCheckOperator):
    """
    Performs checks against BigQuery. The ``BigQueryCheckOperator`` expects
    a sql query that will return a single row. Each value on that
    first row is evaluated using python ``bool`` casting. If any of the
    values return ``False`` the check is failed and errors out.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryCheckOperator`

    Note that Python bool casting evals the following as ``False``:

    * ``False``
    * ``0``
    * Empty string (``""``)
    * Empty list (``[]``)
    * Empty dictionary or set (``{}``)

    Given a query like ``SELECT COUNT(*) FROM foo``, it will fail only if
    the count ``== 0``. You can craft much more complex query that could,
    for instance, check that the table has the same number of rows as
    the source table upstream, or that the count of today's partition is
    greater than yesterday's partition, or that a set of metrics are less
    than 3 standard deviation for the 7 day average.

    This operator can be used as a data quality check in your pipeline, and
    depending on where you put it in your DAG, you have the choice to
    stop the critical path, preventing from
    publishing dubious data, or on the side and receive email alerts
    without stopping the progress of the DAG.

    :param sql: the sql to be executed
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
    :param deferrable: Run operator in the deferrable mode
    """

    template_fields: Sequence[str] = (
        "sql",
        "gcp_conn_id",
        "impersonation_chain",
        "labels",
    )
    template_ext: Sequence[str] = (".sql",)
    ui_color = BigQueryUIColors.CHECK.value

    def __init__(
        self,
        *,
        sql: str,
        gcp_conn_id: str = "google_cloud_default",
        use_legacy_sql: bool = True,
        location: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        labels: dict | None = None,
        deferrable: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(sql=sql, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.sql = sql
        self.use_legacy_sql = use_legacy_sql
        self.location = location
        self.impersonation_chain = impersonation_chain
        self.labels = labels
        self.deferrable = deferrable

    def _submit_job(
        self,
        hook: BigQueryHook,
        job_id: str,
    ) -> BigQueryJob:
        """Submit a new job and get the job id for polling the status using Trigger."""
        configuration = {"query": {"query": self.sql}}

        return hook.insert_job(
            configuration=configuration,
            project_id=hook.project_id,
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
            )
            job = self._submit_job(hook, job_id="")
            context["ti"].xcom_push(key="job_id", value=job.job_id)
            self.defer(
                timeout=self.execution_timeout,
                trigger=BigQueryCheckTrigger(
                    conn_id=self.gcp_conn_id,
                    job_id=job.job_id,
                    project_id=hook.project_id,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])

        records = event["records"]
        if not records:
            raise AirflowException("The query returned empty results")
        elif not all(bool(r) for r in records):
            self._raise_exception(f"Test failed.\nQuery:\n{self.sql}\nResults:\n{records!s}")
        self.log.info("Record: %s", event["records"])
        self.log.info("Success.")


class BigQueryValueCheckOperator(_BigQueryDbHookMixin, SQLValueCheckOperator):
    """
    Performs a simple value check using sql code.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryValueCheckOperator`

    :param sql: the sql to be executed
    :param use_legacy_sql: Whether to use legacy SQL (true)
        or standard SQL (false).
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

    def __init__(
        self,
        *,
        sql: str,
        pass_value: Any,
        tolerance: Any = None,
        gcp_conn_id: str = "google_cloud_default",
        use_legacy_sql: bool = True,
        location: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        labels: dict | None = None,
        deferrable: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(sql=sql, pass_value=pass_value, tolerance=tolerance, **kwargs)
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.use_legacy_sql = use_legacy_sql
        self.impersonation_chain = impersonation_chain
        self.labels = labels
        self.deferrable = deferrable

    def _submit_job(
        self,
        hook: BigQueryHook,
        job_id: str,
    ) -> BigQueryJob:
        """Submit a new job and get the job id for polling the status using Triggerer."""
        configuration = {
            "query": {
                "query": self.sql,
                "useLegacySql": False,
            }
        }
        if self.use_legacy_sql:
            configuration["query"]["useLegacySql"] = self.use_legacy_sql

        return hook.insert_job(
            configuration=configuration,
            project_id=hook.project_id,
            location=self.location,
            job_id=job_id,
            nowait=True,
        )

    def execute(self, context: Context) -> None:  # type: ignore[override]
        if not self.deferrable:
            super().execute(context=context)
        else:
            hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id)

            job = self._submit_job(hook, job_id="")
            context["ti"].xcom_push(key="job_id", value=job.job_id)
            self.defer(
                timeout=self.execution_timeout,
                trigger=BigQueryValueCheckTrigger(
                    conn_id=self.gcp_conn_id,
                    job_id=job.job_id,
                    project_id=hook.project_id,
                    sql=self.sql,
                    pass_value=self.pass_value,
                    tolerance=self.tol,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info(
            "%s completed with response %s ",
            self.task_id,
            event["message"],
        )


class BigQueryIntervalCheckOperator(_BigQueryDbHookMixin, SQLIntervalCheckOperator):
    """
    Checks that the values of metrics given as SQL expressions are within
    a certain tolerance of the ones from days_back before.

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
        impersonation_chain: str | Sequence[str] | None = None,
        labels: dict | None = None,
        deferrable: bool = False,
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
        self.impersonation_chain = impersonation_chain
        self.labels = labels
        self.deferrable = deferrable

    def _submit_job(
        self,
        hook: BigQueryHook,
        sql: str,
        job_id: str,
    ) -> BigQueryJob:
        """Submit a new job and get the job id for polling the status using Triggerer."""
        configuration = {"query": {"query": sql}}
        return hook.insert_job(
            configuration=configuration,
            project_id=hook.project_id,
            location=self.location,
            job_id=job_id,
            nowait=True,
        )

    def execute(self, context: Context):
        if not self.deferrable:
            super().execute(context)
        else:
            hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id)
            self.log.info("Using ratio formula: %s", self.ratio_formula)

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
                    project_id=hook.project_id,
                    table=self.table,
                    metrics_thresholds=self.metrics_thresholds,
                    date_filter_column=self.date_filter_column,
                    days_back=self.days_back,
                    ratio_formula=self.ratio_formula,
                    ignore_zero=self.ignore_zero,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info(
            "%s completed with response %s ",
            self.task_id,
            event["message"],
        )


class BigQueryColumnCheckOperator(_BigQueryDbHookMixin, SQLColumnCheckOperator):
    """
    BigQueryColumnCheckOperator subclasses the SQLColumnCheckOperator
    in order to provide a job id for OpenLineage to parse. See base class
    docstring for usage.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryColumnCheckOperator`

    :param table: the table name
    :param column_mapping: a dictionary relating columns to their checks
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
    """

    def __init__(
        self,
        *,
        table: str,
        column_mapping: dict,
        partition_clause: str | None = None,
        database: str | None = None,
        accept_none: bool = True,
        gcp_conn_id: str = "google_cloud_default",
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
        self.use_legacy_sql = use_legacy_sql
        self.location = location
        self.impersonation_chain = impersonation_chain
        self.labels = labels

    def _submit_job(
        self,
        hook: BigQueryHook,
        job_id: str,
    ) -> BigQueryJob:
        """Submit a new job and get the job id for polling the status using Trigger."""
        configuration = {"query": {"query": self.sql}}

        return hook.insert_job(
            configuration=configuration,
            project_id=hook.project_id,
            location=self.location,
            job_id=job_id,
            nowait=False,
        )

    def execute(self, context=None):
        """Perform checks on the given columns."""
        hook = self.get_db_hook()
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

        failed_tests(
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


class BigQueryTableCheckOperator(_BigQueryDbHookMixin, SQLTableCheckOperator):
    """
    BigQueryTableCheckOperator subclasses the SQLTableCheckOperator
    in order to provide a job id for OpenLineage to parse. See base class
    for usage.

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
    """

    def __init__(
        self,
        *,
        table: str,
        checks: dict,
        partition_clause: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        use_legacy_sql: bool = True,
        location: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        labels: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(table=table, checks=checks, partition_clause=partition_clause, **kwargs)
        self.table = table
        self.checks = checks
        self.partition_clause = partition_clause
        self.gcp_conn_id = gcp_conn_id
        self.use_legacy_sql = use_legacy_sql
        self.location = location
        self.impersonation_chain = impersonation_chain
        self.labels = labels

    def _submit_job(
        self,
        hook: BigQueryHook,
        job_id: str,
    ) -> BigQueryJob:
        """Submit a new job and get the job id for polling the status using Trigger."""
        configuration = {"query": {"query": self.sql}}

        return hook.insert_job(
            configuration=configuration,
            project_id=hook.project_id,
            location=self.location,
            job_id=job_id,
            nowait=False,
        )

    def execute(self, context=None):
        """Execute the given checks on the table."""
        hook = self.get_db_hook()
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


class BigQueryGetDataOperator(BaseOperator):
    """
    Fetches the data from a BigQuery table (alternatively fetch data for selected columns)
    and returns data in a python list. The number of elements in the returned list will
    be equal to the number of rows fetched. Each element in the list will again be a list
    where element would represent the columns values for that row.

    **Example Result**: ``[['Tony', '10'], ['Mike', '20'], ['Steve', '15']]``

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryGetDataOperator`

    .. note::
        If you pass fields to ``selected_fields`` which are in different order than the
        order of columns already in
        BQ table, the data will still be in the order of BQ table.
        For example if the BQ table has 3 columns as
        ``[A,B,C]`` and you pass 'B,A' in the ``selected_fields``
        the data would still be of the form ``'A,B'``.

    **Example**: ::

        get_data = BigQueryGetDataOperator(
            task_id='get_data_from_bq',
            dataset_id='test_dataset',
            table_id='Transaction_partitions',
            project_id='internal-gcp-project',
            max_results=100,
            selected_fields='DATE',
            gcp_conn_id='airflow-conn-id'
        )

    :param dataset_id: The dataset ID of the requested table. (templated)
    :param table_id: The table ID of the requested table. (templated)
    :param project_id: (Optional) The name of the project where the data
        will be returned from. (templated)
    :param max_results: The maximum number of records (rows) to be fetched
        from the table. (templated)
    :param selected_fields: List of fields to return (comma-separated). If
        unspecified, all fields are returned.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
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
    """

    template_fields: Sequence[str] = (
        "dataset_id",
        "table_id",
        "project_id",
        "max_results",
        "selected_fields",
        "impersonation_chain",
    )
    ui_color = BigQueryUIColors.QUERY.value

    def __init__(
        self,
        *,
        dataset_id: str,
        table_id: str,
        project_id: str | None = None,
        max_results: int = 100,
        selected_fields: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        location: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.dataset_id = dataset_id
        self.table_id = table_id
        self.max_results = int(max_results)
        self.selected_fields = selected_fields
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.location = location
        self.impersonation_chain = impersonation_chain
        self.project_id = project_id
        self.deferrable = deferrable

    def _submit_job(
        self,
        hook: BigQueryHook,
        job_id: str,
    ) -> BigQueryJob:
        get_query = self.generate_query()
        configuration = {"query": {"query": get_query}}
        """Submit a new job and get the job id for polling the status using Triggerer."""
        return hook.insert_job(
            configuration=configuration,
            location=self.location,
            project_id=hook.project_id,
            job_id=job_id,
            nowait=True,
        )

    def generate_query(self) -> str:
        """
        Generate a select query if selected fields are given or with *
        for the given dataset and table id
        """
        query = "select "
        if self.selected_fields:
            query += self.selected_fields
        else:
            query += "*"
        query += f" from {self.dataset_id}.{self.table_id} limit {self.max_results}"
        return query

    def execute(self, context: Context):
        hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        self.hook = hook

        if not self.deferrable:
            self.log.info(
                "Fetching Data from %s.%s max results: %s", self.dataset_id, self.table_id, self.max_results
            )
            if not self.selected_fields:
                schema: dict[str, list] = hook.get_schema(
                    dataset_id=self.dataset_id,
                    table_id=self.table_id,
                )
                if "fields" in schema:
                    self.selected_fields = ",".join([field["name"] for field in schema["fields"]])

            rows = hook.list_rows(
                dataset_id=self.dataset_id,
                table_id=self.table_id,
                max_results=self.max_results,
                selected_fields=self.selected_fields,
                location=self.location,
                project_id=self.project_id,
            )

            self.log.info("Total extracted rows: %s", len(rows))

            table_data = [row.values() for row in rows]
            return table_data

        job = self._submit_job(hook, job_id="")
        self.job_id = job.job_id
        context["ti"].xcom_push(key="job_id", value=self.job_id)
        self.defer(
            timeout=self.execution_timeout,
            trigger=BigQueryGetDataTrigger(
                conn_id=self.gcp_conn_id,
                job_id=self.job_id,
                dataset_id=self.dataset_id,
                table_id=self.table_id,
                project_id=hook.project_id,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> Any:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])

        self.log.info("Total extracted rows: %s", len(event["records"]))
        return event["records"]


class BigQueryExecuteQueryOperator(BaseOperator):
    """
    Executes BigQuery SQL queries in a specific BigQuery database.
    This operator does not assert idempotency.

    This operator is deprecated.
    Please use :class:`airflow.providers.google.cloud.operators.bigquery.BigQueryInsertJobOperator`

    :param sql: the SQL code to be executed as a single string, or
        a list of str (sql statements), or a reference to a template file.
        Template references are recognized by str ending in '.sql'
    :param destination_dataset_table: A dotted
        ``(<project>.|<project>:)<dataset>.<table>`` that, if set, will store the results
        of the query. (templated)
    :param write_disposition: Specifies the action that occurs if the destination table
        already exists. (default: 'WRITE_EMPTY')
    :param create_disposition: Specifies whether the job is allowed to create new tables.
        (default: 'CREATE_IF_NEEDED')
    :param allow_large_results: Whether to allow large results.
    :param flatten_results: If true and query uses legacy SQL dialect, flattens
        all nested and repeated fields in the query results. ``allow_large_results``
        must be ``true`` if this is set to ``false``. For standard SQL queries, this
        flag is ignored and results are never flattened.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param udf_config: The User Defined Function configuration for the query.
        See https://cloud.google.com/bigquery/user-defined-functions for details.
    :param use_legacy_sql: Whether to use legacy SQL (true) or standard SQL (false).
    :param maximum_billing_tier: Positive integer that serves as a multiplier
        of the basic price.
        Defaults to None, in which case it uses the value set in the project.
    :param maximum_bytes_billed: Limits the bytes billed for this job.
        Queries that will have bytes billed beyond this limit will fail
        (without incurring a charge). If unspecified, this will be
        set to your project default.
    :param api_resource_configs: a dictionary that contain params
        'configuration' applied for Google BigQuery Jobs API:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs
        for example, {'query': {'useQueryCache': False}}. You could use it
        if you need to provide some params that are not supported by BigQueryOperator
        like args.
    :param schema_update_options: Allows the schema of the destination
        table to be updated as a side effect of the load job.
    :param query_params: a list of dictionary containing query parameter types and
        values, passed to BigQuery. The structure of dictionary should look like
        'queryParameters' in Google BigQuery Jobs API:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs.
        For example, [{ 'name': 'corpus', 'parameterType': { 'type': 'STRING' },
        'parameterValue': { 'value': 'romeoandjuliet' } }]. (templated)
    :param labels: a dictionary containing labels for the job/query,
        passed to BigQuery
    :param priority: Specifies a priority for the query.
        Possible values include INTERACTIVE and BATCH.
        The default value is INTERACTIVE.
    :param time_partitioning: configure optional time partitioning fields i.e.
        partition by field, type and expiration as per API specifications.
    :param cluster_fields: Request that the result of this query be stored sorted
        by one or more columns. BigQuery supports clustering for both partitioned and
        non-partitioned tables. The order of columns given determines the sort order.
    :param location: The geographic location of the job. Required except for
        US and EU. See details at
        https://cloud.google.com/bigquery/docs/locations#specifying_your_location
    :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).
        **Example**: ::

            encryption_configuration = {
                "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key"
            }
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
        "sql",
        "destination_dataset_table",
        "labels",
        "query_params",
        "impersonation_chain",
    )
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"sql": "sql"}
    ui_color = BigQueryUIColors.QUERY.value

    @property
    def operator_extra_links(self):
        """Return operator extra links"""
        if isinstance(self.sql, str):
            return (BigQueryConsoleLink(),)
        return (BigQueryConsoleIndexableLink(i) for i, _ in enumerate(self.sql))

    def __init__(
        self,
        *,
        sql: str | Iterable[str],
        destination_dataset_table: str | None = None,
        write_disposition: str = "WRITE_EMPTY",
        allow_large_results: bool = False,
        flatten_results: bool | None = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        udf_config: list | None = None,
        use_legacy_sql: bool = True,
        maximum_billing_tier: int | None = None,
        maximum_bytes_billed: float | None = None,
        create_disposition: str = "CREATE_IF_NEEDED",
        schema_update_options: list | tuple | set | None = None,
        query_params: list | None = None,
        labels: dict | None = None,
        priority: str = "INTERACTIVE",
        time_partitioning: dict | None = None,
        api_resource_configs: dict | None = None,
        cluster_fields: list[str] | None = None,
        location: str | None = None,
        encryption_configuration: dict | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        warnings.warn(
            "This operator is deprecated. Please use `BigQueryInsertJobOperator`.",
            DeprecationWarning,
            stacklevel=2,
        )

        self.sql = sql
        self.destination_dataset_table = destination_dataset_table
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition
        self.allow_large_results = allow_large_results
        self.flatten_results = flatten_results
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.udf_config = udf_config
        self.use_legacy_sql = use_legacy_sql
        self.maximum_billing_tier = maximum_billing_tier
        self.maximum_bytes_billed = maximum_bytes_billed
        self.schema_update_options = schema_update_options
        self.query_params = query_params
        self.labels = labels
        self.priority = priority
        self.time_partitioning = time_partitioning
        self.api_resource_configs = api_resource_configs
        self.cluster_fields = cluster_fields
        self.location = location
        self.encryption_configuration = encryption_configuration
        self.hook: BigQueryHook | None = None
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        if self.hook is None:
            self.log.info("Executing: %s", self.sql)
            self.hook = BigQueryHook(
                gcp_conn_id=self.gcp_conn_id,
                use_legacy_sql=self.use_legacy_sql,
                delegate_to=self.delegate_to,
                location=self.location,
                impersonation_chain=self.impersonation_chain,
            )
        if isinstance(self.sql, str):
            job_id: str | list[str] = self.hook.run_query(
                sql=self.sql,
                destination_dataset_table=self.destination_dataset_table,
                write_disposition=self.write_disposition,
                allow_large_results=self.allow_large_results,
                flatten_results=self.flatten_results,
                udf_config=self.udf_config,
                maximum_billing_tier=self.maximum_billing_tier,
                maximum_bytes_billed=self.maximum_bytes_billed,
                create_disposition=self.create_disposition,
                query_params=self.query_params,
                labels=self.labels,
                schema_update_options=self.schema_update_options,
                priority=self.priority,
                time_partitioning=self.time_partitioning,
                api_resource_configs=self.api_resource_configs,
                cluster_fields=self.cluster_fields,
                encryption_configuration=self.encryption_configuration,
            )
        elif isinstance(self.sql, Iterable):
            job_id = [
                self.hook.run_query(
                    sql=s,
                    destination_dataset_table=self.destination_dataset_table,
                    write_disposition=self.write_disposition,
                    allow_large_results=self.allow_large_results,
                    flatten_results=self.flatten_results,
                    udf_config=self.udf_config,
                    maximum_billing_tier=self.maximum_billing_tier,
                    maximum_bytes_billed=self.maximum_bytes_billed,
                    create_disposition=self.create_disposition,
                    query_params=self.query_params,
                    labels=self.labels,
                    schema_update_options=self.schema_update_options,
                    priority=self.priority,
                    time_partitioning=self.time_partitioning,
                    api_resource_configs=self.api_resource_configs,
                    cluster_fields=self.cluster_fields,
                    encryption_configuration=self.encryption_configuration,
                )
                for s in self.sql
            ]
        else:
            raise AirflowException(f"argument 'sql' of type {type(str)} is neither a string nor an iterable")
        context["task_instance"].xcom_push(key="job_id", value=job_id)

    def on_kill(self) -> None:
        super().on_kill()
        if self.hook is not None:
            self.log.info("Cancelling running query")
            self.hook.cancel_query()


class BigQueryCreateEmptyTableOperator(BaseOperator):
    """
    Creates a new, empty table in the specified BigQuery dataset,
    optionally with schema.

    The schema to be used for the BigQuery table may be specified in one of
    two ways. You may either directly pass the schema fields in, or you may
    point the operator to a Google Cloud Storage object name. The object in
    Google Cloud Storage must be a JSON file with the schema fields in it.
    You can also create a table without schema.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryCreateEmptyTableOperator`

    :param project_id: The project to create the table into. (templated)
    :param dataset_id: The dataset to create the table into. (templated)
    :param table_id: The Name of the table to be created. (templated)
    :param table_resource: Table resource as described in documentation:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#Table
        If provided all other parameters are ignored.
    :param schema_fields: If set, the schema field list as defined here:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load.schema

        **Example**: ::

            schema_fields=[{"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
                           {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"}]

    :param gcs_schema_object: Full path to the JSON file containing
        schema (templated). For
        example: ``gs://test-bucket/dir1/dir2/employee_schema.json``
    :param time_partitioning: configure optional time partitioning fields i.e.
        partition by field, type and  expiration as per API specifications.

        .. seealso::
            https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#timePartitioning
    :param gcp_conn_id: [Optional] The connection ID used to connect to Google Cloud and
        interact with the Bigquery service.
    :param google_cloud_storage_conn_id: [Optional] The connection ID used to connect to Google Cloud.
        and interact with the Google Cloud Storage service.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param labels: a dictionary containing labels for the table, passed to BigQuery

        **Example (with schema JSON in GCS)**: ::

            CreateTable = BigQueryCreateEmptyTableOperator(
                task_id='BigQueryCreateEmptyTableOperator_task',
                dataset_id='ODS',
                table_id='Employees',
                project_id='internal-gcp-project',
                gcs_schema_object='gs://schema-bucket/employee_schema.json',
                gcp_conn_id='airflow-conn-id',
                google_cloud_storage_conn_id='airflow-conn-id'
            )

        **Corresponding Schema file** (``employee_schema.json``): ::

            [
              {
                "mode": "NULLABLE",
                "name": "emp_name",
                "type": "STRING"
              },
              {
                "mode": "REQUIRED",
                "name": "salary",
                "type": "INTEGER"
              }
            ]

        **Example (with schema in the DAG)**: ::

            CreateTable = BigQueryCreateEmptyTableOperator(
                task_id='BigQueryCreateEmptyTableOperator_task',
                dataset_id='ODS',
                table_id='Employees',
                project_id='internal-gcp-project',
                schema_fields=[{"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
                               {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"}],
                gcp_conn_id='airflow-conn-id-account',
                google_cloud_storage_conn_id='airflow-conn-id'
            )
    :param view: [Optional] A dictionary containing definition for the view.
        If set, it will create a view instead of a table:

        .. seealso::
            https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#ViewDefinition
    :param materialized_view: [Optional] The materialized view definition.
    :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).
        **Example**: ::

            encryption_configuration = {
                "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key"
            }
    :param location: The location used for the operation.
    :param cluster_fields: [Optional] The fields used for clustering.
            BigQuery supports clustering for both partitioned and
            non-partitioned tables.

            .. seealso::
                https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#clustering.fields
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param exists_ok: If ``True``, ignore "already exists" errors when creating the table.
    """

    template_fields: Sequence[str] = (
        "dataset_id",
        "table_id",
        "project_id",
        "gcs_schema_object",
        "labels",
        "view",
        "materialized_view",
        "impersonation_chain",
    )
    template_fields_renderers = {"table_resource": "json", "materialized_view": "json"}
    ui_color = BigQueryUIColors.TABLE.value
    operator_extra_links = (BigQueryTableLink(),)

    def __init__(
        self,
        *,
        dataset_id: str,
        table_id: str,
        table_resource: dict[str, Any] | None = None,
        project_id: str | None = None,
        schema_fields: list | None = None,
        gcs_schema_object: str | None = None,
        time_partitioning: dict | None = None,
        gcp_conn_id: str = "google_cloud_default",
        bigquery_conn_id: str | None = None,
        google_cloud_storage_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        labels: dict | None = None,
        view: dict | None = None,
        materialized_view: dict | None = None,
        encryption_configuration: dict | None = None,
        location: str | None = None,
        cluster_fields: list[str] | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        exists_ok: bool = False,
        **kwargs,
    ) -> None:
        if bigquery_conn_id:
            warnings.warn(
                "The bigquery_conn_id parameter has been deprecated. Use the gcp_conn_id parameter instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            gcp_conn_id = bigquery_conn_id

        super().__init__(**kwargs)

        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.schema_fields = schema_fields
        self.gcs_schema_object = gcs_schema_object
        self.gcp_conn_id = gcp_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.time_partitioning = {} if time_partitioning is None else time_partitioning
        self.labels = labels
        self.view = view
        self.materialized_view = materialized_view
        self.encryption_configuration = encryption_configuration
        self.location = location
        self.cluster_fields = cluster_fields
        self.table_resource = table_resource
        self.impersonation_chain = impersonation_chain
        self.exists_ok = exists_ok

    def execute(self, context: Context) -> None:
        bq_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )

        if not self.schema_fields and self.gcs_schema_object:
            gcs_bucket, gcs_object = _parse_gcs_url(self.gcs_schema_object)
            gcs_hook = GCSHook(
                gcp_conn_id=self.google_cloud_storage_conn_id,
                delegate_to=self.delegate_to,
                impersonation_chain=self.impersonation_chain,
            )
            schema_fields_string = gcs_hook.download_as_byte_array(gcs_bucket, gcs_object).decode("utf-8")
            schema_fields = json.loads(schema_fields_string)
        else:
            schema_fields = self.schema_fields

        try:
            self.log.info("Creating table")
            table = bq_hook.create_empty_table(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                table_id=self.table_id,
                schema_fields=schema_fields,
                time_partitioning=self.time_partitioning,
                cluster_fields=self.cluster_fields,
                labels=self.labels,
                view=self.view,
                materialized_view=self.materialized_view,
                encryption_configuration=self.encryption_configuration,
                table_resource=self.table_resource,
                exists_ok=self.exists_ok,
            )
            BigQueryTableLink.persist(
                context=context,
                task_instance=self,
                dataset_id=table.to_api_repr()["tableReference"]["datasetId"],
                project_id=table.to_api_repr()["tableReference"]["projectId"],
                table_id=table.to_api_repr()["tableReference"]["tableId"],
            )
            self.log.info(
                "Table %s.%s.%s created successfully", table.project, table.dataset_id, table.table_id
            )
        except Conflict:
            self.log.info("Table %s.%s already exists.", self.dataset_id, self.table_id)


class BigQueryCreateExternalTableOperator(BaseOperator):
    """
    Creates a new external table in the dataset with the data from Google Cloud
    Storage.

    The schema to be used for the BigQuery table may be specified in one of
    two ways. You may either directly pass the schema fields in, or you may
    point the operator to a Google Cloud Storage object name. The object in
    Google Cloud Storage must be a JSON file with the schema fields in it.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryCreateExternalTableOperator`

    :param bucket: The bucket to point the external table to. (templated)
    :param source_objects: List of Google Cloud Storage URIs to point
        table to. If source_format is 'DATASTORE_BACKUP', the list must only contain a single URI.
    :param destination_project_dataset_table: The dotted ``(<project>.)<dataset>.<table>``
        BigQuery table to load data into (templated). If ``<project>`` is not included,
        project will be the project defined in the connection json.
    :param schema_fields: If set, the schema field list as defined here:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load.schema

        **Example**: ::

            schema_fields=[{"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
                           {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"}]

        Should not be set when source_format is 'DATASTORE_BACKUP'.
    :param table_resource: Table resource as described in documentation:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#Table
        If provided all other parameters are ignored. External schema from object will be resolved.
    :param schema_object: If set, a GCS object path pointing to a .json file that
        contains the schema for the table. (templated)
    :param source_format: File format of the data.
    :param autodetect: Try to detect schema and format options automatically.
        The schema_fields and schema_object options will be honored when specified explicitly.
        https://cloud.google.com/bigquery/docs/schema-detect#schema_auto-detection_for_external_data_sources
    :param compression: [Optional] The compression type of the data source.
        Possible values include GZIP and NONE.
        The default value is NONE.
        This setting is ignored for Google Cloud Bigtable,
        Google Cloud Datastore backups and Avro formats.
    :param skip_leading_rows: Number of rows to skip when loading from a CSV.
    :param field_delimiter: The delimiter to use for the CSV.
    :param max_bad_records: The maximum number of bad records that BigQuery can
        ignore when running the job.
    :param quote_character: The value that is used to quote data sections in a CSV file.
    :param allow_quoted_newlines: Whether to allow quoted newlines (true) or not (false).
    :param allow_jagged_rows: Accept rows that are missing trailing optional columns.
        The missing values are treated as nulls. If false, records with missing trailing
        columns are treated as bad records, and if there are too many bad records, an
        invalid error is returned in the job result. Only applicable to CSV, ignored
        for other formats.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud and
        interact with the Bigquery service.
    :param google_cloud_storage_conn_id: (Optional) The connection ID used to connect to Google Cloud
        and interact with the Google Cloud Storage service.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param src_fmt_configs: configure optional fields specific to the source format
    :param labels: a dictionary containing labels for the table, passed to BigQuery
    :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).
        **Example**: ::

            encryption_configuration = {
                "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key"
            }
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
        "bucket",
        "source_objects",
        "schema_object",
        "destination_project_dataset_table",
        "labels",
        "table_resource",
        "impersonation_chain",
    )
    template_fields_renderers = {"table_resource": "json"}
    ui_color = BigQueryUIColors.TABLE.value
    operator_extra_links = (BigQueryTableLink(),)

    def __init__(
        self,
        *,
        bucket: str | None = None,
        source_objects: list[str] | None = None,
        destination_project_dataset_table: str | None = None,
        table_resource: dict[str, Any] | None = None,
        schema_fields: list | None = None,
        schema_object: str | None = None,
        source_format: str | None = None,
        autodetect: bool = False,
        compression: str | None = None,
        skip_leading_rows: int | None = None,
        field_delimiter: str | None = None,
        max_bad_records: int = 0,
        quote_character: str | None = None,
        allow_quoted_newlines: bool = False,
        allow_jagged_rows: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        bigquery_conn_id: str | None = None,
        google_cloud_storage_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        src_fmt_configs: dict | None = None,
        labels: dict | None = None,
        encryption_configuration: dict | None = None,
        location: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        if bigquery_conn_id:
            warnings.warn(
                "The bigquery_conn_id parameter has been deprecated. Use the gcp_conn_id parameter instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            gcp_conn_id = bigquery_conn_id

        super().__init__(**kwargs)

        # BQ config
        kwargs_passed = any(
            [
                destination_project_dataset_table,
                schema_fields,
                source_format,
                compression,
                skip_leading_rows,
                field_delimiter,
                max_bad_records,
                autodetect,
                quote_character,
                allow_quoted_newlines,
                allow_jagged_rows,
                src_fmt_configs,
                labels,
                encryption_configuration,
            ]
        )

        if not table_resource:
            warnings.warn(
                "Passing table parameters via keywords arguments will be deprecated. "
                "Please provide table definition using `table_resource` parameter.",
                DeprecationWarning,
                stacklevel=2,
            )
            if not bucket:
                raise ValueError("`bucket` is required when not using `table_resource`.")
            if not source_objects:
                raise ValueError("`source_objects` is required when not using `table_resource`.")
            if not source_format:
                source_format = "CSV"
            if not compression:
                compression = "NONE"
            if not skip_leading_rows:
                skip_leading_rows = 0
            if not field_delimiter:
                field_delimiter = ","
            if not destination_project_dataset_table:
                raise ValueError(
                    "`destination_project_dataset_table` is required when not using `table_resource`."
                )
            self.bucket = bucket
            self.source_objects = source_objects
            self.schema_object = schema_object
            self.destination_project_dataset_table = destination_project_dataset_table
            self.schema_fields = schema_fields
            self.source_format = source_format
            self.compression = compression
            self.skip_leading_rows = skip_leading_rows
            self.field_delimiter = field_delimiter
            self.table_resource = None
        else:
            self.table_resource = table_resource
            self.bucket = ""
            self.source_objects = []
            self.schema_object = None
            self.destination_project_dataset_table = ""

        if table_resource and kwargs_passed:
            raise ValueError("You provided both `table_resource` and exclusive keywords arguments.")

        self.max_bad_records = max_bad_records
        self.quote_character = quote_character
        self.allow_quoted_newlines = allow_quoted_newlines
        self.allow_jagged_rows = allow_jagged_rows
        self.gcp_conn_id = gcp_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.autodetect = autodetect

        self.src_fmt_configs = src_fmt_configs or {}
        self.labels = labels
        self.encryption_configuration = encryption_configuration
        self.location = location
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        bq_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )
        if self.table_resource:
            table = bq_hook.create_empty_table(
                table_resource=self.table_resource,
            )
            BigQueryTableLink.persist(
                context=context,
                task_instance=self,
                dataset_id=table.to_api_repr()["tableReference"]["datasetId"],
                project_id=table.to_api_repr()["tableReference"]["projectId"],
                table_id=table.to_api_repr()["tableReference"]["tableId"],
            )
            return

        if not self.schema_fields and self.schema_object and self.source_format != "DATASTORE_BACKUP":
            gcs_hook = GCSHook(
                gcp_conn_id=self.google_cloud_storage_conn_id,
                delegate_to=self.delegate_to,
                impersonation_chain=self.impersonation_chain,
            )
            schema_fields = json.loads(gcs_hook.download(self.bucket, self.schema_object).decode("utf-8"))
        else:
            schema_fields = self.schema_fields

        source_uris = [f"gs://{self.bucket}/{source_object}" for source_object in self.source_objects]

        project_id, dataset_id, table_id = bq_hook.split_tablename(
            table_input=self.destination_project_dataset_table,
            default_project_id=bq_hook.project_id or "",
        )

        table_resource = {
            "tableReference": {
                "projectId": project_id,
                "datasetId": dataset_id,
                "tableId": table_id,
            },
            "labels": self.labels,
            "schema": {"fields": schema_fields},
            "externalDataConfiguration": {
                "source_uris": source_uris,
                "source_format": self.source_format,
                "maxBadRecords": self.max_bad_records,
                "autodetect": self.autodetect,
                "compression": self.compression,
                "csvOptions": {
                    "fieldDelimeter": self.field_delimiter,
                    "skipLeadingRows": self.skip_leading_rows,
                    "quote": self.quote_character,
                    "allowQuotedNewlines": self.allow_quoted_newlines,
                    "allowJaggedRows": self.allow_jagged_rows,
                },
            },
            "location": self.location,
            "encryptionConfiguration": self.encryption_configuration,
        }

        table = bq_hook.create_empty_table(
            table_resource=table_resource,
        )

        BigQueryTableLink.persist(
            context=context,
            task_instance=self,
            dataset_id=table.to_api_repr()["tableReference"]["datasetId"],
            project_id=table.to_api_repr()["tableReference"]["projectId"],
            table_id=table.to_api_repr()["tableReference"]["tableId"],
        )


class BigQueryDeleteDatasetOperator(BaseOperator):
    """
    This operator deletes an existing dataset from your Project in Big query.
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
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    **Example**: ::

        delete_temp_data = BigQueryDeleteDatasetOperator(
            dataset_id='temp-dataset',
            project_id='temp-project',
            delete_contents=True, # Force the deletion of the dataset as well as its tables (if any).
            gcp_conn_id='_my_gcp_conn_',
            task_id='Deletetemp',
            dag=dag)
    """

    template_fields: Sequence[str] = (
        "dataset_id",
        "project_id",
        "impersonation_chain",
    )
    ui_color = BigQueryUIColors.DATASET.value

    def __init__(
        self,
        *,
        dataset_id: str,
        project_id: str | None = None,
        delete_contents: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.delete_contents = delete_contents
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

        super().__init__(**kwargs)

    def execute(self, context: Context) -> None:
        self.log.info("Dataset id: %s Project id: %s", self.dataset_id, self.project_id)

        bq_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        bq_hook.delete_dataset(
            project_id=self.project_id, dataset_id=self.dataset_id, delete_contents=self.delete_contents
        )


class BigQueryCreateEmptyDatasetOperator(BaseOperator):
    """
    This operator is used to create new dataset for your Project in BigQuery.
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
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param exists_ok: If ``True``, ignore "already exists" errors when creating the dataset.
        **Example**: ::

            create_new_dataset = BigQueryCreateEmptyDatasetOperator(
                dataset_id='new-dataset',
                project_id='my-project',
                dataset_reference={"friendlyName": "New Dataset"}
                gcp_conn_id='_my_gcp_conn_',
                task_id='newDatasetCreator',
                dag=dag)
    """

    template_fields: Sequence[str] = (
        "dataset_id",
        "project_id",
        "dataset_reference",
        "impersonation_chain",
    )
    template_fields_renderers = {"dataset_reference": "json"}
    ui_color = BigQueryUIColors.DATASET.value
    operator_extra_links = (BigQueryDatasetLink(),)

    def __init__(
        self,
        *,
        dataset_id: str | None = None,
        project_id: str | None = None,
        dataset_reference: dict | None = None,
        location: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        exists_ok: bool = False,
        **kwargs,
    ) -> None:

        self.dataset_id = dataset_id
        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.dataset_reference = dataset_reference if dataset_reference else {}
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.exists_ok = exists_ok

        super().__init__(**kwargs)

    def execute(self, context: Context) -> None:
        bq_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )

        try:
            dataset = bq_hook.create_empty_dataset(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                dataset_reference=self.dataset_reference,
                location=self.location,
                exists_ok=self.exists_ok,
            )
            BigQueryDatasetLink.persist(
                context=context,
                task_instance=self,
                dataset_id=dataset["datasetReference"]["datasetId"],
                project_id=dataset["datasetReference"]["projectId"],
            )
        except Conflict:
            dataset_id = self.dataset_reference.get("datasetReference", {}).get("datasetId", self.dataset_id)
            self.log.info("Dataset %s already exists.", dataset_id)


class BigQueryGetDatasetOperator(BaseOperator):
    """
    This operator is used to return the dataset specified by dataset_id.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryGetDatasetOperator`

    :param dataset_id: The id of dataset. Don't need to provide,
        if datasetId in dataset_reference.
    :param project_id: The name of the project where we want to create the dataset.
        Don't need to provide, if projectId in dataset_reference.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
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
        "impersonation_chain",
    )
    ui_color = BigQueryUIColors.DATASET.value
    operator_extra_links = (BigQueryDatasetLink(),)

    def __init__(
        self,
        *,
        dataset_id: str,
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        super().__init__(**kwargs)

    def execute(self, context: Context):
        bq_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Start getting dataset: %s:%s", self.project_id, self.dataset_id)

        dataset = bq_hook.get_dataset(dataset_id=self.dataset_id, project_id=self.project_id)
        dataset = dataset.to_api_repr()
        BigQueryDatasetLink.persist(
            context=context,
            task_instance=self,
            dataset_id=dataset["datasetReference"]["datasetId"],
            project_id=dataset["datasetReference"]["projectId"],
        )
        return dataset


class BigQueryGetDatasetTablesOperator(BaseOperator):
    """
    This operator retrieves the list of tables in the specified dataset.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryGetDatasetTablesOperator`

    :param dataset_id: the dataset ID of the requested dataset.
    :param project_id: (Optional) the project of the requested dataset. If None,
        self.project_id will be used.
    :param max_results: (Optional) the maximum number of tables to return.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
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
        "impersonation_chain",
    )
    ui_color = BigQueryUIColors.DATASET.value

    def __init__(
        self,
        *,
        dataset_id: str,
        project_id: str | None = None,
        max_results: int | None = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.max_results = max_results
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        super().__init__(**kwargs)

    def execute(self, context: Context):
        bq_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        return bq_hook.get_dataset_tables(
            dataset_id=self.dataset_id,
            project_id=self.project_id,
            max_results=self.max_results,
        )


class BigQueryPatchDatasetOperator(BaseOperator):
    """
    This operator is used to patch dataset for your Project in BigQuery.
    It only replaces fields that are provided in the submitted dataset resource.

    This operator is deprecated.
    Please use :class:`airflow.providers.google.cloud.operators.bigquery.BigQueryUpdateTableOperator`

    :param dataset_id: The id of dataset. Don't need to provide,
        if datasetId in dataset_reference.
    :param dataset_resource: Dataset resource that will be provided with request body.
        https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
    :param project_id: The name of the project where we want to create the dataset.
        Don't need to provide, if projectId in dataset_reference.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
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
        "impersonation_chain",
    )
    template_fields_renderers = {"dataset_resource": "json"}
    ui_color = BigQueryUIColors.DATASET.value

    def __init__(
        self,
        *,
        dataset_id: str,
        dataset_resource: dict,
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        warnings.warn(
            "This operator is deprecated. Please use BigQueryUpdateDatasetOperator.",
            DeprecationWarning,
            stacklevel=2,
        )
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.dataset_resource = dataset_resource
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        super().__init__(**kwargs)

    def execute(self, context: Context):
        bq_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        return bq_hook.patch_dataset(
            dataset_id=self.dataset_id,
            dataset_resource=self.dataset_resource,
            project_id=self.project_id,
        )


class BigQueryUpdateTableOperator(BaseOperator):
    """
    This operator is used to update table for your Project in BigQuery.
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
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
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
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.project_id = project_id
        self.fields = fields
        self.gcp_conn_id = gcp_conn_id
        self.table_resource = table_resource
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        super().__init__(**kwargs)

    def execute(self, context: Context):
        bq_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        table = bq_hook.update_table(
            table_resource=self.table_resource,
            fields=self.fields,
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            project_id=self.project_id,
        )

        BigQueryTableLink.persist(
            context=context,
            task_instance=self,
            dataset_id=table["tableReference"]["datasetId"],
            project_id=table["tableReference"]["projectId"],
            table_id=table["tableReference"]["tableId"],
        )

        return table


class BigQueryUpdateDatasetOperator(BaseOperator):
    """
    This operator is used to update dataset for your Project in BigQuery.
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
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
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
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.fields = fields
        self.gcp_conn_id = gcp_conn_id
        self.dataset_resource = dataset_resource
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        super().__init__(**kwargs)

    def execute(self, context: Context):
        bq_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        fields = self.fields or list(self.dataset_resource.keys())

        dataset = bq_hook.update_dataset(
            dataset_resource=self.dataset_resource,
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            fields=fields,
        )

        dataset = dataset.to_api_repr()
        BigQueryDatasetLink.persist(
            context=context,
            task_instance=self,
            dataset_id=dataset["datasetReference"]["datasetId"],
            project_id=dataset["datasetReference"]["projectId"],
        )
        return dataset


class BigQueryDeleteTableOperator(BaseOperator):
    """
    Deletes BigQuery tables

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryDeleteTableOperator`

    :param deletion_dataset_table: A dotted
        ``(<project>.|<project>:)<dataset>.<table>`` that indicates which table
        will be deleted. (templated)
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
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
        "impersonation_chain",
    )
    ui_color = BigQueryUIColors.TABLE.value

    def __init__(
        self,
        *,
        deletion_dataset_table: str,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        ignore_if_missing: bool = False,
        location: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.deletion_dataset_table = deletion_dataset_table
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.ignore_if_missing = ignore_if_missing
        self.location = location
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        self.log.info("Deleting: %s", self.deletion_dataset_table)
        hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )
        hook.delete_table(table_id=self.deletion_dataset_table, not_found_ok=self.ignore_if_missing)


class BigQueryUpsertTableOperator(BaseOperator):
    """
    Upsert BigQuery table

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
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
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
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        location: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.dataset_id = dataset_id
        self.table_resource = table_resource
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.location = location
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        self.log.info("Upserting Dataset: %s with table_resource: %s", self.dataset_id, self.table_resource)
        hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )
        table = hook.run_table_upsert(
            dataset_id=self.dataset_id,
            table_resource=self.table_resource,
            project_id=self.project_id,
        )
        BigQueryTableLink.persist(
            context=context,
            task_instance=self,
            dataset_id=table["tableReference"]["datasetId"],
            project_id=table["tableReference"]["projectId"],
            table_id=table["tableReference"]["tableId"],
        )


class BigQueryUpdateTableSchemaOperator(BaseOperator):
    """
    Update BigQuery Table Schema
    Updates fields on a table schema based on contents of the supplied schema_fields_updates
    parameter. The supplied schema does not need to be complete, if the field
    already exists in the schema you only need to supply keys & values for the
    items you want to patch, just ensure the "name" key is set.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryUpdateTableSchemaOperator`

    :param schema_fields_updates: a partial schema resource. see
        https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableSchema

    **Example**: ::

        schema_fields_updates=[
            {"name": "emp_name", "description": "Some New Description"},
            {"name": "salary", "policyTags": {'names': ['some_new_policy_tag']},},
            {"name": "departments", "fields": [
                {"name": "name", "description": "Some New Description"},
                {"name": "type", "description": "Some New Description"}
            ]},
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
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
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
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.schema_fields_updates = schema_fields_updates
        self.include_policy_tags = include_policy_tags
        self.table_id = table_id
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        super().__init__(**kwargs)

    def execute(self, context: Context):
        bq_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        table = bq_hook.update_table_schema(
            schema_fields_updates=self.schema_fields_updates,
            include_policy_tags=self.include_policy_tags,
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            project_id=self.project_id,
        )

        BigQueryTableLink.persist(
            context=context,
            task_instance=self,
            dataset_id=table["tableReference"]["datasetId"],
            project_id=table["tableReference"]["projectId"],
            table_id=table["tableReference"]["tableId"],
        )
        return table


class BigQueryInsertJobOperator(BaseOperator):
    """
    Executes a BigQuery job. Waits for the job to complete and returns job id.
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
        https://cloud.google.com/bigquery/docs/reference/v2/jobs
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
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
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
    """

    template_fields: Sequence[str] = (
        "configuration",
        "job_id",
        "impersonation_chain",
        "project_id",
    )
    template_ext: Sequence[str] = (
        ".json",
        ".sql",
    )
    template_fields_renderers = {"configuration": "json", "configuration.query.query": "sql"}
    ui_color = BigQueryUIColors.QUERY.value
    operator_extra_links = (BigQueryTableLink(),)

    def __init__(
        self,
        configuration: dict[str, Any],
        project_id: str | None = None,
        location: str | None = None,
        job_id: str | None = None,
        force_rerun: bool = True,
        reattach_states: set[str] | None = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        cancel_on_kill: bool = True,
        result_retry: Retry = DEFAULT_RETRY,
        result_timeout: float | None = None,
        deferrable: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.configuration = configuration
        self.location = location
        self.job_id = job_id
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.force_rerun = force_rerun
        self.reattach_states: set[str] = reattach_states or set()
        self.impersonation_chain = impersonation_chain
        self.cancel_on_kill = cancel_on_kill
        self.result_retry = result_retry
        self.result_timeout = result_timeout
        self.hook: BigQueryHook | None = None
        self.deferrable = deferrable

    def prepare_template(self) -> None:
        # If .json is passed then we have to read the file
        if isinstance(self.configuration, str) and self.configuration.endswith(".json"):
            with open(self.configuration) as file:
                self.configuration = json.loads(file.read())

    def _submit_job(
        self,
        hook: BigQueryHook,
        job_id: str,
    ) -> BigQueryJob:
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

    @staticmethod
    def _handle_job_error(job: BigQueryJob) -> None:
        if job.error_result:
            raise AirflowException(f"BigQuery job {job.job_id} failed: {job.error_result}")

    def execute(self, context: Any):
        hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        self.hook = hook

        job_id = hook.generate_job_id(
            job_id=self.job_id,
            dag_id=self.dag_id,
            task_id=self.task_id,
            logical_date=context["logical_date"],
            configuration=self.configuration,
            force_rerun=self.force_rerun,
        )

        try:
            self.log.info("Executing: %s'", self.configuration)
            job = self._submit_job(hook, job_id)
        except Conflict:
            # If the job already exists retrieve it
            job = hook.get_job(
                project_id=self.project_id,
                location=self.location,
                job_id=job_id,
            )
            if job.state in self.reattach_states:
                # We are reattaching to a job
                job._begin()
                self._handle_job_error(job)
            else:
                # Same job configuration so we need force_rerun
                raise AirflowException(
                    f"Job with id: {job_id} already exists and is in {job.state} state. If you "
                    f"want to force rerun it consider setting `force_rerun=True`."
                    f"Or, if you want to reattach in this scenario add {job.state} to `reattach_states`"
                )

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
                                "task_instance": self,
                                "project_id": self.project_id,
                                "table_id": table,
                            }
                            if not isinstance(table, str):
                                persist_kwargs["table_id"] = table["tableId"]
                                persist_kwargs["dataset_id"] = table["datasetId"]

                            BigQueryTableLink.persist(**persist_kwargs)

        self.job_id = job.job_id
        context["ti"].xcom_push(key="job_id", value=self.job_id)
        # Wait for the job to complete
        if not self.deferrable:
            job.result(timeout=self.result_timeout, retry=self.result_retry)
            self._handle_job_error(job)

            return self.job_id
        self.defer(
            timeout=self.execution_timeout,
            trigger=BigQueryInsertJobTrigger(
                conn_id=self.gcp_conn_id,
                job_id=self.job_id,
                project_id=self.project_id,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict[str, Any]):
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info(
            "%s completed with response %s ",
            self.task_id,
            event["message"],
        )
        return self.job_id

    def on_kill(self) -> None:
        if self.job_id and self.cancel_on_kill:
            self.hook.cancel_job(  # type: ignore[union-attr]
                job_id=self.job_id, project_id=self.project_id, location=self.location
            )
        else:
            self.log.info("Skipping to cancel job: %s:%s.%s", self.project_id, self.location, self.job_id)
