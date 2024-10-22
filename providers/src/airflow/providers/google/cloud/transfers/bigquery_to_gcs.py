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
"""This module contains Google BigQuery to Google Cloud Storage operator."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

from google.api_core.exceptions import Conflict
from google.cloud.bigquery import DEFAULT_RETRY, UnknownJob

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook, BigQueryJob
from airflow.providers.google.cloud.links.bigquery import BigQueryTableLink
from airflow.providers.google.cloud.triggers.bigquery import BigQueryInsertJobTrigger
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID
from airflow.utils.helpers import merge_dicts

if TYPE_CHECKING:
    from google.api_core.retry import Retry

    from airflow.utils.context import Context


class BigQueryToGCSOperator(BaseOperator):
    """
    Transfers a BigQuery table to a Google Cloud Storage bucket.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryToGCSOperator`
    .. seealso::
        For more details about these parameters:
        https://cloud.google.com/bigquery/docs/reference/v2/jobs

    :param source_project_dataset_table: The dotted
        ``(<project>.|<project>:)<dataset>.<table>`` BigQuery table to use as the
        source data. If ``<project>`` is not included, project will be the project
        defined in the connection json. (templated)
    :param destination_cloud_storage_uris: The destination Google Cloud
        Storage URI (e.g. gs://some-bucket/some-file.txt). (templated) Follows
        convention defined here:
        https://cloud.google.com/bigquery/exporting-data-from-bigquery#exportingmultiple
    :param project_id: Google Cloud Project where the job is running
    :param compression: Type of compression to use.
    :param export_format: File format to export.
    :param field_delimiter: The delimiter to use when extracting to a CSV.
    :param print_header: Whether to print a header for a CSV file extract.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param labels: a dictionary containing labels for the job/query,
        passed to BigQuery
    :param location: The location used for the operation.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param result_retry: How to retry the `result` call that retrieves rows
    :param result_timeout: The number of seconds to wait for `result` method before using `result_retry`
    :param job_id: The ID of the job. It will be suffixed with hash of job configuration
        unless ``force_rerun`` is True.
        The ID must contain only letters (a-z, A-Z), numbers (0-9), underscores (_), or
        dashes (-). The maximum length is 1,024 characters. If not provided then uuid will
        be generated.
    :param force_rerun: If True then operator will use hash of uuid as job id suffix
    :param reattach_states: Set of BigQuery job's states in case of which we should reattach
        to the job. Should be other than final states.
    :param deferrable: Run operator in the deferrable mode
    """

    template_fields: Sequence[str] = (
        "source_project_dataset_table",
        "destination_cloud_storage_uris",
        "export_format",
        "labels",
        "impersonation_chain",
        "job_id",
    )
    template_ext: Sequence[str] = ()
    ui_color = "#e4e6f0"
    operator_extra_links = (BigQueryTableLink(),)

    def __init__(
        self,
        *,
        source_project_dataset_table: str,
        destination_cloud_storage_uris: list[str],
        project_id: str = PROVIDE_PROJECT_ID,
        compression: str = "NONE",
        export_format: str = "CSV",
        field_delimiter: str = ",",
        print_header: bool = True,
        gcp_conn_id: str = "google_cloud_default",
        labels: dict | None = None,
        location: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        result_retry: Retry = DEFAULT_RETRY,
        result_timeout: float | None = None,
        job_id: str | None = None,
        force_rerun: bool = False,
        reattach_states: set[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.source_project_dataset_table = source_project_dataset_table
        self.destination_cloud_storage_uris = destination_cloud_storage_uris
        self.compression = compression
        self.export_format = export_format
        self.field_delimiter = field_delimiter
        self.print_header = print_header
        self.gcp_conn_id = gcp_conn_id
        self.labels = labels
        self.location = location
        self.impersonation_chain = impersonation_chain
        self.result_retry = result_retry
        self.result_timeout = result_timeout
        self.job_id = job_id
        self.force_rerun = force_rerun
        self.reattach_states: set[str] = reattach_states or set()
        self.hook: BigQueryHook | None = None
        self.deferrable = deferrable

    @staticmethod
    def _handle_job_error(job: BigQueryJob | UnknownJob) -> None:
        if job.error_result:
            raise AirflowException(f"BigQuery job {job.job_id} failed: {job.error_result}")

    def _prepare_configuration(self):
        source_project, source_dataset, source_table = self.hook.split_tablename(
            table_input=self.source_project_dataset_table,
            default_project_id=self.hook.project_id,
            var_name="source_project_dataset_table",
        )

        configuration: dict[str, Any] = {
            "extract": {
                "sourceTable": {
                    "projectId": source_project,
                    "datasetId": source_dataset,
                    "tableId": source_table,
                },
                "compression": self.compression,
                "destinationUris": self.destination_cloud_storage_uris,
                "destinationFormat": self.export_format,
            }
        }

        if self.labels:
            configuration["labels"] = self.labels

        if self.export_format == "CSV":
            # Only set fieldDelimiter and printHeader fields if using CSV.
            # Google does not like it if you set these fields for other export
            # formats.
            configuration["extract"]["fieldDelimiter"] = self.field_delimiter
            configuration["extract"]["printHeader"] = self.print_header
        return configuration

    def _submit_job(
        self,
        hook: BigQueryHook,
        job_id: str,
        configuration: dict,
    ) -> BigQueryJob:
        # Submit a new job without waiting for it to complete.

        return hook.insert_job(
            configuration=configuration,
            project_id=self.project_id or hook.project_id,
            location=self.location,
            job_id=job_id,
            timeout=self.result_timeout,
            retry=self.result_retry,
            nowait=self.deferrable,
        )

    def execute(self, context: Context):
        self.log.info(
            "Executing extract of %s into: %s",
            self.source_project_dataset_table,
            self.destination_cloud_storage_uris,
        )
        hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )
        self.hook = hook

        configuration = self._prepare_configuration()
        self.job_id = hook.generate_job_id(
            job_id=self.job_id,
            dag_id=self.dag_id,
            task_id=self.task_id,
            logical_date=context["logical_date"],
            configuration=configuration,
            force_rerun=self.force_rerun,
        )

        try:
            self.log.info("Executing: %s", configuration)
            job: BigQueryJob | UnknownJob = self._submit_job(
                hook=hook, job_id=self.job_id, configuration=configuration
            )
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
            else:
                # Job already reached state DONE
                if job.state == "DONE":
                    raise AirflowException("Job is already in state DONE. Can not reattach to this job.")

                # We are reattaching to a job
                self.log.info("Reattaching to existing Job in state %s", job.state)
                self._handle_job_error(job)

        self.job_id = job.job_id
        conf = job.to_api_repr()["configuration"]["extract"]["sourceTable"]
        dataset_id, project_id, table_id = conf["datasetId"], conf["projectId"], conf["tableId"]
        BigQueryTableLink.persist(
            context=context,
            task_instance=self,
            dataset_id=dataset_id,
            project_id=project_id,
            table_id=table_id,
        )

        if self.deferrable:
            self.defer(
                timeout=self.execution_timeout,
                trigger=BigQueryInsertJobTrigger(
                    conn_id=self.gcp_conn_id,
                    job_id=self.job_id,
                    project_id=self.project_id or self.hook.project_id,
                    location=self.location or self.hook.location,
                    impersonation_chain=self.impersonation_chain,
                ),
                method_name="execute_complete",
            )
        else:
            job.result(timeout=self.result_timeout, retry=self.result_retry)

    def execute_complete(self, context: Context, event: dict[str, Any]):
        """
        Return immediately and relies on trigger to throw a success event. Callback for the trigger.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
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

    def get_openlineage_facets_on_complete(self, task_instance):
        """Implement on_complete as we will include final BQ job id."""
        from pathlib import Path

        from airflow.providers.common.compat.openlineage.facet import (
            Dataset,
            ExternalQueryRunFacet,
            Identifier,
            SymlinksDatasetFacet,
        )
        from airflow.providers.google.cloud.hooks.gcs import _parse_gcs_url
        from airflow.providers.google.cloud.openlineage.utils import (
            get_facets_from_bq_table,
            get_identity_column_lineage_facet,
        )
        from airflow.providers.openlineage.extractors import OperatorLineage

        if not self.hook:
            self.hook = BigQueryHook(
                gcp_conn_id=self.gcp_conn_id,
                location=self.location,
                impersonation_chain=self.impersonation_chain,
            )

        project_id = self.project_id or self.hook.project_id
        table_object = self.hook.get_client(project_id).get_table(self.source_project_dataset_table)

        input_dataset = Dataset(
            namespace="bigquery",
            name=str(table_object.reference),
            facets=get_facets_from_bq_table(table_object),
        )

        output_dataset_facets = {
            "schema": input_dataset.facets["schema"],
            "columnLineage": get_identity_column_lineage_facet(
                field_names=[field.name for field in table_object.schema], input_datasets=[input_dataset]
            ),
        }
        output_datasets = []
        for uri in sorted(self.destination_cloud_storage_uris):
            bucket, blob = _parse_gcs_url(uri)
            additional_facets = {}

            if "*" in blob:
                # If wildcard ("*") is used in gcs path, we want the name of dataset to be directory name,
                # but we create a symlink to the full object path with wildcard.
                additional_facets = {
                    "symlink": SymlinksDatasetFacet(
                        identifiers=[Identifier(namespace=f"gs://{bucket}", name=blob, type="file")]
                    ),
                }
                blob = Path(blob).parent.as_posix()
                if blob == ".":
                    # blob path does not have leading slash, but we need root dataset name to be "/"
                    blob = "/"

            dataset = Dataset(
                namespace=f"gs://{bucket}",
                name=blob,
                facets=merge_dicts(output_dataset_facets, additional_facets),
            )
            output_datasets.append(dataset)

        run_facets = {}
        if self.job_id:
            run_facets = {
                "externalQuery": ExternalQueryRunFacet(externalQueryId=self.job_id, source="bigquery"),
            }

        return OperatorLineage(inputs=[input_dataset], outputs=output_datasets, run_facets=run_facets)
