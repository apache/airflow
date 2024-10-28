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
"""This module contains Google BigQuery to BigQuery operator."""

from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.links.bigquery import BigQueryTableLink

if TYPE_CHECKING:
    from airflow.utils.context import Context


class BigQueryToBigQueryOperator(BaseOperator):
    """
    Copies data from one BigQuery table to another.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryToBigQueryOperator`
    .. seealso::
        For more details about these parameters:
        https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.copy

    :param source_project_dataset_tables: One or more
        dotted ``(project:|project.)<dataset>.<table>`` BigQuery tables to use as the
        source data. If ``<project>`` is not included, project will be the
        project defined in the connection json. Use a list if there are multiple
        source tables. (templated)
    :param destination_project_dataset_table: The destination BigQuery
        table. Format is: ``(project:|project.)<dataset>.<table>`` (templated)
    :param write_disposition: The write disposition if the table already exists.
    :param create_disposition: The create disposition if the table doesn't exist.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param labels: a dictionary containing labels for the job/query,
        passed to BigQuery
    :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).

        .. code-block:: python

            encryption_configuration = {
                "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key",
            }
    :param location: The geographic location of the job. You must specify the location to run the job if
        the location to run a job is not in the US or the EU multi-regional location or
        the location is in a single region (for example, us-central1).
        For more details check:
        https://cloud.google.com/bigquery/docs/locations#specifying_your_location
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
        "source_project_dataset_tables",
        "destination_project_dataset_table",
        "labels",
        "impersonation_chain",
    )
    template_ext: Sequence[str] = (".sql",)
    ui_color = "#e6f0e4"
    operator_extra_links = (BigQueryTableLink(),)

    def __init__(
        self,
        *,
        source_project_dataset_tables: list[str] | str,
        destination_project_dataset_table: str,
        write_disposition: str = "WRITE_EMPTY",
        create_disposition: str = "CREATE_IF_NEEDED",
        gcp_conn_id: str = "google_cloud_default",
        labels: dict | None = None,
        encryption_configuration: dict | None = None,
        location: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.source_project_dataset_tables = source_project_dataset_tables
        self.destination_project_dataset_table = destination_project_dataset_table
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition
        self.gcp_conn_id = gcp_conn_id
        self.labels = labels
        self.encryption_configuration = encryption_configuration
        self.location = location
        self.impersonation_chain = impersonation_chain
        self.hook: BigQueryHook | None = None

    def _prepare_job_configuration(self):
        self.source_project_dataset_tables = (
            [self.source_project_dataset_tables]
            if not isinstance(self.source_project_dataset_tables, list)
            else self.source_project_dataset_tables
        )

        source_project_dataset_tables_fixup = []
        for source_project_dataset_table in self.source_project_dataset_tables:
            source_project, source_dataset, source_table = self.hook.split_tablename(
                table_input=source_project_dataset_table,
                default_project_id=self.hook.project_id,
                var_name="source_project_dataset_table",
            )
            source_project_dataset_tables_fixup.append(
                {
                    "projectId": source_project,
                    "datasetId": source_dataset,
                    "tableId": source_table,
                }
            )

        destination_project, destination_dataset, destination_table = (
            self.hook.split_tablename(
                table_input=self.destination_project_dataset_table,
                default_project_id=self.hook.project_id,
            )
        )
        configuration = {
            "copy": {
                "createDisposition": self.create_disposition,
                "writeDisposition": self.write_disposition,
                "sourceTables": source_project_dataset_tables_fixup,
                "destinationTable": {
                    "projectId": destination_project,
                    "datasetId": destination_dataset,
                    "tableId": destination_table,
                },
            }
        }

        if self.labels:
            configuration["labels"] = self.labels

        if self.encryption_configuration:
            configuration["copy"]["destinationEncryptionConfiguration"] = (
                self.encryption_configuration
            )

        return configuration

    def _submit_job(
        self,
        hook: BigQueryHook,
        configuration: dict,
    ) -> str:
        job = hook.insert_job(configuration=configuration, project_id=hook.project_id)
        return job.job_id

    def execute(self, context: Context) -> None:
        self.log.info(
            "Executing copy of %s into: %s",
            self.source_project_dataset_tables,
            self.destination_project_dataset_table,
        )
        hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )
        self.hook = hook

        if not hook.project_id:
            raise ValueError("The project_id should be set")

        configuration = self._prepare_job_configuration()
        job_id = self._submit_job(hook=hook, configuration=configuration)

        job = hook.get_job(job_id=job_id, location=self.location).to_api_repr()
        conf = job["configuration"]["copy"]["destinationTable"]
        BigQueryTableLink.persist(
            context=context,
            task_instance=self,
            dataset_id=conf["datasetId"],
            project_id=conf["projectId"],
            table_id=conf["tableId"],
        )
