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

from collections.abc import Sequence
from typing import TYPE_CHECKING

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.links.bigquery import BigQueryTableLink
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID
from airflow.providers.google.version_compat import BaseOperator

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


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
    :param project_id: Google Cloud Project where the job is running
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
        project_id: str = PROVIDE_PROJECT_ID,
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
        self._job_conf: dict = {}
        self.project_id = project_id

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
                default_project_id=self.project_id,
                var_name="source_project_dataset_table",
            )
            source_project_dataset_tables_fixup.append(
                {"projectId": source_project, "datasetId": source_dataset, "tableId": source_table}
            )

        destination_project, destination_dataset, destination_table = self.hook.split_tablename(
            table_input=self.destination_project_dataset_table,
            default_project_id=self.project_id,
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
            configuration["copy"]["destinationEncryptionConfiguration"] = self.encryption_configuration

        return configuration

    def execute(self, context: Context) -> None:
        self.log.info(
            "Executing copy of %s into: %s",
            self.source_project_dataset_tables,
            self.destination_project_dataset_table,
        )
        self.hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )

        if not self.project_id:
            self.project_id = self.hook.project_id

        configuration = self._prepare_job_configuration()
        self._job_conf = self.hook.insert_job(
            configuration=configuration, project_id=self.project_id
        ).to_api_repr()

        dest_table_info = self._job_conf["configuration"]["copy"]["destinationTable"]
        BigQueryTableLink.persist(
            context=context,
            dataset_id=dest_table_info["datasetId"],
            project_id=dest_table_info["projectId"],
            table_id=dest_table_info["tableId"],
        )

    def get_openlineage_facets_on_complete(self, task_instance):
        """Implement on_complete as we will include final BQ job id."""
        from airflow.providers.common.compat.openlineage.facet import (
            Dataset,
            ExternalQueryRunFacet,
        )
        from airflow.providers.google.cloud.openlineage.utils import (
            BIGQUERY_NAMESPACE,
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

        if not self._job_conf:
            self.log.debug("OpenLineage could not find BQ job configuration.")
            return OperatorLineage()

        bq_job_id = self._job_conf["jobReference"]["jobId"]
        source_tables_info = self._job_conf["configuration"]["copy"]["sourceTables"]
        dest_table_info = self._job_conf["configuration"]["copy"]["destinationTable"]

        run_facets = {
            "externalQuery": ExternalQueryRunFacet(externalQueryId=bq_job_id, source="bigquery"),
        }

        input_datasets = []
        for in_table_info in source_tables_info:
            table_id = ".".join(
                (in_table_info["projectId"], in_table_info["datasetId"], in_table_info["tableId"])
            )
            table_object = self.hook.get_client().get_table(table_id)
            input_datasets.append(
                Dataset(
                    namespace=BIGQUERY_NAMESPACE, name=table_id, facets=get_facets_from_bq_table(table_object)
                )
            )

        out_table_id = ".".join(
            (dest_table_info["projectId"], dest_table_info["datasetId"], dest_table_info["tableId"])
        )
        out_table_object = self.hook.get_client().get_table(out_table_id)
        output_dataset_facets = {
            **get_facets_from_bq_table(out_table_object),
            **get_identity_column_lineage_facet(
                dest_field_names=[field.name for field in out_table_object.schema],
                input_datasets=input_datasets,
            ),
        }
        output_dataset = Dataset(
            namespace=BIGQUERY_NAMESPACE,
            name=out_table_id,
            facets=output_dataset_facets,
        )

        return OperatorLineage(inputs=input_datasets, outputs=[output_dataset], run_facets=run_facets)
