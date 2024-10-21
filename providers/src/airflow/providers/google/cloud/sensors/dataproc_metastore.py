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
from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.dataproc_metastore import DataprocMetastoreHook
from airflow.providers.google.cloud.hooks.gcs import parse_json_from_gcs
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from google.api_core.operation import Operation

    from airflow.utils.context import Context


class MetastoreHivePartitionSensor(BaseSensorOperator):
    """
    Waits for partitions to show up in Hive.

    This sensor uses Google Cloud SDK and passes requests via gRPC.

    :param service_id: Required. Dataproc Metastore service id.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param table: Required. Name of the partitioned table
    :param partitions: List of table partitions to wait for.
        A name of a partition should look like "ds=1", or "a=1/b=2" in case of nested partitions.
        Note that you cannot use logical or comparison operators as in HivePartitionSensor.
        If not specified then the sensor will wait for at least one partition regardless its name.
    :param gcp_conn_id: Airflow Google Cloud connection ID.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    """

    template_fields: Sequence[str] = (
        "service_id",
        "region",
        "table",
        "partitions",
        "impersonation_chain",
    )

    def __init__(
        self,
        service_id: str,
        region: str,
        table: str,
        partitions: list[str] | None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.service_id = service_id
        self.region = region
        self.table = table
        self.partitions = partitions or []
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def poke(self, context: Context) -> bool:
        hook = DataprocMetastoreHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        operation: Operation = hook.list_hive_partitions(
            region=self.region, service_id=self.service_id, table=self.table, partition_names=self.partitions
        )
        metadata = hook.wait_for_operation(timeout=self.timeout, operation=operation)
        result_manifest_uri: str = metadata.result_manifest_uri
        self.log.info("Received result manifest URI: %s", result_manifest_uri)

        self.log.info("Extracting result manifest")
        manifest: dict = parse_json_from_gcs(
            gcp_conn_id=self.gcp_conn_id,
            file_uri=result_manifest_uri,
            impersonation_chain=self.impersonation_chain,
        )
        if not (manifest and isinstance(manifest, dict)):
            message = (
                f"Failed to extract result manifest. "
                f"Expected not empty dict, but this was received: {manifest}"
            )
            raise AirflowException(message)

        if manifest.get("status", {}).get("code") != 0:
            message = f"Request failed: {manifest.get('message')}"
            raise AirflowException(message)

        # Extract actual query results
        result_base_uri = result_manifest_uri.rsplit("/", 1)[0]
        results = (f"{result_base_uri}//{filename}" for filename in manifest.get("filenames", []))
        found_partitions = sum(
            len(
                parse_json_from_gcs(
                    gcp_conn_id=self.gcp_conn_id,
                    file_uri=uri,
                    impersonation_chain=self.impersonation_chain,
                ).get("rows", [])
            )
            for uri in results
        )

        # Return True if we got all requested partitions.
        # If no partitions were given in the request, then we expect to find at least one.
        return found_partitions >= max(1, len(set(self.partitions)))
