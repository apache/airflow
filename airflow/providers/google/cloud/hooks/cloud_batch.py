from __future__ import annotations

import warnings
from typing import Sequence

from google.cloud import batch_v1
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID, GoogleBaseHook



class CloudBatchHook(GoogleBaseHook):
    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        if kwargs.get("delegate_to") is not None:
            raise RuntimeError(
                "The `delegate_to` parameter has been deprecated before and finally removed in this version"
                " of Google Provider. You MUST convert it to `impersonate_chain`"
            )
        self._client: batch_v1.BatchServiceClient | None
        super().__init__(gcp_conn_id=gcp_conn_id, impersonation_chain=impersonation_chain)
        

    def get_conn(self) -> batch_v1.BatchServiceClient: 
        return batch_v1.BatchServiceClient()


    def submit_build_job(
            self,
            job_name: str,
            job: batch_v1.Job,
            region: str,
            project_id: str = PROVIDE_PROJECT_ID
            
    ):
        client = self.get_conn()
        create_request = batch_v1.CreateJobRequest()
        create_request.job = job
        create_request.job_id = job_name
        # The job's parent is the region in which the job will run
        create_request.parent = f"projects/{project_id}/locations/{region}"

        return client.create_job(create_request)


