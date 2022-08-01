import asyncio

from airflow import AirflowException
from airflow.providers.google.cloud.hooks.dataproc import DataprocAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent
from google.cloud.dataproc_v1 import JobStatus, Job
from typing import Any, Dict, Optional, Sequence, Tuple, Union


class DataprocBaseTrigger(BaseTrigger):
    """
    Trigger that periodically polls information from Dataproc API to verify job status.
    Implementation leverages asynchronous transport.
    """

    def __init__(
        self,
        job_id: str,
        project_id: str,
        region: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        delegate_to: Optional[str] = None,
        polling_interval_seconds: int = 30,
    ):
        super().__init__()
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.job_id = job_id
        self.project_id = project_id
        self.region = region
        self.polling_interval_seconds = polling_interval_seconds
        self.delegate_to = delegate_to
        self.hook = DataprocAsyncHook(
            delegate_to=self.delegate_to,
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    def serialize(self):
        return (
            "airflow.providers.google.cloud.triggers.dataproc.DataprocBaseTrigger",
            {
                "job_id": self.job_id,
                "project_id": self.project_id,
                "region": self.region,
                "gcp_conn_id": self.gcp_conn_id,
                "delegate_to": self.delegate_to,
                "impersonation_chain": self.impersonation_chain,
                "polling_interval_seconds": self.polling_interval_seconds,
            },
        )

    async def run(self):
        while True:
            job = await self.hook.get_job(project_id=self.project_id, region=self.region, job_id=self.job_id)
            state = job.status.state
            self.log.info("Dataproc job: %s is in state: %s", self.job_id, state)
            if state in (JobStatus.State.ERROR, JobStatus.State.DONE, JobStatus.State.CANCELLED):
                if state in (JobStatus.State.DONE, JobStatus.State.CANCELLED):
                    break
                elif state == JobStatus.State.ERROR:
                    raise AirflowException(f"Dataproc job execution failed {self.job_id}")
            await asyncio.sleep(self.polling_interval_seconds)
        yield TriggerEvent({"job_id": self.job_id, "job_state": state})
