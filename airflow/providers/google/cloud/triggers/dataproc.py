
import asyncio

from airflow import AirflowException
from airflow.providers.google.cloud.hooks.dataproc import DataprocHook
from airflow.triggers.base import BaseTrigger, TriggerEvent
from google.cloud.dataproc_v1 import JobStatus, Job
from typing import Any, Dict, Optional, Sequence, Tuple, Union


class DataprocBaseTrigger(BaseTrigger):
    """
    TODO: description
    """

    def __init__(
          self,
          job_id: str,
          project_id: str,
          region: str,
          gcp_conn_id: str = "google_cloud_default",
          impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
          delegate_to: Optional[str] = None,
          pooling_period_seconds: int = 30
    ):
        super().__init__()
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.job_id = job_id
        self.project_id = project_id
        self.region = region
        self.pooling_period_seconds = pooling_period_seconds
        self.delegate_to = delegate_to
        self.hook = DataprocHook(
            delegate_to=self.delegate_to,
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            async_client=True
        )

    def serialize(self):
        return (
          "airflow.providers.google.cloud.operators.dataproc.DataprocBaseTrigger",
                {
                  "job_id": self.job_id,
                  "project_id": self.project_id,
                  "region": self.region,
                  "gcp_conn_id": self.gcp_conn_id,
                  "delegate_to": self.delegate_to,
                  "impersonation_chain": self.impersonation_chain,
                  "pooling_period_seconds": self.pooling_period_seconds
                })

    def get_job(self) -> Job:
        return self.hook.get_job(
            job_id=self.job_id,
            project_id=self.project_id,
            region=self.region
        )

    async def run(self):
        while True:
            job = self.get_job()
            if job.done.state:
              if job.status.state in (JobStatus.State.DONE, JobStatus.State.CANCELLED):
                break
              elif job.status.state == JobStatus.State.ERROR:
                raise AirflowException(f"Dataproc job execution failed {self.job_id}")
              else:
                raise AirflowException(f"Dataproc job execution finished in uknknown stat {job.staus.state} {self.job_id}")
            await asyncio.sleep(self.pooling_period_seconds)
        yield TriggerEvent({"job": job})
