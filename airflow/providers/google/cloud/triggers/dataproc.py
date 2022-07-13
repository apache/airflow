
import asyncio
from airflow.providers.google.cloud.hooks.dataproc import DataprocHook
from airflow.triggers.base import BaseTrigger, TriggerEvent
from google.cloud.dataproc_v1 import JobStatus, Job


class DataprocBaseTrigger(BaseTrigger):
    """
    TODO: description
    """

    def __init__(self, job_id, project_id, region, gcp_conn_id, impersonation_chain):
        super().__init__()
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.job_id = job_id
        self.project_id = project_id
        self.region = region
        self.hook = DataprocHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain, async_client=True)

    def serialize(self):
        return ("airflow.providers.google.cloud.operators.dataproc.DataprocBaseTrigger",
                {"job_id": self.job_id, "project_id": self.project_id, "region": self.region})

    def get_job(self) -> Job:
        return self.hook.get_job(
            job_id=self.job_id,
            project_id=self.project_id,
            region=self.region
        )

    def job_finished(self) -> bool:
        job = self.get_job()
        state = job.status.state
        return state in (JobStatus.State.ERROR, JobStatus.State.DONE, JobStatus.State.CANCELLED)

    async def run(self):
        while self.job_finished():
            await asyncio.sleep(10)
        yield TriggerEvent({"job": self.get_job()})
