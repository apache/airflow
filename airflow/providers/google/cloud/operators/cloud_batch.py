from airflow.models.baseoperator import BaseOperator
from google.cloud import batch_v1
from airflow.providers.google.cloud.hooks.cloud_batch import CloudBatchHook
from typing import Sequence
from typing import Optional
from typing import Union

class CloudBatchSubmitJobOperator(BaseOperator):
    def __init__(
        self,
        project_id: str,
        region: str,
        job_name: str,
        job: batch_v1.Job,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        deferrable: bool = False,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.job_name = job_name
        self.job = job
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.deferrable = deferrable
        

    def execute(self, context):
        hook = CloudBatchHook(self.gcp_conn_id, self.impersonation_chain)
        return hook.submit_build_job(self.job_name, self.job, self.region, self.project_id)
