from airflow.models.baseoperator import BaseOperator
from google.cloud import batch_v1
from airflow.providers.google.cloud.hooks.cloud_batch import CloudBatchHook, CloudBatchAsyncHook
from airflow.providers.google.cloud.triggers.cloud_batch import CloudBatchJobFinishedTrigger
from typing import Sequence
from typing import Optional
from typing import Union
from google.protobuf.json_format import MessageToJson
import json
from airflow.utils.context import Context
from airflow.exceptions import AirflowException
from google.api_core import operation  # type: ignore


class CloudBatchSubmitJobOperator(BaseOperator):
    def __init__(
        self,
        project_id: str,
        region: str,
        job_name: str,
        job: batch_v1.Job,
        polling_period_seconds: float = 10,
        timeout: Union[float, None] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        deferrable: bool = False,
        **kwargs
    ) -> None:
        """
        Submit a job and wait for its completion.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param sleep_time: Optional: Control the rate of the poll for the result of deferrable run.
            By default, the trigger will poll every 10 seconds.
        :param timeout: The timeout for this request.
        :param gcp_conn_id:
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
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.job_name = job_name
        self.job = job
        self.polling_period_seconds = polling_period_seconds
        self.timeout = timeout
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.deferrable = deferrable
        self.polling_period_seconds = polling_period_seconds

    def execute(self, context):
        hook: CloudBatchHook = CloudBatchHook(
            self.gcp_conn_id, self.impersonation_chain)
        job = hook.submit_build_job(
            self.job_name, self.job, self.region, self.project_id)

        if not self.deferrable:
            completed_job = hook.wait_for_job(
                job_name=job.name, polling_period_seconds=self.polling_period_seconds, timeout=self.timeout)
            return self._convert_job_to_json_serializable(completed_job)

        else:
            self.defer(
                trigger=CloudBatchJobFinishedTrigger(
                    job_name=job.name,
                    project_id=self.project_id,
                    gcp_conn_id=self.gcp_conn_id,
                    impersonation_chain=self.impersonation_chain,
                    location=self.region,
                    polling_period_seconds=self.polling_period_seconds

                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: Context, event: dict):
        job_status = event["status"]
        if job_status == "success":
            hook: CloudBatchHook = CloudBatchHook(
                self.gcp_conn_id, self.impersonation_chain)
            job = hook.get_job(job_name=event["job_name"])
            return self._convert_job_to_json_serializable(job)
        else:
            raise AirflowException(
                f"Unexpected error in the operation: {event['message']}")

    def _convert_job_to_json_serializable(self, job: batch_v1.Job):
        json_representation = MessageToJson(job._pb)
        convertable_object = json.loads(json_representation, object_hook=batch_v1.Job)
        return convertable_object


class CloudBatchDeleteJobOperator(BaseOperator):
    def __init__(
        self,
        project_id: str,
        region: str,
        job_name: str,
        timeout: Union[float, None] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.job_name = job_name
        self.timeout = timeout
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook: CloudBatchHook = CloudBatchHook(
            self.gcp_conn_id, self.impersonation_chain)

        operation = hook.delete_job(
            self.job_name, self.region, self.project_id)

        self._wait_for_operation(operation)

    def _wait_for_operation(self, operation: operation.Operation):
        try:
            return operation.result(timeout=self.timeout)
        except Exception:
            error = operation.exception(timeout=self.timeout)
            raise AirflowException(error)
