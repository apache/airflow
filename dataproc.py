from airflow.providers.google.cloud.operators.dataproc import DataprocWorkflowTemplateInstantiateOperator
from google.cloud import dataproc_v1 dataproc
from google.auth import credentials

class CancelableDataprocWorkflowTemplateInstantiateOperator(DataprocWorkflowTemplateInstantiateOperator)
def __init__(self,*args,**kwargs):
  super().__intit__(*args,**kwargs)
  self.client = dataproc.WorkflowTemplateServiceClient(credentitals=credentials)
  
def on_kill(self):
  super().on_kill()
  try:
    self.log.info("attempt to cancel Cloud Dataproc Workflow template instace.")
    response = self._client.instantiate_inline_workflow_template(name=self.instance_name, project_id=self.project_id, location= self.region, version=self.version)
    operation_name= response.operation.name
    self.log.info(f"Cancelling Cloud Dataproc workflow template instace operation :{operation_name}")
    cancel_response= self.response.cancel_operation(operation_name)
    self.log.info(f"Cancel operation response: {cancel_response}:")
  except Exception as e:
    self.log.warning(f"failed to cancel Cloud Dataproc workflow template instance:{e}")
