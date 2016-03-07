import logging
import boto3

from airflow import settings
from airflow.utils import AirflowException
from airflow.models import BaseOperator
from airflow.utils import apply_defaults
from airflow.models import Connection as DB
from airflow.contrib.hooks import ecs_hook
import sys

class FusionOperator(BaseOperator):


    """
    Execute a task on AWS EC2 Container Service using Fusion Operator

    """
    
    
    ui_color = '#f0ede4'
    client = None
    arn = None
    template_fields = ('params',)
    
    @apply_defaults
    def __init__(
            self,
            repository,
            function_name,
            params,
            aws_conn_id ="ecs_default",
            *args, **kwargs):
        super(FusionOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.hook = self.get_hook()
        self.repository = repository
        self.function_name = function_name
        self.params = params
        self.cluster = 'default' 
        if ('cluster' in kwargs):
            self.cluster = kwargs['cluster']
        self.task_definition = 'worker'
        if ('task_defintion' in kwargs):
            self.task_definition = kwargs['task_definition']
    
        #if ()
    def execute(self, context):
        import json, os, base64
        json_data = json.dumps(self.params)
        param_encoded64 = base64.b64encode(json_data.encode('utf-8'))
        params = [self.repository, self.function_name, param_encoded64]
        overrides={'containerOverrides': [{'name': 'worker', 'command' : params}]} 

        self.client = self.hook.get_conn()
        response = self.client.run_task(taskDefinition=self.task_definition, cluster=self.cluster, overrides= overrides)
        
        failures = response["failures"]
        if (len(failures) > 0):
            raise AirflowException(response)
        
        logging.info("Task started: "+str(response))
        self.arn = response["tasks"][0]['taskArn']
        
        waiter = self.client.get_waiter('tasks_stopped')
        waiter.config.max_attempts = sys.maxint
        waiter.config.delay = 10
        waiter.wait(cluster=self.cluster, tasks=[self.arn])
        response = self.client.describe_tasks(cluster= self.cluster,tasks=[self.arn])
            
        failures = response["failures"]
        if (len(failures) > 0):
            raise AirflowException(response)
        
        logging.info("Task stopped: "+str(response))
        container = response["tasks"][0]['containers'][0]
        exitCode = container['exitCode']
        if exitCode:
            if 'reason' in container:
                reason = container['reason']
                raise AirflowException("ECS task failed  with Exit Code:"+str(exitCode)+ "and Reason ["+str(reason)+"]")
            else:
                raise AirflowException("ECS task failed  with Exit Code:"+str(exitCode))
 
    def get_hook(self):
        return ecs_hook.ECSHook(aws_conn_id=self.aws_conn_id)
    
    def on_kill(self):
        response = self.client.stop_task(
                                         cluster= self.cluster,
                                         task=self.arn,
                                         reason='Task killed by the user'
                                         )
        logging.info(response)
