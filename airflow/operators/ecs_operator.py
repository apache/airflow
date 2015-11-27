
import logging

from airflow import settings
from airflow.utils import AirflowException
from airflow.models import BaseOperator
from airflow.utils import apply_defaults
from airflow.models import Connection as DB
from airflow.hooks import ECSHook

import boto3


class ECSOperator(BaseOperator):

    ui_color = '#f0ede4'

    @apply_defaults
    def __init__(
            self,
            taskDefinition,
            cluster,
            overrides,
            aws_conn_id ="ecs_default",
            *args, **kwargs):
        super(ECSOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.hook = self.get_hook()
        self.taskDefinition = taskDefinition
        self.cluster = cluster
        self.overrides = overrides

    def execute(self, context):
        client = self.hook.get_conn()
        logging.info("Running ecs task - Task definition: " + self.taskDefinition+" - on cluster "+self.cluster);
        logging.info("Command: "+str(self.overrides))
        response = client.run_task(taskDefinition=self.taskDefinition, cluster=self.cluster, overrides= self.overrides)
        
        failures = response["failures"]
        if (len(failures) > 0):
            raise AirflowException(response)
        
        logging.info("Task started: "+str(response))
        arn = response["tasks"][0]['taskArn']
        waiter = client.get_waiter('tasks_stopped')
        waiter.wait(cluster=self.cluster, tasks=[arn])
        
        response = client.describe_tasks(cluster= self.cluster,tasks=[arn])
        
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
        return ECSHook(aws_conn_id=self.aws_conn_id)
    
    #def on_kill(self):
    #    logging.info('Sending SIGTERM signal to bash subprocess')
    #    self.sp.terminate()
