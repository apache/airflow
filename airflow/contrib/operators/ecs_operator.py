
import logging

from airflow import settings
from airflow.utils import AirflowException
from airflow.models import BaseOperator
from airflow.utils import apply_defaults
from airflow.models import Connection as DB
from airflow.contrib.hooks import ecs_hook

import boto3


class ECSOperator(BaseOperator):


    """
    Execute a task on AWS EC2 Container Service

    :param task_id: task identifier
    :type: task_id:str
    :param taskDefinition: the task definition name on EC2 Container Service
    :type taskDefinition: str
    :param cluster: the cluster name on EC2 Container Service
    :type cluster: str
    :param: overrides: the same parameter that boto3 will receive: http://boto3.readthedocs.org/en/latest/reference/services/ecs.html#ECS.Client.run_task
    :type: overrides: dict
    """
    ui_color = '#f0ede4'

    client = None
    arn = None
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
        
        self.client = self.hook.get_conn()
        logging.info("Running ecs task - Task definition: " + self.taskDefinition+" - on cluster "+self.cluster);
        logging.info("Command: "+str(self.overrides))
        response = self.client.run_task(taskDefinition=self.taskDefinition, cluster=self.cluster, overrides= self.overrides)
        
        failures = response["failures"]
        if (len(failures) > 0):
            raise AirflowException(response)
        
        logging.info("Task started: "+str(response))
        self.arn = response["tasks"][0]['taskArn']
        #r = client.describe_tasks(cluster='default', tasks=[arn])
        #logging.info("Describe task: "+str(r))
        import sys
        waiter = self.client.get_waiter('tasks_stopped')
        waiter.config.max_attempts = sys.maxint
        waiter.config.delay = 10
        waiter.wait(cluster=self.cluster, tasks=[self.arn])
        response = self.client.describe_tasks(cluster= self.cluster,tasks=[self.arn])
       
        #ECS.Client.describe_services() 
        
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
