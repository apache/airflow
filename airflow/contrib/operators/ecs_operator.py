import logging
import boto3

from airflow import settings
from airflow.utils import AirflowException
from airflow.models import BaseOperator
from airflow.utils import apply_defaults
from airflow.models import Connection as DB
from airflow.contrib.hooks import ecs_hook
import subprocess

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
    template_fields = ('overrides',)
    
    @apply_defaults
    def __init__(
            self,
            taskDefinition,
            cluster,
            overrides,
            aws_conn_id ="ecs_default",
            python_callable = None,
            local_execution=False,
            *args, **kwargs):
        super(ECSOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.hook = self.get_hook()
        self.taskDefinition = taskDefinition
        self.cluster = cluster
        self.overrides = overrides
        self.python_callable =  python_callable
        self.local_execution = local_execution
        logging.info("overrides: {0}".format(self.overrides))
          

    def execute_local(self):
        from subprocess import Popen, PIPE
        process = Popen(['docker', 'zxventures/airflow'], stdout=PIPE, stderr=PIPE)
        stdout, stderr = process.communicate()


    
    def execute(self, context):
        if (self.python_callable):
            self.overrides = self.python_callable(**context)
        logging.info("Running ecs task - Task definition: " + self.taskDefinition+" - on cluster "+self.cluster);
        logging.info("Command: "+str(self.overrides))

        if (self.local_execution):
            self.execute_local()
        else:
            self.client = self.hook.get_conn()
            response = self.client.run_task(taskDefinition=self.taskDefinition, cluster=self.cluster, overrides= self.overrides)
            
            failures = response["failures"]
            if (len(failures) > 0):
                raise AirflowException(response)
            
            logging.info("Task started: "+str(response))
            self.arn = response["tasks"][0]['taskArn']
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
