:mod:`airflow.providers.amazon.aws.operators.ecs`
=================================================

.. py:module:: airflow.providers.amazon.aws.operators.ecs


Module Contents
---------------

.. py:class:: ECSProtocol

   Bases: :class:`airflow.typing_compat.Protocol`

   A structured Protocol for ``boto3.client('ecs')``. This is used for type hints on
   :py:meth:`.ECSOperator.client`.

   .. seealso::

       - https://mypy.readthedocs.io/en/latest/protocols.html
       - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html

   
   .. method:: run_task(self, **kwargs)

      https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.run_task



   
   .. method:: get_waiter(self, x: str)

      https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.get_waiter



   
   .. method:: describe_tasks(self, cluster: str, tasks)

      https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.describe_tasks



   
   .. method:: stop_task(self, cluster, task, reason: str)

      https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.stop_task



   
   .. method:: describe_task_definition(self, taskDefinition: str)

      https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.describe_task_definition



   
   .. method:: list_tasks(self, cluster: str, launchType: str, desiredStatus: str, family: str)

      https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.list_tasks




.. py:class:: ECSOperator(*, task_definition: str, cluster: str, overrides: dict, aws_conn_id: Optional[str] = None, region_name: Optional[str] = None, launch_type: str = 'EC2', group: Optional[str] = None, placement_constraints: Optional[list] = None, placement_strategy: Optional[list] = None, platform_version: str = 'LATEST', network_configuration: Optional[dict] = None, tags: Optional[dict] = None, awslogs_group: Optional[str] = None, awslogs_region: Optional[str] = None, awslogs_stream_prefix: Optional[str] = None, propagate_tags: Optional[str] = None, reattach: bool = False, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Execute a task on AWS ECS (Elastic Container Service)

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:ECSOperator`

   :param task_definition: the task definition name on Elastic Container Service
   :type task_definition: str
   :param cluster: the cluster name on Elastic Container Service
   :type cluster: str
   :param overrides: the same parameter that boto3 will receive (templated):
       https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.run_task
   :type overrides: dict
   :param aws_conn_id: connection id of AWS credentials / region name. If None,
       credential boto3 strategy will be used
       (http://boto3.readthedocs.io/en/latest/guide/configuration.html).
   :type aws_conn_id: str
   :param region_name: region name to use in AWS Hook.
       Override the region_name in connection (if provided)
   :type region_name: str
   :param launch_type: the launch type on which to run your task ('EC2' or 'FARGATE')
   :type launch_type: str
   :param group: the name of the task group associated with the task
   :type group: str
   :param placement_constraints: an array of placement constraint objects to use for
       the task
   :type placement_constraints: list
   :param placement_strategy: an array of placement strategy objects to use for
       the task
   :type placement_strategy: list
   :param platform_version: the platform version on which your task is running
   :type platform_version: str
   :param network_configuration: the network configuration for the task
   :type network_configuration: dict
   :param tags: a dictionary of tags in the form of {'tagKey': 'tagValue'}.
   :type tags: dict
   :param awslogs_group: the CloudWatch group where your ECS container logs are stored.
       Only required if you want logs to be shown in the Airflow UI after your job has
       finished.
   :type awslogs_group: str
   :param awslogs_region: the region in which your CloudWatch logs are stored.
       If None, this is the same as the `region_name` parameter. If that is also None,
       this is the default AWS region based on your connection settings.
   :type awslogs_region: str
   :param awslogs_stream_prefix: the stream prefix that is used for the CloudWatch logs.
       This is usually based on some custom name combined with the name of the container.
       Only required if you want logs to be shown in the Airflow UI after your job has
       finished.
   :type awslogs_stream_prefix: str
   :param reattach: If set to True, will check if a task from the same family is already running.
       If so, the operator will attach to it instead of starting a new task.
   :type reattach: bool

   .. attribute:: ui_color
      :annotation: = #f0ede4

      

   .. attribute:: template_fields
      :annotation: = ['overrides']

      

   
   .. method:: execute(self, context)



   
   .. method:: _start_task(self)



   
   .. method:: _try_reattach_task(self)



   
   .. method:: _wait_for_task_ended(self)



   
   .. method:: _check_success_task(self)



   
   .. method:: get_hook(self)

      Create and return an AwsHook.



   
   .. method:: get_logs_hook(self)

      Create and return an AwsLogsHook.



   
   .. method:: on_kill(self)




