:mod:`airflow.providers.microsoft.azure.operators.azure_batch`
==============================================================

.. py:module:: airflow.providers.microsoft.azure.operators.azure_batch


Module Contents
---------------

.. py:class:: AzureBatchOperator(*, batch_pool_id: str, batch_pool_vm_size: str, batch_job_id: str, batch_task_command_line: str, batch_task_id: str, vm_publisher: Optional[str] = None, vm_offer: Optional[str] = None, sku_starts_with: Optional[str] = None, vm_sku: Optional[str] = None, vm_version: Optional[str] = None, vm_node_agent_sku_id: Optional[str] = None, os_family: Optional[str] = None, os_version: Optional[str] = None, batch_pool_display_name: Optional[str] = None, batch_job_display_name: Optional[str] = None, batch_job_manager_task: Optional[batch_models.JobManagerTask] = None, batch_job_preparation_task: Optional[batch_models.JobPreparationTask] = None, batch_job_release_task: Optional[batch_models.JobReleaseTask] = None, batch_task_display_name: Optional[str] = None, batch_task_container_settings: Optional[batch_models.TaskContainerSettings] = None, batch_start_task: Optional[batch_models.StartTask] = None, batch_max_retries: int = 3, batch_task_resource_files: Optional[List[batch_models.ResourceFile]] = None, batch_task_output_files: Optional[List[batch_models.OutputFile]] = None, batch_task_user_identity: Optional[batch_models.UserIdentity] = None, target_low_priority_nodes: Optional[int] = None, target_dedicated_nodes: Optional[int] = None, enable_auto_scale: bool = False, auto_scale_formula: Optional[str] = None, azure_batch_conn_id='azure_batch_default', use_latest_verified_vm_image_and_sku: bool = False, timeout: int = 25, should_delete_job: bool = False, should_delete_pool: bool = False, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Executes a job on Azure Batch Service

   :param batch_pool_id: A string that uniquely identifies the Pool within the Account.
   :type batch_pool_id: str

   :param batch_pool_vm_size: The size of virtual machines in the Pool
   :type batch_pool_vm_size: str

   :param batch_job_id: A string that uniquely identifies the Job within the Account.
   :type batch_job_id: str

   :param batch_task_command_line: The command line of the Task
   :type batch_command_line: str

   :param batch_task_id: A string that uniquely identifies the task within the Job.
   :type batch_task_id: str

   :param batch_pool_display_name: The display name for the Pool.
       The display name need not be unique
   :type batch_pool_display_name: Optional[str]

   :param batch_job_display_name: The display name for the Job.
       The display name need not be unique
   :type batch_job_display_name: Optional[str]

   :param batch_job_manager_task: Details of a Job Manager Task to be launched when the Job is started.
   :type job_manager_task: Optional[batch_models.JobManagerTask]

   :param batch_job_preparation_task: The Job Preparation Task. If set, the Batch service will
       run the Job Preparation Task on a Node before starting any Tasks of that
       Job on that Compute Node. Required if batch_job_release_task is set.
   :type batch_job_preparation_task: Optional[batch_models.JobPreparationTask]

   :param batch_job_release_task: The Job Release Task. Use to undo changes to Compute Nodes
       made by the Job Preparation Task
   :type batch_job_release_task: Optional[batch_models.JobReleaseTask]

   :param batch_task_display_name:  The display name for the task.
       The display name need not be unique
   :type batch_task_display_name: Optional[str]

   :param batch_task_container_settings: The settings for the container under which the Task runs
   :type batch_task_container_settings: Optional[batch_models.TaskContainerSettings]

   :param batch_start_task: A Task specified to run on each Compute Node as it joins the Pool.
       The Task runs when the Compute Node is added to the Pool or
       when the Compute Node is restarted.
   :type batch_start_task: Optional[batch_models.StartTask]

   :param batch_max_retries: The number of times to retry this batch operation before it's
       considered a failed operation. Default is 3
   :type batch_max_retries: int

   :param batch_task_resource_files: A list of files that the Batch service will
       download to the Compute Node before running the command line.
   :type batch_task_resource_files: Optional[List[batch_models.ResourceFile]]

   :param batch_task_output_files: A list of files that the Batch service will upload
       from the Compute Node after running the command line.
   :type batch_task_output_files: Optional[List[batch_models.OutputFile]]

   :param batch_task_user_identity: The user identity under which the Task runs.
       If omitted, the Task runs as a non-administrative user unique to the Task.
   :type batch_task_user_identity: Optional[batch_models.UserIdentity]

   :param target_low_priority_nodes: The desired number of low-priority Compute Nodes in the Pool.
       This property must not be specified if enable_auto_scale is set to true.
   :type target_low_priority_nodes: Optional[int]

   :param target_dedicated_nodes: The desired number of dedicated Compute Nodes in the Pool.
       This property must not be specified if enable_auto_scale is set to true.
   :type target_dedicated_nodes: Optional[int]

   :param enable_auto_scale: Whether the Pool size should automatically adjust over time. Default is false
   :type enable_auto_scale: bool

   :param auto_scale_formula: A formula for the desired number of Compute Nodes in the Pool.
       This property must not be specified if enableAutoScale is set to false.
       It is required if enableAutoScale is set to true.
   :type auto_scale_formula: Optional[str]

   :param azure_batch_conn_id: The connection id of Azure batch service
   :type azure_batch_conn_id: str

   :param use_latest_verified_vm_image_and_sku: Whether to use the latest verified virtual
       machine image and sku in the batch account. Default is false.
   :type use_latest_verified_vm_image_and_sku: bool

   :param vm_publisher: The publisher of the Azure Virtual Machines Marketplace Image.
       For example, Canonical or MicrosoftWindowsServer. Required if
       use_latest_image_and_sku is set to True
   :type vm_publisher: Optional[str]

   :param vm_offer: The offer type of the Azure Virtual Machines Marketplace Image.
       For example, UbuntuServer or WindowsServer. Required if
       use_latest_image_and_sku is set to True
   :type vm_offer: Optional[str]

   :param sku_starts_with: The starting string of the Virtual Machine SKU. Required if
       use_latest_image_and_sku is set to True
   :type sku_starts_with: Optional[str]

   :param vm_sku: The name of the virtual machine sku to use
   :type vm_sku: Optional[str]

   :param vm_version: The version of the virtual machine
   :param vm_version: Optional[str]

   :param vm_node_agent_sku_id: The node agent sku id of the virtual machine
   :type vm_node_agent_sku_id: Optional[str]

   :param os_family: The Azure Guest OS family to be installed on the virtual machines in the Pool.
   :type os_family: Optional[str]

   :param os_version: The OS family version
   :type os_version: Optional[str]

   :param timeout: The amount of time to wait for the job to complete in minutes. Default is 25
   :type timeout: int

   :param should_delete_job: Whether to delete job after execution. Default is False
   :type should_delete_job: bool

   :param should_delete_pool: Whether to delete pool after execution of jobs. Default is False
   :type should_delete_pool: bool


   .. attribute:: template_fields
      :annotation: = ['batch_pool_id', 'batch_pool_vm_size', 'batch_job_id', 'batch_task_id', 'batch_task_command_line']

      

   .. attribute:: ui_color
      :annotation: = #f0f0e4

      

   
   .. method:: _check_inputs(self)



   
   .. method:: execute(self, context: dict)



   
   .. method:: on_kill(self)



   
   .. method:: get_hook(self)

      Create and return an AzureBatchHook.



   
   .. method:: clean_up(self, pool_id: Optional[str] = None, job_id: Optional[str] = None)

      Delete the given pool and job in the batch account

      :param pool_id: The id of the pool to delete
      :type pool_id: str
      :param job_id: The id of the job to delete
      :type job_id: str




