:mod:`airflow.providers.microsoft.azure.hooks.azure_batch`
==========================================================

.. py:module:: airflow.providers.microsoft.azure.hooks.azure_batch


Module Contents
---------------

.. py:class:: AzureBatchHook(azure_batch_conn_id: str = 'azure_batch_default')

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Hook for Azure Batch APIs

   Account name and account key should be in login and password parameters.
   The account url should be in extra parameter as account_url

   
   .. method:: _connection(self)

      Get connected to azure batch service



   
   .. method:: get_conn(self)

      Get the batch client connection

      :return: Azure batch client



   
   .. method:: configure_pool(self, pool_id: str, vm_size: Optional[str] = None, vm_publisher: Optional[str] = None, vm_offer: Optional[str] = None, sku_starts_with: Optional[str] = None, vm_sku: Optional[str] = None, vm_version: Optional[str] = None, vm_node_agent_sku_id: Optional[str] = None, os_family: Optional[str] = None, os_version: Optional[str] = None, display_name: Optional[str] = None, target_dedicated_nodes: Optional[int] = None, use_latest_image_and_sku: bool = False, **kwargs)

      Configures a pool

      :param pool_id: A string that uniquely identifies the Pool within the Account
      :type pool_id: str

      :param vm_size: The size of virtual machines in the Pool.
      :type vm_size: str

      :param display_name: The display name for the Pool
      :type display_name: str

      :param target_dedicated_nodes: The desired number of dedicated Compute Nodes in the Pool.
      :type target_dedicated_nodes: Optional[int]

      :param use_latest_image_and_sku: Whether to use the latest verified vm image and sku
      :type use_latest_image_and_sku: bool

      :param vm_publisher: The publisher of the Azure Virtual Machines Marketplace Image.
          For example, Canonical or MicrosoftWindowsServer.
      :type vm_publisher: Optional[str]

      :param vm_offer: The offer type of the Azure Virtual Machines Marketplace Image.
          For example, UbuntuServer or WindowsServer.
      :type vm_offer: Optional[str]

      :param sku_starts_with: The start name of the sku to search
      :type sku_starts_with: Optional[str]

      :param vm_sku: The name of the virtual machine sku to use
      :type vm_sku: Optional[str]

      :param vm_version: The version of the virtual machine
      :param vm_version: str

      :param vm_node_agent_sku_id: The node agent sku id of the virtual machine
      :type vm_node_agent_sku_id: Optional[str]

      :param os_family: The Azure Guest OS family to be installed on the virtual machines in the Pool.
      :type os_family: Optional[str]

      :param os_version: The OS family version
      :type os_version: Optional[str]



   
   .. method:: create_pool(self, pool: PoolAddParameter)

      Creates a pool if not already existing

      :param pool: the pool object to create
      :type pool: batch_models.PoolAddParameter



   
   .. method:: _get_latest_verified_image_vm_and_sku(self, publisher: Optional[str] = None, offer: Optional[str] = None, sku_starts_with: Optional[str] = None)

      Get latest verified image vm and sku

      :param publisher: The publisher of the Azure Virtual Machines Marketplace Image.
          For example, Canonical or MicrosoftWindowsServer.
      :type publisher: str
      :param offer: The offer type of the Azure Virtual Machines Marketplace Image.
          For example, UbuntuServer or WindowsServer.
      :type offer: str
      :param sku_starts_with: The start name of the sku to search
      :type sku_starts_with: str



   
   .. method:: wait_for_all_node_state(self, pool_id: str, node_state: Set)

      Wait for all nodes in a pool to reach given states

      :param pool_id: A string that identifies the pool
      :type pool_id: str
      :param node_state: A set of batch_models.ComputeNodeState
      :type node_state: set



   
   .. method:: configure_job(self, job_id: str, pool_id: str, display_name: Optional[str] = None, **kwargs)

      Configures a job for use in the pool

      :param job_id: A string that uniquely identifies the job within the account
      :type job_id: str
      :param pool_id: A string that identifies the pool
      :type pool_id: str
      :param display_name: The display name for the job
      :type display_name: str



   
   .. method:: create_job(self, job: JobAddParameter)

      Creates a job in the pool

      :param job: The job object to create
      :type job: batch_models.JobAddParameter



   
   .. method:: configure_task(self, task_id: str, command_line: str, display_name: Optional[str] = None, container_settings=None, **kwargs)

      Creates a task

      :param task_id: A string that identifies the task to create
      :type task_id: str
      :param command_line: The command line of the Task.
      :type command_line: str
      :param display_name: A display name for the Task
      :type display_name: str
      :param container_settings: The settings for the container under which the Task runs.
          If the Pool that will run this Task has containerConfiguration set,
          this must be set as well. If the Pool that will run this Task doesn't have
          containerConfiguration set, this must not be set.
      :type container_settings: batch_models.TaskContainerSettings



   
   .. method:: add_single_task_to_job(self, job_id: str, task: TaskAddParameter)

      Add a single task to given job if it doesn't exist

      :param job_id: A string that identifies the given job
      :type job_id: str
      :param task: The task to add
      :type task: batch_models.TaskAddParameter



   
   .. method:: wait_for_job_tasks_to_complete(self, job_id: str, timeout: int)

      Wait for tasks in a particular job to complete

      :param job_id: A string that identifies the job
      :type job_id: str
      :param timeout: The amount of time to wait before timing out in minutes
      :type timeout: int




