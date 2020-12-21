:mod:`airflow.providers.microsoft.azure.operators.azure_container_instances`
============================================================================

.. py:module:: airflow.providers.microsoft.azure.operators.azure_container_instances


Module Contents
---------------

.. data:: Volume
   

   

.. data:: DEFAULT_ENVIRONMENT_VARIABLES
   :annotation: :Dict[str, str]

   

.. data:: DEFAULT_SECURED_VARIABLES
   :annotation: :Sequence[str] = []

   

.. data:: DEFAULT_VOLUMES
   :annotation: :Sequence[Volume] = []

   

.. data:: DEFAULT_MEMORY_IN_GB
   :annotation: = 2.0

   

.. data:: DEFAULT_CPU
   :annotation: = 1.0

   

.. py:class:: AzureContainerInstancesOperator(*, ci_conn_id: str, registry_conn_id: Optional[str], resource_group: str, name: str, image: str, region: str, environment_variables: Optional[dict] = None, secured_variables: Optional[str] = None, volumes: Optional[list] = None, memory_in_gb: Optional[Any] = None, cpu: Optional[Any] = None, gpu: Optional[Any] = None, command: Optional[List[str]] = None, remove_on_error: bool = True, fail_if_exists: bool = True, tags: Optional[Dict[str, str]] = None, os_type: str = 'Linux', restart_policy: str = 'Never', ip_address: Optional[IpAddress] = None, ports: Optional[List[ContainerPort]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Start a container on Azure Container Instances

   :param ci_conn_id: connection id of a service principal which will be used
       to start the container instance
   :type ci_conn_id: str
   :param registry_conn_id: connection id of a user which can login to a
       private docker registry. If None, we assume a public registry
   :type registry_conn_id: Optional[str]
   :param resource_group: name of the resource group wherein this container
       instance should be started
   :type resource_group: str
   :param name: name of this container instance. Please note this name has
       to be unique in order to run containers in parallel.
   :type name: str
   :param image: the docker image to be used
   :type image: str
   :param region: the region wherein this container instance should be started
   :type region: str
   :param environment_variables: key,value pairs containing environment
       variables which will be passed to the running container
   :type environment_variables: Optional[dict]
   :param secured_variables: names of environmental variables that should not
       be exposed outside the container (typically passwords).
   :type secured_variables: Optional[str]
   :param volumes: list of ``Volume`` tuples to be mounted to the container.
       Currently only Azure Fileshares are supported.
   :type volumes: list[<conn_id, account_name, share_name, mount_path, read_only>]
   :param memory_in_gb: the amount of memory to allocate to this container
   :type memory_in_gb: double
   :param cpu: the number of cpus to allocate to this container
   :type cpu: double
   :param gpu: GPU Resource for the container.
   :type gpu: azure.mgmt.containerinstance.models.GpuResource
   :param command: the command to run inside the container
   :type command: Optional[List[str]]
   :param container_timeout: max time allowed for the execution of
       the container instance.
   :type container_timeout: datetime.timedelta
   :param tags: azure tags as dict of str:str
   :type tags: Optional[dict[str, str]]
   :param os_type: The operating system type required by the containers
       in the container group. Possible values include: 'Windows', 'Linux'
   :type os_type: str
   :param restart_policy: Restart policy for all containers within the container group.
       Possible values include: 'Always', 'OnFailure', 'Never'
   :type restart_policy: str
   :param ip_address: The IP address type of the container group.
   :type ip_address: IpAddress

   **Example**::

               AzureContainerInstancesOperator(
                   ci_conn_id = "azure_service_principal",
                   registry_conn_id = "azure_registry_user",
                   resource_group = "my-resource-group",
                   name = "my-container-name-{{ ds }}",
                   image = "myprivateregistry.azurecr.io/my_container:latest",
                   region = "westeurope",
                   environment_variables = {"MODEL_PATH":  "my_value",
                    "POSTGRES_LOGIN": "{{ macros.connection('postgres_default').login }}",
                    "POSTGRES_PASSWORD": "{{ macros.connection('postgres_default').password }}",
                    "JOB_GUID": "{{ ti.xcom_pull(task_ids='task1', key='guid') }}" },
                   secured_variables = ['POSTGRES_PASSWORD'],
                   volumes = [("azure_wasb_conn_id",
                           "my_storage_container",
                           "my_fileshare",
                           "/input-data",
                       True),],
                   memory_in_gb=14.0,
                   cpu=4.0,
                   gpu=GpuResource(count=1, sku='K80'),
                   command=["/bin/echo", "world"],
                   task_id="start_container"
               )

   .. attribute:: template_fields
      :annotation: = ['name', 'image', 'command', 'environment_variables']

      

   
   .. method:: execute(self, context: dict)



   
   .. method:: on_kill(self)



   
   .. method:: _monitor_logging(self, resource_group: str, name: str)



   
   .. method:: _log_last(self, logs: Optional[list], last_line_logged: Any)



   
   .. staticmethod:: _check_name(name: str)




