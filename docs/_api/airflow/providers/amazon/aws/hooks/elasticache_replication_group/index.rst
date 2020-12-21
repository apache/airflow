:mod:`airflow.providers.amazon.aws.hooks.elasticache_replication_group`
=======================================================================

.. py:module:: airflow.providers.amazon.aws.hooks.elasticache_replication_group


Module Contents
---------------

.. py:class:: ElastiCacheReplicationGroupHook(max_retries: int = 10, exponential_back_off_factor: float = 1, initial_poke_interval: float = 60, *args, **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with AWS ElastiCache

   :param max_retries: Max retries for checking availability of and deleting replication group
           If this is not supplied then this is defaulted to 10
   :type max_retries: int
   :param exponential_back_off_factor: Multiplication factor for deciding next sleep time
           If this is not supplied then this is defaulted to 1
   :type exponential_back_off_factor: float
   :param initial_poke_interval: Initial sleep time in seconds
           If this is not supplied then this is defaulted to 60 seconds
   :type initial_poke_interval: float

   .. attribute:: TERMINAL_STATES
      

      

   
   .. method:: create_replication_group(self, config: dict)

      Call ElastiCache API for creating a replication group

      :param config: Configuration for creating the replication group
      :type config: dict
      :return: Response from ElastiCache create replication group API
      :rtype: dict



   
   .. method:: delete_replication_group(self, replication_group_id: str)

      Call ElastiCache API for deleting a replication group

      :param replication_group_id: ID of replication group to delete
      :type replication_group_id: str
      :return: Response from ElastiCache delete replication group API
      :rtype: dict



   
   .. method:: describe_replication_group(self, replication_group_id: str)

      Call ElastiCache API for describing a replication group

      :param replication_group_id: ID of replication group to describe
      :type replication_group_id: str
      :return: Response from ElastiCache describe replication group API
      :rtype: dict



   
   .. method:: get_replication_group_status(self, replication_group_id: str)

      Get current status of replication group

      :param replication_group_id: ID of replication group to check for status
      :type replication_group_id: str
      :return: Current status of replication group
      :rtype: str



   
   .. method:: is_replication_group_available(self, replication_group_id: str)

      Helper for checking if replication group is available or not

      :param replication_group_id: ID of replication group to check for availability
      :type replication_group_id: str
      :return: True if available else False
      :rtype: bool



   
   .. method:: wait_for_availability(self, replication_group_id: str, initial_sleep_time: Optional[float] = None, exponential_back_off_factor: Optional[float] = None, max_retries: Optional[int] = None)

      Check if replication group is available or not by performing a describe over it

      :param replication_group_id: ID of replication group to check for availability
      :type replication_group_id: str
      :param initial_sleep_time: Initial sleep time in seconds
          If this is not supplied then this is defaulted to class level value
      :type initial_sleep_time: float
      :param exponential_back_off_factor: Multiplication factor for deciding next sleep time
          If this is not supplied then this is defaulted to class level value
      :type exponential_back_off_factor: float
      :param max_retries: Max retries for checking availability of replication group
          If this is not supplied then this is defaulted to class level value
      :type max_retries: int
      :return: True if replication is available else False
      :rtype: bool



   
   .. method:: wait_for_deletion(self, replication_group_id: str, initial_sleep_time: Optional[float] = None, exponential_back_off_factor: Optional[float] = None, max_retries: Optional[int] = None)

      Helper for deleting a replication group ensuring it is either deleted or can't be deleted

      :param replication_group_id: ID of replication to delete
      :type replication_group_id: str
      :param initial_sleep_time: Initial sleep time in second
          If this is not supplied then this is defaulted to class level value
      :type initial_sleep_time: float
      :param exponential_back_off_factor: Multiplication factor for deciding next sleep time
          If this is not supplied then this is defaulted to class level value
      :type exponential_back_off_factor: float
      :param max_retries: Max retries for checking availability of replication group
          If this is not supplied then this is defaulted to class level value
      :type max_retries: int
      :return: Response from ElastiCache delete replication group API and flag to identify if deleted or not
      :rtype: (dict, bool)



   
   .. method:: ensure_delete_replication_group(self, replication_group_id: str, initial_sleep_time: Optional[float] = None, exponential_back_off_factor: Optional[float] = None, max_retries: Optional[int] = None)

      Delete a replication group ensuring it is either deleted or can't be deleted

      :param replication_group_id: ID of replication to delete
      :type replication_group_id: str
      :param initial_sleep_time: Initial sleep time in second
          If this is not supplied then this is defaulted to class level value
      :type initial_sleep_time: float
      :param exponential_back_off_factor: Multiplication factor for deciding next sleep time
          If this is not supplied then this is defaulted to class level value
      :type exponential_back_off_factor: float
      :param max_retries: Max retries for checking availability of replication group
          If this is not supplied then this is defaulted to class level value
      :type max_retries: int
      :return: Response from ElastiCache delete replication group API
      :rtype: dict
      :raises AirflowException: If replication group is not deleted




