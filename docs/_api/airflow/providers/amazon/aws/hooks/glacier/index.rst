:mod:`airflow.providers.amazon.aws.hooks.glacier`
=================================================

.. py:module:: airflow.providers.amazon.aws.hooks.glacier


Module Contents
---------------

.. py:class:: GlacierHook(aws_conn_id: str = 'aws_default')

   Bases: :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Hook for connection with Amazon Glacier

   
   .. method:: retrieve_inventory(self, vault_name: str)

      Initiate an Amazon Glacier inventory-retrieval job

      :param vault_name: the Glacier vault on which job is executed
      :type vault_name: str



   
   .. method:: retrieve_inventory_results(self, vault_name: str, job_id: str)

      Retrieve the results of an Amazon Glacier inventory-retrieval job

      :param vault_name: the Glacier vault on which job is executed
      :type vault_name: string
      :param job_id: the job ID was returned by retrieve_inventory()
      :type job_id: str



   
   .. method:: describe_job(self, vault_name: str, job_id: str)

      Retrieve the status of an Amazon S3 Glacier job, such as an
      inventory-retrieval job

      :param vault_name: the Glacier vault on which job is executed
      :type vault_name: string
      :param job_id: the job ID was returned by retrieve_inventory()
      :type job_id: str




