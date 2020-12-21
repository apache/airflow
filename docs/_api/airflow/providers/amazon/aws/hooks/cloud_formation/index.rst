:mod:`airflow.providers.amazon.aws.hooks.cloud_formation`
=========================================================

.. py:module:: airflow.providers.amazon.aws.hooks.cloud_formation

.. autoapi-nested-parse::

   This module contains AWS CloudFormation Hook



Module Contents
---------------

.. py:class:: AWSCloudFormationHook(*args, **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with AWS CloudFormation.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   
   .. method:: get_stack_status(self, stack_name: Union[client, resource])

      Get stack status from CloudFormation.



   
   .. method:: create_stack(self, stack_name: str, params: dict)

      Create stack in CloudFormation.

      :param stack_name: stack_name.
      :type stack_name: str
      :param params: parameters to be passed to CloudFormation.
      :type params: dict



   
   .. method:: delete_stack(self, stack_name: str, params: Optional[dict] = None)

      Delete stack in CloudFormation.

      :param stack_name: stack_name.
      :type stack_name: str
      :param params: parameters to be passed to CloudFormation (optional).
      :type params: dict




