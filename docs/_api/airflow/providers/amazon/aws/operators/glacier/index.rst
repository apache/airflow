:mod:`airflow.providers.amazon.aws.operators.glacier`
=====================================================

.. py:module:: airflow.providers.amazon.aws.operators.glacier


Module Contents
---------------

.. py:class:: GlacierCreateJobOperator(*, aws_conn_id='aws_default', vault_name: str, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Initiate an Amazon Glacier inventory-retrieval job

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GlacierCreateJobOperator`

   :param aws_conn_id: The reference to the AWS connection details
   :type aws_conn_id: str
   :param vault_name: the Glacier vault on which job is executed
   :type vault_name: str

   .. attribute:: template_fields
      :annotation: = ['vault_name']

      

   
   .. method:: execute(self, context)




