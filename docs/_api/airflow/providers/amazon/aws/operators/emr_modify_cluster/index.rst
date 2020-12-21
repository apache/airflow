:mod:`airflow.providers.amazon.aws.operators.emr_modify_cluster`
================================================================

.. py:module:: airflow.providers.amazon.aws.operators.emr_modify_cluster


Module Contents
---------------

.. py:class:: EmrModifyClusterOperator(*, cluster_id: str, step_concurrency_level: int, aws_conn_id: str = 'aws_default', **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   An operator that modifies an existing EMR cluster.
   :param cluster_id: cluster identifier
   :type cluster_id: str
   :param step_concurrency_level: Concurrency of the cluster
   :type step_concurrency_level: int
   :param aws_conn_id: aws connection to uses
   :type aws_conn_id: str
   :param do_xcom_push: if True, cluster_id is pushed to XCom with key cluster_id.
   :type do_xcom_push: bool

   .. attribute:: template_fields
      :annotation: = ['cluster_id', 'step_concurrency_level']

      

   .. attribute:: template_ext
      :annotation: = []

      

   .. attribute:: ui_color
      :annotation: = #f9c915

      

   
   .. method:: execute(self, context: Dict[str, Any])




