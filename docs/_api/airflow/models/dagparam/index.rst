:mod:`airflow.models.dagparam`
==============================

.. py:module:: airflow.models.dagparam


Module Contents
---------------

.. py:class:: DagParam(current_dag, name: str, default: Optional[Any] = None)

   Class that represents a DAG run parameter.

   It can be used to parameterize your dags. You can overwrite its value by setting it on conf
   when you trigger your DagRun.

   This can also be used in templates by accessing {{context.params}} dictionary.

   **Example**:

       with DAG(...) as dag:
         EmailOperator(subject=dag.param('subject', 'Hi from Airflow!'))

   :param current_dag: Dag being used for parameter.
   :type current_dag: airflow.models.DAG
   :param name: key value which is used to set the parameter
   :type name: str
   :param default: Default value used if no parameter was set.
   :type default: Any

   
   .. method:: resolve(self, context: Dict)

      Pull DagParam value from DagRun context. This method is run during ``op.execute()``.




