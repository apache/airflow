:mod:`airflow.models.renderedtifields`
======================================

.. py:module:: airflow.models.renderedtifields

.. autoapi-nested-parse::

   Save Rendered Template Fields



Module Contents
---------------

.. py:class:: RenderedTaskInstanceFields(ti: TaskInstance, render_templates=True)

   Bases: :class:`airflow.models.base.Base`

   Save Rendered Template Fields

   .. attribute:: __tablename__
      :annotation: = rendered_task_instance_fields

      

   .. attribute:: dag_id
      

      

   .. attribute:: task_id
      

      

   .. attribute:: execution_date
      

      

   .. attribute:: rendered_fields
      

      

   .. attribute:: k8s_pod_yaml
      

      

   
   .. method:: __repr__(self)



   
   .. classmethod:: get_templated_fields(cls, ti: TaskInstance, session: Session = None)

      Get templated field for a TaskInstance from the RenderedTaskInstanceFields
      table.

      :param ti: Task Instance
      :param session: SqlAlchemy Session
      :return: Rendered Templated TI field



   
   .. classmethod:: get_k8s_pod_yaml(cls, ti: TaskInstance, session: Session = None)

      Get rendered Kubernetes Pod Yaml for a TaskInstance from the RenderedTaskInstanceFields
      table.

      :param ti: Task Instance
      :param session: SqlAlchemy Session
      :return: Kubernetes Pod Yaml



   
   .. method:: write(self, session: Session = None)

      Write instance to database

      :param session: SqlAlchemy Session



   
   .. classmethod:: delete_old_records(cls, task_id: str, dag_id: str, num_to_keep=conf.getint('core', 'max_num_rendered_ti_fields_per_task', fallback=0), session: Session = None)

      Keep only Last X (num_to_keep) number of records for a task by deleting others

      :param task_id: Task ID
      :param dag_id: Dag ID
      :param num_to_keep: Number of Records to keep
      :param session: SqlAlchemy Session




