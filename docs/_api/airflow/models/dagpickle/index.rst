:mod:`airflow.models.dagpickle`
===============================

.. py:module:: airflow.models.dagpickle


Module Contents
---------------

.. py:class:: DagPickle(dag)

   Bases: :class:`airflow.models.base.Base`

   Dags can originate from different places (user repos, master repo, ...)
   and also get executed in different places (different executors). This
   object represents a version of a DAG and becomes a source of truth for
   a BackfillJob execution. A pickle is a native python serialized object,
   and in this case gets stored in the database for the duration of the job.

   The executors pick up the DagPickle id and read the dag definition from
   the database.

   .. attribute:: id
      

      

   .. attribute:: pickle
      

      

   .. attribute:: created_dttm
      

      

   .. attribute:: pickle_hash
      

      

   .. attribute:: __tablename__
      :annotation: = dag_pickle

      


