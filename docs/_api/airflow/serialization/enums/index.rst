:mod:`airflow.serialization.enums`
==================================

.. py:module:: airflow.serialization.enums

.. autoapi-nested-parse::

   Enums for DAG serialization.



Module Contents
---------------

.. py:class:: Encoding

   Bases: :class:`str`, :class:`enum.Enum`

   Enum of encoding constants.

   .. attribute:: TYPE
      :annotation: = __type

      

   .. attribute:: VAR
      :annotation: = __var

      


.. py:class:: DagAttributeTypes

   Bases: :class:`str`, :class:`enum.Enum`

   Enum of supported attribute types of DAG.

   .. attribute:: DAG
      :annotation: = dag

      

   .. attribute:: OP
      :annotation: = operator

      

   .. attribute:: DATETIME
      :annotation: = datetime

      

   .. attribute:: TIMEDELTA
      :annotation: = timedelta

      

   .. attribute:: TIMEZONE
      :annotation: = timezone

      

   .. attribute:: RELATIVEDELTA
      :annotation: = relativedelta

      

   .. attribute:: DICT
      :annotation: = dict

      

   .. attribute:: SET
      :annotation: = set

      

   .. attribute:: TUPLE
      :annotation: = tuple

      

   .. attribute:: POD
      :annotation: = k8s.V1Pod

      

   .. attribute:: TASK_GROUP
      :annotation: = taskgroup

      


