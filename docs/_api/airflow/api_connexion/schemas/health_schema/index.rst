:mod:`airflow.api_connexion.schemas.health_schema`
==================================================

.. py:module:: airflow.api_connexion.schemas.health_schema


Module Contents
---------------

.. py:class:: BaseInfoSchema

   Bases: :class:`marshmallow.Schema`

   Base status field for metadatabase and scheduler

   .. attribute:: status
      

      


.. py:class:: MetaDatabaseInfoSchema

   Bases: :class:`airflow.api_connexion.schemas.health_schema.BaseInfoSchema`

   Schema for Metadatabase info


.. py:class:: SchedulerInfoSchema

   Bases: :class:`airflow.api_connexion.schemas.health_schema.BaseInfoSchema`

   Schema for Metadatabase info

   .. attribute:: latest_scheduler_heartbeat
      

      


.. py:class:: HeathInfoSchema

   Bases: :class:`marshmallow.Schema`

   Schema for the Health endpoint

   .. attribute:: metadatabase
      

      

   .. attribute:: scheduler
      

      


.. data:: health_schema
   

   

