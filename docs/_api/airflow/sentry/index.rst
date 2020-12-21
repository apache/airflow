:mod:`airflow.sentry`
=====================

.. py:module:: airflow.sentry

.. autoapi-nested-parse::

   Sentry Integration



Module Contents
---------------

.. data:: log
   

   

.. py:class:: DummySentry

   Blank class for Sentry.

   
   .. classmethod:: add_tagging(cls, task_instance)

      Blank function for tagging.



   
   .. classmethod:: add_breadcrumbs(cls, task_instance, session=None)

      Blank function for breadcrumbs.



   
   .. classmethod:: enrich_errors(cls, run)

      Blank function for formatting a TaskInstance._run_raw_task.



   
   .. method:: flush(self)

      Blank function for flushing errors.




.. data:: Sentry
   :annotation: :DummySentry

   

.. py:class:: ConfiguredSentry

   Bases: :class:`airflow.sentry.DummySentry`

   Configure Sentry SDK.

   .. attribute:: SCOPE_TAGS
      

      

   .. attribute:: SCOPE_CRUMBS
      

      

   .. attribute:: UNSUPPORTED_SENTRY_OPTIONS
      

      

   
   .. method:: add_tagging(self, task_instance)

      Function to add tagging for a task_instance.



   
   .. method:: add_breadcrumbs(self, task_instance, session=None)

      Function to add breadcrumbs inside of a task_instance.



   
   .. method:: enrich_errors(self, func)

      Wrap TaskInstance._run_raw_task to support task specific tags and breadcrumbs.



   
   .. method:: flush(self)




