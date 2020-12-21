:mod:`airflow.utils.trigger_rule`
=================================

.. py:module:: airflow.utils.trigger_rule


Module Contents
---------------

.. py:class:: TriggerRule

   Class with task's trigger rules.

   .. attribute:: ALL_SUCCESS
      :annotation: = all_success

      

   .. attribute:: ALL_FAILED
      :annotation: = all_failed

      

   .. attribute:: ALL_DONE
      :annotation: = all_done

      

   .. attribute:: ONE_SUCCESS
      :annotation: = one_success

      

   .. attribute:: ONE_FAILED
      :annotation: = one_failed

      

   .. attribute:: NONE_FAILED
      :annotation: = none_failed

      

   .. attribute:: NONE_FAILED_OR_SKIPPED
      :annotation: = none_failed_or_skipped

      

   .. attribute:: NONE_SKIPPED
      :annotation: = none_skipped

      

   .. attribute:: DUMMY
      :annotation: = dummy

      

   .. attribute:: _ALL_TRIGGER_RULES
      :annotation: :Set[str]

      

   
   .. classmethod:: is_valid(cls, trigger_rule)

      Validates a trigger rule.



   
   .. classmethod:: all_triggers(cls)

      Returns all trigger rules.




