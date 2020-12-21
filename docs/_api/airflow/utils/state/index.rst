:mod:`airflow.utils.state`
==========================

.. py:module:: airflow.utils.state


Module Contents
---------------

.. py:class:: State

   Static class with task instance states constants and color method to
   avoid hardcoding.

   .. attribute:: NONE
      :annotation: :Const.NoneType(value=None)

      

   .. attribute:: REMOVED
      :annotation: = removed

      

   .. attribute:: SCHEDULED
      :annotation: = scheduled

      

   .. attribute:: QUEUED
      :annotation: = queued

      

   .. attribute:: RUNNING
      :annotation: = running

      

   .. attribute:: SUCCESS
      :annotation: = success

      

   .. attribute:: SHUTDOWN
      :annotation: = shutdown

      

   .. attribute:: FAILED
      :annotation: = failed

      

   .. attribute:: UP_FOR_RETRY
      :annotation: = up_for_retry

      

   .. attribute:: UP_FOR_RESCHEDULE
      :annotation: = up_for_reschedule

      

   .. attribute:: UPSTREAM_FAILED
      :annotation: = upstream_failed

      

   .. attribute:: SKIPPED
      :annotation: = skipped

      

   .. attribute:: SENSING
      :annotation: = sensing

      

   .. attribute:: task_states
      

      

   .. attribute:: dag_states
      

      

   .. attribute:: state_color
      

      

   .. attribute:: running
      

      A list of states indicating that a task is being executed.


   .. attribute:: finished
      

      A list of states indicating a task has reached a terminal state (i.e. it has "finished") and needs no
      further action.

      Note that the attempt could have resulted in failure or have been
      interrupted; or perhaps never run at all (skip, or upstream_failed) in any
      case, it is no longer running.


   .. attribute:: unfinished
      

      A list of states indicating that a task either has not completed
      a run or has not even started.


   .. attribute:: failed_states
      

      A list of states indicating that a task or dag is a failed state.


   .. attribute:: success_states
      

      A list of states indicating that a task or dag is a success state.


   
   .. classmethod:: color(cls, state)

      Returns color for a state.



   
   .. classmethod:: color_fg(cls, state)

      Black&white colors for a state.




.. py:class:: PokeState

   Static class with poke states constants used in smart operator.

   .. attribute:: LANDED
      :annotation: = landed

      

   .. attribute:: NOT_LANDED
      :annotation: = not_landed

      

   .. attribute:: POKE_EXCEPTION
      :annotation: = poke_exception

      


