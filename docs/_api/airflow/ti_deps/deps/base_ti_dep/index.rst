:mod:`airflow.ti_deps.deps.base_ti_dep`
=======================================

.. py:module:: airflow.ti_deps.deps.base_ti_dep


Module Contents
---------------

.. py:class:: BaseTIDep

   Abstract base class for dependencies that must be satisfied in order for task
   instances to run. For example, a task that can only run if a certain number of its
   upstream tasks succeed. This is an abstract class and must be subclassed to be used.

   .. attribute:: IGNOREABLE
      :annotation: = False

      

   .. attribute:: IS_TASK_DEP
      :annotation: = False

      

   .. attribute:: name
      

      The human-readable name for the dependency. Use the classname as the default name
      if this method is not overridden in the subclass.


   
   .. method:: __eq__(self, other)



   
   .. method:: __hash__(self)



   
   .. method:: __repr__(self)



   
   .. method:: _get_dep_statuses(self, ti, session, dep_context)

      Abstract method that returns an iterable of TIDepStatus objects that describe
      whether the given task instance has this dependency met.

      For example a subclass could return an iterable of TIDepStatus objects, each one
      representing if each of the passed in task's upstream tasks succeeded or not.

      :param ti: the task instance to get the dependency status for
      :type ti: airflow.models.TaskInstance
      :param session: database session
      :type session: sqlalchemy.orm.session.Session
      :param dep_context: the context for which this dependency should be evaluated for
      :type dep_context: DepContext



   
   .. method:: get_dep_statuses(self, ti, session, dep_context=None)

      Wrapper around the private _get_dep_statuses method that contains some global
      checks for all dependencies.

      :param ti: the task instance to get the dependency status for
      :type ti: airflow.models.TaskInstance
      :param session: database session
      :type session: sqlalchemy.orm.session.Session
      :param dep_context: the context for which this dependency should be evaluated for
      :type dep_context: DepContext



   
   .. method:: is_met(self, ti, session, dep_context=None)

      Returns whether or not this dependency is met for a given task instance. A
      dependency is considered met if all of the dependency statuses it reports are
      passing.

      :param ti: the task instance to see if this dependency is met for
      :type ti: airflow.models.TaskInstance
      :param session: database session
      :type session: sqlalchemy.orm.session.Session
      :param dep_context: The context this dependency is being checked under that stores
          state that can be used by this dependency.
      :type dep_context: BaseDepContext



   
   .. method:: get_failure_reasons(self, ti, session, dep_context=None)

      Returns an iterable of strings that explain why this dependency wasn't met.

      :param ti: the task instance to see if this dependency is met for
      :type ti: airflow.models.TaskInstance
      :param session: database session
      :type session: sqlalchemy.orm.session.Session
      :param dep_context: The context this dependency is being checked under that stores
          state that can be used by this dependency.
      :type dep_context: BaseDepContext



   
   .. method:: _failing_status(self, reason='')



   
   .. method:: _passing_status(self, reason='')




.. py:class:: TIDepStatus

   Bases: :class:`typing.NamedTuple`

   Dependency status for a specific task instance indicating whether or not the task
   instance passed the dependency.

   .. attribute:: dep_name
      :annotation: :str

      

   .. attribute:: passed
      :annotation: :bool

      

   .. attribute:: reason
      :annotation: :str

      


