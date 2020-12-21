:mod:`airflow.jobs.base_job`
============================

.. py:module:: airflow.jobs.base_job


Module Contents
---------------

.. py:class:: BaseJob(executor=None, heartrate=None, *args, **kwargs)

   Bases: :class:`airflow.models.base.Base`, :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   Abstract class to be derived for jobs. Jobs are processing items with state
   and duration that aren't task instances. For instance a BackfillJob is
   a collection of task instance runs, but should have its own state, start
   and end time.

   .. attribute:: __tablename__
      :annotation: = job

      

   .. attribute:: id
      

      

   .. attribute:: dag_id
      

      

   .. attribute:: state
      

      

   .. attribute:: job_type
      

      

   .. attribute:: start_date
      

      

   .. attribute:: end_date
      

      

   .. attribute:: latest_heartbeat
      

      

   .. attribute:: executor_class
      

      

   .. attribute:: hostname
      

      

   .. attribute:: unixname
      

      

   .. attribute:: __mapper_args__
      

      

   .. attribute:: __table_args__
      

      

   .. attribute:: task_instances_enqueued
      

      

   .. attribute:: dag_runs
      

      TaskInstances which have been enqueued by this Job.

      Only makes sense for SchedulerJob and BackfillJob instances.


   .. attribute:: heartrate
      

      

   
   .. classmethod:: most_recent_job(cls, session=None)

      Return the most recent job of this type, if any, based on last
      heartbeat received.

      This method should be called on a subclass (i.e. on SchedulerJob) to
      return jobs of that type.

      :param session: Database session
      :rtype: BaseJob or None



   
   .. method:: is_alive(self, grace_multiplier=2.1)

      Is this job currently alive.

      We define alive as in a state of RUNNING, and having sent a heartbeat
      within a multiple of the heartrate (default of 2.1)

      :param grace_multiplier: multiplier of heartrate to require heart beat
          within
      :type grace_multiplier: number
      :rtype: boolean



   
   .. method:: kill(self, session=None)

      Handles on_kill callback and updates state in database.



   
   .. method:: on_kill(self)

      Will be called when an external kill command is received



   
   .. method:: heartbeat_callback(self, session=None)

      Callback that is called during heartbeat. This method should be overwritten.



   
   .. method:: heartbeat(self, only_if_necessary: bool = False)

      Heartbeats update the job's entry in the database with a timestamp
      for the latest_heartbeat and allows for the job to be killed
      externally. This allows at the system level to monitor what is
      actually active.

      For instance, an old heartbeat for SchedulerJob would mean something
      is wrong.

      This also allows for any job to be killed externally, regardless
      of who is running it or on which machine it is running.

      Note that if your heart rate is set to 60 seconds and you call this
      method after 10 seconds of processing since the last heartbeat, it
      will sleep 50 seconds to complete the 60 seconds and keep a steady
      heart rate. If you go over 60 seconds before calling it, it won't
      sleep at all.

      :param only_if_necessary: If the heartbeat is not yet due then do
          nothing (don't update column, don't call ``heartbeat_callback``)
      :type only_if_necessary: boolean



   
   .. method:: run(self)

      Starts the job.



   
   .. method:: _execute(self)




