Running Airflow with systemd
============================

Airflow can integrate with systemd based systems. This makes watching your
daemons easy as systemd can take care of restarting a daemon on failure.
In the ``scripts/systemd`` directory you can find unit files that
have been tested on Redhat based systems. You can copy those to
``/usr/lib/systemd/system``. It is assumed that Airflow will run under
``airflow:airflow``. If not (or if you are running on a non Redhat
based system) you probably need to adjust the unit files.

Environment configuration is picked up from ``/etc/sysconfig/airflow``.
An example file is supplied. Make sure to specify the ``SCHEDULER_RUNS``
variable in this file when you run the scheduler. You
can also define here, for example, ``AIRFLOW_HOME`` or ``AIRFLOW_CONFIG``.
