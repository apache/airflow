Running Airflow with upstart
============================

Airflow can integrate with upstart based systems. Upstart automatically starts all airflow services for which you
have a corresponding ``*.conf`` file in ``/etc/init`` upon system boot. On failure, upstart automatically restarts
the process (until it reaches re-spawn limit set in a ``*.conf`` file).

You can find sample upstart job files in the ``scripts/upstart`` directory. These files have been tested on
Ubuntu 14.04 LTS. You may have to adjust ``start on`` and ``stop on`` stanzas to make it work on other upstart
systems. Some of the possible options are listed in ``scripts/upstart/README``.

Modify ``*.conf`` files as needed and copy to ``/etc/init`` directory. It is assumed that airflow will run
under ``airflow:airflow``. Change ``setuid`` and ``setgid`` in ``*.conf`` files if you use other user/group

You can use ``initctl`` to manually start, stop, view status of the airflow process that has been
integrated with upstart

.. code-block:: bash

    initctl airflow-webserver status
