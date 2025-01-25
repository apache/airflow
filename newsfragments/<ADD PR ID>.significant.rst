.. Write a short and imperative summary of this changes

.. Provide additional contextual information

.. Check the type of change that applies to this change
.. Dag changes: requires users to change their dag code
.. Config changes: requires users to change their airflow config
.. API changes: requires users to change their Airflow REST API calls
.. CLI changes: requires users to change their Airflow CLI usage
.. Behaviour changes: the existing code won't break, but the behavior is different
.. Plugin changes: requires users to change their Airflow plugin implementation
.. Dependency changes: requires users to change their dependencies (e.g., Postgres 12)
.. Code interface changes: requires users to change other implementations (e.g., auth manager)
In Airflow 3.0, the ``timer_unit_consistency`` setting in the ``metrics`` section is removed as it is now the default behaviour.
This is done to standardize all timer and timing metrics to milliseconds across all metric loggers.

Airflow 2.11 introduced the ``timer_unit_consistency`` setting in the ``metrics`` section of the configuration file. The
default value was ``False`` which meant that the timer and timing metrics were logged in seconds. This was done to maintain
backwards compatibility with the previous versions of Airflow.

In Airflow 3.0, the ``smtp`` setting section, the base operator parameters ``email``, ``email_on_retry`` and ``email_on_failure``, and the ``airflow.operators.email.EmailOperator`` are removed.

Instead of using the operator ``EmailOperator`` from the ``airflow.operators.email`` module, users should use the operator ``EmailOperator`` from the ``smtp`` provider in the module ``airflow.providers.smtp.operators.smtp``.

And to send emails on failure or retry, users should use the ```airflow.providers.smtp.notifications.smtp.SmtpNotifier`` with the ``on_failure_callback`` and ``on_retry_callback`` parameters. For more details you can check `this guide <https://airflow.apache.org/docs/apache-airflow-providers-smtp/stable/notifications/smtp_notifier_howto_guide.html>`_.

* Types of change

  * [ ] Dag changes
  * [x] Config changes
  * [ ] API changes
  * [ ] CLI changes
  * [x] Behaviour changes
  * [ ] Plugin changes
  * [ ] Dependency changes
  * [x] Code interface changes

* Migration rules needed

.. TODO: create the migration rule that:
.. * Import ``EmailOperator`` from ``airflow.providers.smtp.operators.smtp`` instead of ``airflow.operators.email``
.. * Detect and remove the ``email``, ``email_on_retry`` and ``email_on_failure`` parameters from the operator parameters (and replace them by an ``SmtpNotifier`` with the ``on_failure_callback`` and ``on_retry_callback`` parameters)?
