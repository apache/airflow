Task instance listener hooks now receive a ``msg`` keyword argument carrying
short canonical context for the state change (``"started"``, ``"success"``,
``"skipped"``, ``"failed"``, ``"up_for_retry"`` from the worker;
``"manually_set_to_*"`` when the state was changed via the API).

This mirrors the DagRun listener pattern and lets listener implementations
route or filter events without re-deriving intent from other fields. Existing
``hookimpl`` implementations that don't declare ``msg`` continue to work
unchanged — pluggy passes only the parameters each implementation declares.

Affected hooks: ``on_task_instance_running``, ``on_task_instance_success``,
``on_task_instance_failed``, ``on_task_instance_skipped``.
