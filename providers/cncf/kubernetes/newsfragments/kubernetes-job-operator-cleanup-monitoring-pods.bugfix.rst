Fix pod cleanup gaps in ``KubernetesJobOperator``.
``execute()`` and ``execute_complete()`` now consistently clean up pods discovered via ``get_pods()``,
including deferrable resume paths where pod lookup can fail.
The inherited ``on_finish_action`` parameter (``delete_pod`` / ``delete_succeeded_pod`` /
``delete_active_pod`` / ``keep_pod``) is honored for these pods, matching
``KubernetesPodOperator`` behavior.
In ``on_kill()``, pod cleanup now respects ``on_kill_action`` (``delete_pod`` deletes discovered pods;
``keep_pod`` skips pod deletion).
Per-pod cleanup errors are logged but never mask a Job-level failure.
