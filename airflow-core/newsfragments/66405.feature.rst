AIP-97 (Infrastructure-Aware Task Execution) foundation — adds the
``FailureDetails`` value object and extends the ``on_task_instance_failed``
listener hook to accept it as an optional ``failure_details`` keyword
argument.

Today the listener only sees the worker-side ``error`` exception; failure
causes that originate outside the worker process — pod ``OOMKilled`` /
``PodEvicted`` on Kubernetes, ``WorkerLost`` / ``SoftTimeLimit`` on Celery,
oom-killer ``SIGKILL`` on the local executor, preemption / eviction on
resource-managed clusters — are visible only to the executor and never
reach the listener.

``FailureDetails`` is the executor-agnostic shape every executor populates
when surfacing infrastructure failure causes. The ``executor_kind`` is the
canonical executor name; ``infra_reason`` is the executor's short
categorical token; ``infra_metadata`` carries any structured payload the
executor wants to ship.

.. code-block:: python

    from airflow.listeners import hookimpl
    from airflow.listeners.types import FailureDetails

    class InfraTrackingListener:
        @hookimpl
        def on_task_instance_failed(
            self,
            previous_state,
            task_instance,
            error,
            failure_details: FailureDetails | None = None,
        ):
            if failure_details and failure_details.infra_reason == "OOMKilled":
                ...  # route to capacity-planning alert

This change ships only the type and the hookspec extension. Per-executor
wiring (Kubernetes, Celery, local, etc.) is intentionally deferred to
follow-up PRs so each executor's surfacing PR can iterate against a fixed
contract. pluggy dispatches by parameter name, so existing ``hookimpl``
implementations that don't declare ``failure_details`` keep working
unchanged.
