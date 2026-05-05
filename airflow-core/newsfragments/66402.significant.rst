AIP-96 (Resumable Operators) foundation — adds the ``CHECKPOINTED`` task
instance state and the ``AirflowTaskCheckpointed`` exception so an operator
can signal "I have reached a stable checkpoint and intend to pause":

.. code-block:: python

    from airflow.sdk.exceptions import AirflowTaskCheckpointed

    class ResumablePythonOperator(PythonOperator):
        def execute(self, context):
            for step in range(self.total_steps):
                if self.should_pause():
                    raise AirflowTaskCheckpointed(checkpoint_data={"step": step})
                self.do_work(step)

The worker catches the exception and reports ``CHECKPOINTED`` state.
``CHECKPOINTED`` is an intermediate state and is included in
``State.unfinished``.

This change intentionally ships only the vocabulary plus the
worker-side state transition. ``checkpoint_data`` persistence,
scheduler auto-resume semantics, the listener hook, and downstream
trigger-rule integration are deferred to follow-up PRs so the API
shape can be discussed against a real artifact before committing to a
single resumption policy.
