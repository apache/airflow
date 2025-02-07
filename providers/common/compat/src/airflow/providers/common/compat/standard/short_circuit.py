import logging


def get_tasks_to_skip(log, condition, task, downstream_task_ids, ignore_downstream_trigger_rules: bool):
    """
    Compatibility function for short-circuiting tasks.
    In Airflow v2, it will be utilized by the ShortCircuitOperator.
    In Airflow v3+, it will be utilized by the task_runner.
    """
    log.info("Condition result is %s", condition)

    if condition:
        log.info("Proceeding with downstream tasks...")
        return []

    if not downstream_task_ids:
        log.info("No downstream tasks; nothing to do.")
        return []

    def _get_tasks_to_skip():
        if ignore_downstream_trigger_rules is True:
            tasks = task.get_flat_relatives(upstream=False)
        else:
            tasks = task.get_direct_relatives(upstream=False)
        for t in tasks:
            if not t.is_teardown:
                yield t

    to_skip = list(_get_tasks_to_skip())
    # In Python's native logger - this lets us avoid an intermediate list unless debug logging.
    # In structlog this is already handled.
    if not isinstance(log, logging.Logger) or log.getEffectiveLevel() <= logging.DEBUG:
        log.debug("Downstream task IDs %s", to_skip)

    log.info("Skipping downstream tasks")
    return to_skip
