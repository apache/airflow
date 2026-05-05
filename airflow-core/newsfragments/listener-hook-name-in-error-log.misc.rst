When a task instance listener hook raises an exception, the suppressed-error
log line now identifies which hook raised. Previously every listener failure
across the codebase logged the same generic ``"error calling listener"``
message; the new format ``"error calling listener for hook 'on_task_instance_success'"``
lets plugin authors identify the failing hook directly from the log without
re-reading the stack frame.

Behavior is otherwise unchanged — listener exceptions are still suppressed and
do not affect task execution.
