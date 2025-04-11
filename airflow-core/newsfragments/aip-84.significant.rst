Modernize and improve the REST API.

As part of this change the following breaking changes have occurred:

- The API returns 422 status code instead of 400 for validation errors.

  For instance when the request payload, path params, or query params are invalid.

- When listing a resource for instance on GET ``/dags``, ``fields`` parameter is not supported anymore to obtain a partial response.

  The full objects will be returned by the endpoint. This feature might be added back in upcoming 3.x versions.

- Passing list in query parameters switched from ``form, non exploded`` to ``form, exploded``
  i.e before ``?my_list=item1,item2`` now ``?my_list=item1&my_list=item2``

- ``execution_date`` was deprecated and has been removed. Any payload or parameter mentioning this field has been removed.

- Datetime format are RFC3339-compliant in FastAPI, more permissive than ISO8601,
  meaning that the API returns zulu datetime for responses, more info here https://github.com/fastapi/fastapi/discussions/7693#discussioncomment-5143311.
  Both ``Z`` and ``00+xx`` are supported for payload and params.

  This is due FastAPI and pydantic v2 default behavior.

- PATCH on ``DagRun`` and ``TaskInstance`` are more generic and allow in addition to update the resource state and the note content.

  Therefore the two legacy dedicated endpoints to update a ``DagRun`` note and ``TaskInstance`` note have been removed.

  Same for the set task instance state, it is now handled by the broader PATCH on task instances.

- ``assets/queuedEvent`` endpoints have moved to ``assets/queuedEvents`` for consistency.

- dag_parsing endpoint now returns a 409 when the DagPriorityParsingRequest already exists. It was returning 201 before.

- ``clearTaskInstances`` endpoint default value for ``reset_dag_runs`` field has been updated from ``False`` to ``True``.

- Pool name can't be modified in the PATCH pool endpoint anymore. Pool name shouldn't be updated via pool PATCH API call.

- Logical date is now a nullable. In addition it is a nullable required payload field for Triggering a DagRun endpoint.


* Types of change

  * [ ] Dag changes
  * [ ] Config changes
  * [x] API changes
  * [ ] CLI changes
  * [ ] Behaviour changes
  * [ ] Plugin changes
  * [ ] Dependency changes
  * [ ] Code interface changes
