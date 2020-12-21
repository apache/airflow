:mod:`airflow.utils.helpers`
============================

.. py:module:: airflow.utils.helpers


Module Contents
---------------

.. data:: KEY_REGEX
   

   

.. function:: validate_key(k, max_length=250)
   Validates value used as a key.


.. function:: alchemy_to_dict(obj: Any) -> Optional[Dict]
   Transforms a SQLAlchemy model instance into a dictionary


.. function:: ask_yesno(question)
   Helper to get yes / no answer from user.


.. function:: is_container(obj)
   Test if an object is a container (iterable) but not a string


.. function:: as_tuple(obj)
   If obj is a container, returns obj as a tuple.
   Otherwise, returns a tuple containing obj.


.. data:: T
   

   

.. data:: S
   

   

.. function:: chunks(items: List[T], chunk_size: int) -> Generator[List[T], None, None]
   Yield successive chunks of a given size from a list of items


.. function:: reduce_in_chunks(fn: Callable[[S, List[T]], S], iterable: List[T], initializer: S, chunk_size: int = 0)
   Reduce the given list of items by splitting it into chunks
   of the given size and passing each chunk through the reducer


.. function:: as_flattened_list(iterable: Iterable[Iterable[T]]) -> List[T]
   Return an iterable with one level flattened

   >>> as_flattened_list((('blue', 'red'), ('green', 'yellow', 'pink')))
   ['blue', 'red', 'green', 'yellow', 'pink']


.. function:: parse_template_string(template_string)
   Parses Jinja template string.


.. function:: render_log_filename(ti, try_number, filename_template)
   Given task instance, try_number, filename_template, return the rendered log
   filename

   :param ti: task instance
   :param try_number: try_number of the task
   :param filename_template: filename template, which can be jinja template or
       python string template


.. function:: convert_camel_to_snake(camel_str)
   Converts CamelCase to snake_case.


.. function:: merge_dicts(dict1, dict2)
   Merge two dicts recursively, returning new dict (input dict is not mutated).

   Lists are not concatenated. Items in dict2 overwrite those also found in dict1.


.. function:: partition(pred: Callable, iterable: Iterable)
   Use a predicate to partition entries into false entries and true entries


.. function:: chain(*args, **kwargs)
   This function is deprecated. Please use `airflow.models.baseoperator.chain`.


.. function:: cross_downstream(*args, **kwargs)
   This function is deprecated. Please use `airflow.models.baseoperator.cross_downstream`.


.. function:: build_airflow_url_with_query(query: Dict[str, Any]) -> str
   Build airflow url using base_url and default_view and provided query
   For example:
   'http://0.0.0.0:8000/base/graph?dag_id=my-task&root=&execution_date=2020-10-27T10%3A59%3A25.615587


