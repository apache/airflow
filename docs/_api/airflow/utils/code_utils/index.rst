:mod:`airflow.utils.code_utils`
===============================

.. py:module:: airflow.utils.code_utils


Module Contents
---------------

.. function:: get_python_source(x: Any) -> Optional[str]
   Helper function to get Python source (or not), preventing exceptions


.. function:: prepare_code_snippet(file_path: str, line_no: int, context_lines_count: int = 5) -> str
   Prepare code snippet with line numbers and  a specific line marked.

   :param file_path: File nam
   :param line_no: Line number
   :param context_lines_count: The number of lines that will be cut before and after.
   :return: str


.. function:: get_terminal_formatter(**opts)
   Returns the best formatter available in the current terminal.


