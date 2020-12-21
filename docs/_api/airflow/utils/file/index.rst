:mod:`airflow.utils.file`
=========================

.. py:module:: airflow.utils.file


Module Contents
---------------

.. data:: log
   

   

.. function:: TemporaryDirectory(*args, **kwargs)
   This function is deprecated. Please use `tempfile.TemporaryDirectory`


.. function:: mkdirs(path, mode)
   Creates the directory specified by path, creating intermediate directories
   as necessary. If directory already exists, this is a no-op.

   :param path: The directory to create
   :type path: str
   :param mode: The mode to give to the directory e.g. 0o755, ignores umask
   :type mode: int


.. data:: ZIP_REGEX
   

   

.. function:: correct_maybe_zipped(fileloc)
   If the path contains a folder with a .zip suffix, then
   the folder is treated as a zip archive and path to zip is returned.


.. function:: open_maybe_zipped(fileloc, mode='r')
   Opens the given file. If the path contains a folder with a .zip suffix, then
   the folder is treated as a zip archive, opening the file inside the archive.

   :return: a file object, as in `open`, or as in `ZipFile.open`.


.. function:: find_path_from_directory(base_dir_path: str, ignore_file_name: str) -> Generator[str, None, None]
   Search the file and return the path of the file that should not be ignored.
   :param base_dir_path: the base path to be searched for.
   :param ignore_file_name: the file name in which specifies a regular expression pattern is written.

   :return : file path not to be ignored.


.. function:: list_py_file_paths(directory: str, safe_mode: bool = conf.getboolean('core', 'DAG_DISCOVERY_SAFE_MODE', fallback=True), include_examples: Optional[bool] = None, include_smart_sensor: Optional[bool] = conf.getboolean('smart_sensor', 'use_smart_sensor'))
   Traverse a directory and look for Python files.

   :param directory: the directory to traverse
   :type directory: unicode
   :param safe_mode: whether to use a heuristic to determine whether a file
       contains Airflow DAG definitions. If not provided, use the
       core.DAG_DISCOVERY_SAFE_MODE configuration setting. If not set, default
       to safe.
   :type safe_mode: bool
   :param include_examples: include example DAGs
   :type include_examples: bool
   :param include_smart_sensor: include smart sensor native control DAGs
   :type include_examples: bool
   :return: a list of paths to Python files in the specified directory
   :rtype: list[unicode]


.. function:: find_dag_file_paths(directory: str, file_paths: list, safe_mode: bool)
   Finds file paths of all DAG files.


.. data:: COMMENT_PATTERN
   

   

.. function:: might_contain_dag(file_path: str, safe_mode: bool, zip_file: Optional[zipfile.ZipFile] = None)
   Heuristic that guesses whether a Python file contains an Airflow DAG definition.

   :param file_path: Path to the file to be checked.
   :param safe_mode: Is safe mode active?. If no, this function always returns True.
   :param zip_file: if passed, checks the archive. Otherwise, check local filesystem.
   :return: True, if file might contain DAGS.


