import json
import logging
import os
from typing import Any, Dict, Optional

from jinja2 import Environment, Template, meta
from pandas import DataFrame

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


def read_json_file(file_path: str) -> Any:
    """
    Loads and returns the contents of the json file.

    :param file_path: path to the file to open.
    :type file_path: str

    :return: contents of the json file.
    :rtype: Any

    :raises: json.decoder.JSONDecodeError if the data being deserialized is not a valid json
        document.
    """

    try:
        with open(file_path) as file:
            file_contents = json.load(file)
    except json.decoder.JSONDecodeError as exception:
        log.error("Error occurred when trying to read file: %s", file_path)
        raise exception

    return file_contents


def read_templated_json_file(
    file_path: str,
    jinja_variables_dict: Optional[Dict[str, str]] = None,
    fill_with_nulls: bool = False,
) -> Any:
    """
    Loads a file containing jinja template, renders it with variables
    provided in jinja_variables_dict and returns the resulting json.

    :param file_path: path to the file to load.
    :type file_path: str
    :param jinja_variables_dict: a dictionary with values for jinja variables.
        Must contain all variables present in the template.
    :type jinja_variables_dict: Dict[str, str]
    :param fill_with_nulls: set to True if you want the file to be rendered by substituting
        all variables with nulls.
    :type fill_with_nulls: bool

    :return: filled contents of the templated file.
    :rtype: Any

    :raises: ValueError jinja_variables_dict does not contain all variables present in the template
        and fill_with_nulls was not set.
    """

    jinja_variables_dict = jinja_variables_dict or {}

    with open(file_path) as file:
        file_contents = "".join(file.readlines())

    env = Environment()
    parsed_contents = env.parse(file_contents)
    template_variables = meta.find_undeclared_variables(parsed_contents)

    if fill_with_nulls:
        jinja_variables_dict = {variable: "null" for variable in template_variables}
    # if jinja_variables_dict does not contain even one of the variables present in template, then
    # throw an exception
    elif template_variables - set(jinja_variables_dict):
        raise ValueError(
            f"Provided jinja variables dict does not contain all variables present in the "
            f"template. Missing variables: {template_variables - set(jinja_variables_dict)}."
        )

    rendered_template = json.loads(
        Template(file_contents).render(**jinja_variables_dict)
    )

    return rendered_template


def save_output_file(
    results_df: DataFrame, output_path: str, default_file_name: Optional[str] = None
) -> None:
    """
    Saves results dataframe as a file in CSV format under specified path.

    :param results_df: pandas Dataframe containing information about tested environment
        configuration and performance metrics.
    :type results_df: DataFrame
    :param output_path: path to the output file. If the output_path is a directory, then
        file will be saved using default_file_name as its name.
    :type output_path: str
    :param default_file_name: default name for the output file, used only if output_path points to
        a directory.
    :type default_file_name: str

    :raises: ValueError if output_path points to a directory and default_file_name was not provided.
    """

    check_output_path(output_path)

    if os.path.isdir(output_path):
        if default_file_name is None:
            raise ValueError(
                f"Provided output_path {output_path} is a directory "
                f" and default_file_name was not provided."
            )
        # if default_file_name does not contain extension, then add it
        if not os.path.splitext(default_file_name)[-1]:
            default_file_name = f"{default_file_name}.csv"
        output_path = os.path.join(output_path, default_file_name)

    log.info("Saving results dataframe under path: %s", output_path)
    results_df.to_csv(output_path, index=False)


def check_output_path(output_path: str) -> None:
    """
    Checks if provided path is valid in terms of saving results file
    and raises exception if it is not.

    :param output_path: path to a directory or a file under which the results should be saved.
    :type output_path: str

    :raises: ValueError if output_path contains a path to non-existing directory.
    """

    if not os.path.exists(output_path) and not os.path.isdir(
        os.path.dirname(output_path)
    ):
        raise ValueError(
            f"Output path '{output_path}' contains a path to non-existing directory."
        )
