# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import ast
import os
import re
from glob import glob
from itertools import chain
from typing import Iterable, List, Optional, Set

from docs.exts.docs_build.errors import DocBuildError  # pylint: disable=no-name-in-module

ROOT_PROJECT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir, os.pardir)
)
ROOT_PACKAGE_DIR = os.path.join(ROOT_PROJECT_DIR, "airflow")
DOCS_DIR = os.path.join(ROOT_PROJECT_DIR, "docs")


def find_existing_guide_operator_names(src_dir: str) -> Set[str]:
    """
    Find names of existing operators.
    :return names of existing operators.
    """
    operator_names = set()

    paths = glob(f"{src_dir}/**/*.rst", recursive=True)
    for path in paths:
        with open(path) as f:
            operator_names |= set(re.findall(".. _howto/operator:(.+?):", f.read()))

    return operator_names


def extract_ast_class_def_by_name(ast_tree, class_name):
    """
    Extracts class definition by name
    :param ast_tree: AST tree
    :param class_name: name of the class.
    :return: class node found
    """

    class ClassVisitor(ast.NodeVisitor):
        """Visitor."""

        def __init__(self):
            self.found_class_node = None

        def visit_ClassDef(self, node):  # pylint: disable=invalid-name
            """
            Visit class definition.
            :param node: node.
            :return:
            """
            if node.name == class_name:
                self.found_class_node = node

    visitor = ClassVisitor()
    visitor.visit(ast_tree)

    return visitor.found_class_node


def check_guide_links_in_operator_descriptions() -> List[DocBuildError]:
    """Check if there are links to guides in operator's descriptions."""
    # TODO: We should also check the guides  in the provider documentations.
    #     For now, we are only checking the core documentation.
    #     This is easiest to do after the content has been fully migrated.
    build_errors = []

    def generate_build_error(path, line_no, operator_name):
        return DocBuildError(
            file_path=path,
            line_no=line_no,
            message=(
                f"Link to the guide is missing in operator's description: {operator_name}.\n"
                f"Please add link to the guide to the description in the following form:\n"
                f"\n"
                f".. seealso::\n"
                f"    For more information on how to use this operator, take a look at the guide:\n"
                f"    :ref:`apache-airflow:howto/operator:{operator_name}`\n"
            ),
        )

    # Extract operators for which there are existing .rst guides
    operator_names = find_existing_guide_operator_names(f"{DOCS_DIR}/howto/operator")

    # Extract all potential python modules that can contain operators
    python_module_paths = chain(
        glob(f"{ROOT_PACKAGE_DIR}/operators/*.py"),
        glob(f"{ROOT_PACKAGE_DIR}/sensors/*.py"),
        glob(f"{ROOT_PACKAGE_DIR}/providers/**/operators/*.py", recursive=True),
        glob(f"{ROOT_PACKAGE_DIR}/providers/**/sensors/*.py", recursive=True),
        glob(f"{ROOT_PACKAGE_DIR}/providers/**/transfers/*.py", recursive=True),
    )

    for py_module_path in python_module_paths:
        with open(py_module_path) as f:
            py_content = f.read()

        if "This module is deprecated" in py_content:
            continue

        for existing_operator in operator_names:
            if f"class {existing_operator}" not in py_content:
                continue
            # This is a potential file with necessary class definition.
            # To make sure it's a real Python class definition, we build AST tree
            ast_tree = ast.parse(py_content)
            class_def = extract_ast_class_def_by_name(ast_tree, existing_operator)

            if class_def is None:
                continue

            docstring = ast.get_docstring(class_def)
            if "This class is deprecated." in docstring:
                continue

            if f":ref:`apache-airflow:howto/operator:{existing_operator}`" in ast.get_docstring(
                class_def
            ) or f":ref:`howto/operator:{existing_operator}`" in ast.get_docstring(class_def):
                continue

            build_errors.append(generate_build_error(py_module_path, class_def.lineno, existing_operator))
    return build_errors


def assert_file_not_contains(file_path: str, pattern: str, message: str) -> Optional[DocBuildError]:
    """
    Asserts that file does not contain the pattern. Return message error if it does.
    :param file_path: file
    :param pattern: pattern
    :param message: message to return
    """
    with open(file_path, "rb", 0) as doc_file:
        pattern_compiled = re.compile(pattern)

        for num, line in enumerate(doc_file, 1):
            line_decode = line.decode()
            if re.search(pattern_compiled, line_decode):
                return DocBuildError(file_path=file_path, line_no=num, message=message)
    return None


def filter_file_list_by_pattern(file_paths: Iterable[str], pattern: str) -> List[str]:
    """
    Filters file list to those tha content matches the pattern
    :param file_paths: file paths to check
    :param pattern: pattern to match
    :return: list of files matching the pattern
    """
    output_paths = []
    pattern_compiled = re.compile(pattern)
    for file_path in file_paths:
        with open(file_path, "rb", 0) as text_file:
            text_file_content = text_file.read().decode()
            if re.findall(pattern_compiled, text_file_content):
                output_paths.append(file_path)
    return output_paths


def find_modules(deprecated_only: bool = False) -> Set[str]:
    """
    Finds all modules.
    :param deprecated_only: whether only deprecated modules should be found.
    :return: set of all modules found
    """
    file_paths = glob(f"{ROOT_PACKAGE_DIR}/**/*.py", recursive=True)
    # Exclude __init__.py
    file_paths = [f for f in file_paths if not f.endswith("__init__.py")]
    if deprecated_only:
        file_paths = filter_file_list_by_pattern(file_paths, r"This module is deprecated.")
    # Make path relative
    file_paths = [os.path.relpath(f, ROOT_PROJECT_DIR) for f in file_paths]
    # Convert filename to module
    modules_names = {file_path.rpartition(".")[0].replace("/", ".") for file_path in file_paths}
    return modules_names


def check_exampleinclude_for_example_dags() -> List[DocBuildError]:
    """Checks all exampleincludes for  example dags."""
    all_docs_files = glob(f"${DOCS_DIR}/**/*rst", recursive=True)
    build_errors = []
    for doc_file in all_docs_files:
        build_error = assert_file_not_contains(
            file_path=doc_file,
            pattern=r"literalinclude::.+example_dags",
            message=(
                "literalinclude directive is prohibited for example DAGs. \n"
                "You should use the exampleinclude directive to include example DAGs."
            ),
        )
        if build_error:
            build_errors.append(build_error)
    return build_errors


def check_enforce_code_block() -> List[DocBuildError]:
    """Checks all code:: blocks."""
    all_docs_files = glob(f"{DOCS_DIR}/**/*rst", recursive=True)
    build_errors = []
    for doc_file in all_docs_files:
        build_error = assert_file_not_contains(
            file_path=doc_file,
            pattern=r"^.. code::",
            message=(
                "We recommend using the code-block directive instead of the code directive. "
                "The code-block directive is more feature-full."
            ),
        )
        if build_error:
            build_errors.append(build_error)
    return build_errors
