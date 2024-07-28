import json
import os
from pprint import pformat
from typing import Dict, List, Optional, Any

from marshmallow import Schema, fields, post_load

from performance_test import PerformanceTest

COMMON_VALUES = [
    "attempts",
    "delete_if_exists",
    "delete_upon_finish",
    "jinja_variables",
    "output_path",
    "elastic_dag_config_file_path",
    "results_columns",
]


class CommonSchema(Schema):
    attempts = fields.Integer()
    delete_if_exists = fields.Boolean()
    delete_upon_finish = fields.Boolean()
    jinja_variables = fields.Dict()
    output_path = fields.Str()
    elastic_dag_config_file_path = fields.Str()
    results_columns = fields.Dict()

    @post_load
    def make(self, data, **kwargs):  # pylint: disable=unused-argument,no-self-use
        return DefaultValues(**data)


class TestSpecSchema(CommonSchema):
    instance_specification_file_path = fields.Str(required=True)
    randomize_environment_name = fields.Boolean()

    @post_load
    def make(self, data, **kwargs):  # pylint: disable=unused-argument,no-self-use
        return TestSpec(**data)


class StudySchema(CommonSchema):
    logs_bucket = fields.Str()
    logs_project_id = fields.Str()
    defaults = fields.Nested(CommonSchema)
    study_components = fields.List(fields.Nested(TestSpecSchema), required=True)

    @post_load
    def make(self, data, **kwargs):  # pylint: disable=unused-argument,no-self-use
        return Study(**data)


class DefaultValues:
    def __init__(
        self,
        *,
        output_path: Optional[str] = None,
        jinja_variables: Optional[Dict] = None,
        delete_if_exists: Optional[bool] = None,
        delete_upon_finish: Optional[bool] = None,
        attempts: Optional[int] = None,
        elastic_dag_config_file_path: Optional[str] = None,
        results_columns: Optional[Dict] = None,
    ):
        self.output_path = output_path
        self.jinja_variables = jinja_variables
        self.delete_if_exists = delete_if_exists
        self.delete_upon_finish = delete_upon_finish
        self.attempts = attempts
        self.elastic_dag_config_file_path = elastic_dag_config_file_path
        self.results_columns = results_columns

    def set_defaults(self, source: Any) -> None:
        for attr in COMMON_VALUES:
            if getattr(self, attr) is None:
                setattr(self, attr, getattr(source, attr))

    def validate(self):
        if not os.path.isfile(self.elastic_dag_config_file_path):
            raise ValueError(
                f"Elastic dag configuration file " f"'{self.elastic_dag_config_file_path}' does not exist."
            )

        if self.output_path:
            raise KeyError(f"Please provide path to store results " f"for study component.")


class TestSpec(DefaultValues):
    def __init__(
        self,
        *,
        instance_specification_file_path: str,
        randomize_environment_name: Optional[bool] = None,
        ci_build_id: Optional[str] = None,
        script_user: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.instance_specification_file_path = instance_specification_file_path
        self.randomize_environment_name = (randomize_environment_name,)
        self.ci_build_id = ci_build_id
        self.script_user = script_user

        self.environment_type = PerformanceTest.get_instance_type(self.instance_specification_file_path)

    def __str__(self):
        return f"<TestSpec>: \n{pformat(TestSpecSchema().dump(self), indent=2, compact=True)}"

    def validate(self):
        super().validate()

        if not os.path.isfile(self.instance_specification_file_path):
            raise ValueError(
                f"Environment specification file "
                f"'{self.instance_specification_file_path}' "
                f"does not exist."
            )


class Study:
    def __init__(
        self,
        *,
        study_components: List[TestSpec],
        defaults: Optional[DefaultValues] = None,
        logs_bucket: Optional[str] = None,
        logs_project_id: Optional[str] = None,
    ):
        self.study_components = study_components
        self.logs_bucket = logs_bucket
        self.logs_project_id = logs_project_id
        self.defaults = defaults

    def __str__(self):
        return f"<Study>: \n{pformat(StudySchema().dump(self), indent=2, compact=True)}"

    @property
    def output_paths(self):
        return [c.output_path for c in self.study_components if c.output_path]

    def set_script_user(self, script_user: str) -> None:
        for spec in self.study_components:
            spec.script_user = script_user

    def set_ci_build_id(self, ci_build_id: str) -> None:
        for spec in self.study_components:
            spec.ci_build_id = ci_build_id

    @property
    def study_components_multiplied(self):
        return self.study_components

    @classmethod
    def read_from_json_file(cls, file_path: str) -> "Study":
        with open(file_path, "r") as f:
            study_content = json.load(f)

        study_obj: Study = StudySchema().load(study_content)
        for spec in study_obj.study_components:
            spec.set_defaults(source=study_obj.defaults)

        return study_obj


if __name__ == "__main__":
    study = Study.read_from_json_file("studies/composer_study_example.json")
    print(study)
