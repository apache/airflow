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
from __future__ import annotations

import logging

from airflow.providers.openlineage.extractors.base import BaseExtractor, OperatorLineage
from openlineage.client.run import Dataset

log = logging.getLogger(__name__)


def generate_s3_dataset(path) -> Dataset:
    return Dataset(namespace=f"s3://{path.replace('s3://', '').split('/')[0]}", name=path, facets={})


class SageMakerProcessingExtractor(BaseExtractor):
    """SageMakerProcessingOperator extractor"""

    @classmethod
    def get_operator_classnames(cls) -> list[str]:
        return ["SageMakerProcessingOperator", "SageMakerProcessingOperatorAsync"]

    def extract(self) -> OperatorLineage | None:
        pass

    def extract_on_complete(self, task_instance) -> OperatorLineage | None:

        xcom_values = task_instance.xcom_pull(task_ids=task_instance.task_id)

        inputs = []
        outputs = []

        try:
            inputs, outputs = self._get_s3_datasets(
                processing_inputs=xcom_values["Processing"]["ProcessingInputs"],
                processing_outputs=xcom_values["Processing"]["ProcessingOutputConfig"]["Outputs"],
            )
        except KeyError:
            log.exception("Could not find input/output information in Xcom.")

        return OperatorLineage(
            inputs=inputs,
            outputs=outputs,
        )

    @staticmethod
    def _get_s3_datasets(processing_inputs, processing_outputs):

        inputs = []
        outputs = []

        try:
            for processing_input in processing_inputs:
                inputs.append(generate_s3_dataset(processing_input["S3Input"]["S3Uri"]))
        except Exception:
            log.exception("Cannot find S3 input details", exc_info=True)

        try:
            for processing_output in processing_outputs:
                outputs.append(generate_s3_dataset(processing_output["S3Output"]["S3Uri"]))
        except Exception:
            log.exception("Cannot find S3 output details.", exc_info=True)

        return inputs, outputs


class SageMakerTransformExtractor(BaseExtractor):
    """SageMakerTransformOperator extractor"""

    @classmethod
    def get_operator_classnames(cls) -> list[str]:
        return ["SageMakerTransformOperator", "SageMakerTransformOperatorAsync"]

    def extract(self) -> OperatorLineage | None:
        pass

    def extract_on_complete(self, task_instance) -> OperatorLineage | None:
        log.debug("extract_on_complete(%s)", task_instance)

        xcom_values = task_instance.xcom_pull(task_ids=task_instance.task_id)

        model_package_arn = None
        transform_input = None
        transform_output = None

        try:
            model_package_arn = xcom_values["Model"]["PrimaryContainer"]["ModelPackageName"]
        except KeyError:
            log.error("Cannot find Model Package Name in Xcom values.", exc_info=True)

        try:
            transform = xcom_values["Transform"]
            transform_input = transform["TransformInput"]["DataSource"]["S3DataSource"]["S3Uri"]
            transform_output = transform["TransformOutput"]["S3OutputPath"]
        except KeyError:
            log.error("Cannot find some required input/output details in XCom.", exc_info=True)

        inputs = []

        if transform_input is not None:
            inputs.append(generate_s3_dataset(transform_input))

        if model_package_arn is not None:
            model_data_urls = self._get_model_data_urls(model_package_arn)
            for model_data_url in model_data_urls:
                inputs.append(generate_s3_dataset(model_data_url))

        output = []
        if transform_output is not None:
            output.append(generate_s3_dataset(transform_output))

        return OperatorLineage(inputs=inputs, outputs=output)

    def _get_model_data_urls(self, model_package_arn):
        model_data_urls = []
        try:
            model_containers = self.operator.hook.get_conn().describe_model_package(
                ModelPackageName=model_package_arn
            )["InferenceSpecification"]["Containers"]

            for container in model_containers:
                model_data_urls.append(container["ModelDataUrl"])
        except Exception:
            log.exception("Cannot retrieve model details.", exc_info=True)

        return model_data_urls


class SageMakerTrainingExtractor(BaseExtractor):
    """SageMakerTrainingOperator extractor"""

    @classmethod
    def get_operator_classnames(cls) -> list[str]:
        return ["SageMakerTrainingOperator", "SageMakerTrainingOperatorAsync"]

    def extract(self) -> OperatorLineage | None:
        pass

    def extract_on_complete(self, task_instance) -> OperatorLineage | None:
        log.debug("extract_on_complete(%s)", str(task_instance))

        xcom_values = task_instance.xcom_pull(task_ids=task_instance.task_id)

        inputs = []
        output = []

        try:
            for input_data in xcom_values["Training"]["InputDataConfig"]:
                inputs.append(generate_s3_dataset(input_data["DataSource"]["S3DataSource"]["S3Uri"]))
        except KeyError:
            log.exception("Issues extracting inputs.")

        try:
            output.append(generate_s3_dataset(xcom_values["Training"]["ModelArtifacts"]["S3ModelArtifacts"]))
        except KeyError:
            log.exception("Issues extracting inputs.")

        return OperatorLineage(inputs=inputs, outputs=output)
