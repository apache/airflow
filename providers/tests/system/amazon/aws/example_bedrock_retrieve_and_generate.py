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

import json
import logging
import os.path
import tempfile
from datetime import datetime
from time import sleep
from urllib.request import urlretrieve

import boto3
from botocore.exceptions import ClientError
from opensearchpy import (
    AuthorizationException,
    AWSV4SignerAuth,
    OpenSearch,
    RequestsHttpConnection,
)

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.bedrock import BedrockAgentHook
from airflow.providers.amazon.aws.hooks.opensearch_serverless import (
    OpenSearchServerlessHook,
)
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.sts import StsHook
from airflow.providers.amazon.aws.operators.bedrock import (
    BedrockCreateDataSourceOperator,
    BedrockCreateKnowledgeBaseOperator,
    BedrockIngestDataOperator,
    BedrockInvokeModelOperator,
    BedrockRaGOperator,
    BedrockRetrieveOperator,
)
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.amazon.aws.sensors.bedrock import (
    BedrockIngestionJobSensor,
    BedrockKnowledgeBaseActiveSensor,
)
from airflow.providers.amazon.aws.sensors.opensearch_serverless import (
    OpenSearchServerlessCollectionActiveSensor,
)
from airflow.providers.amazon.aws.utils import get_botocore_version
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.amazon.aws.utils import SystemTestContextBuilder

#######################################################################
# NOTE:
#   Access to the following foundation model must be requested via
#   the Amazon Bedrock console and may take up to 24 hours to apply:
#######################################################################

CLAUDE_MODEL_ID = "anthropic.claude-v2"
TITAN_MODEL_ID = "amazon.titan-embed-text-v1"

# Externally fetched variables:
ROLE_ARN_KEY = "ROLE_ARN"
sys_test_context_task = SystemTestContextBuilder().add_variable(ROLE_ARN_KEY).build()

DAG_ID = "example_bedrock_knowledge_base"

log = logging.getLogger(__name__)


@task_group
def external_sources_rag_group():
    """External Sources were added in boto 1.34.90, skip this operator if the version is below that."""

    # [START howto_operator_bedrock_external_sources_rag]
    external_sources_rag = BedrockRaGOperator(
        task_id="external_sources_rag",
        input="Who was the CEO of Amazon in 2022?",
        source_type="EXTERNAL_SOURCES",
        model_arn=f"arn:aws:bedrock:{region_name}::foundation-model/anthropic.claude-3-sonnet-20240229-v1:0",
        sources=[
            {
                "sourceType": "S3",
                "s3Location": {
                    "uri": f"s3://{bucket_name}/AMZN-2022-Shareholder-Letter.pdf"
                },
            }
        ],
    )
    # [END howto_operator_bedrock_external_sources_rag]

    @task.branch
    def run_or_skip():
        log.info("Found botocore version %s.", botocore_version := get_botocore_version())
        return (
            end_workflow.task_id
            if botocore_version < (1, 34, 90)
            else external_sources_rag.task_id
        )

    run_or_skip = run_or_skip()
    end_workflow = EmptyOperator(
        task_id="end_workflow", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    chain(
        run_or_skip, Label("Boto version does not support External Sources"), end_workflow
    )
    chain(run_or_skip, external_sources_rag, end_workflow)


@task
def create_opensearch_policies(
    bedrock_role_arn: str, collection_name: str, policy_name_suffix: str
) -> None:
    """
    Create security, network and data access policies within Amazon OpenSearch Serverless.

    :param bedrock_role_arn: Arn of the Bedrock Knowledge Base Execution Role.
    :param collection_name: Name of the OpenSearch collection to apply the policies to.
    :param policy_name_suffix: EnvironmentID or other unique suffix to append to the policy name.
    """

    encryption_policy_name = f"{naming_prefix}sp-{policy_name_suffix}"
    network_policy_name = f"{naming_prefix}np-{policy_name_suffix}"
    access_policy_name = f"{naming_prefix}ap-{policy_name_suffix}"

    def _create_security_policy(name, policy_type, policy):
        try:
            aoss_client.create_security_policy(
                name=name, policy=json.dumps(policy), type=policy_type
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConflictException":
                log.info("OpenSearch security policy %s already exists.", name)
            raise

    def _create_access_policy(name, policy_type, policy):
        try:
            aoss_client.create_access_policy(
                name=name, policy=json.dumps(policy), type=policy_type
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConflictException":
                log.info("OpenSearch data access policy %s already exists.", name)
            raise

    _create_security_policy(
        name=encryption_policy_name,
        policy_type="encryption",
        policy={
            "Rules": [
                {
                    "Resource": [f"collection/{collection_name}"],
                    "ResourceType": "collection",
                }
            ],
            "AWSOwnedKey": True,
        },
    )

    _create_security_policy(
        name=network_policy_name,
        policy_type="network",
        policy=[
            {
                "Rules": [
                    {
                        "Resource": [f"collection/{collection_name}"],
                        "ResourceType": "collection",
                    }
                ],
                "AllowFromPublic": True,
            }
        ],
    )

    _create_access_policy(
        name=access_policy_name,
        policy_type="data",
        policy=[
            {
                "Rules": [
                    {
                        "Resource": [f"collection/{collection_name}"],
                        "Permission": [
                            "aoss:CreateCollectionItems",
                            "aoss:DeleteCollectionItems",
                            "aoss:UpdateCollectionItems",
                            "aoss:DescribeCollectionItems",
                        ],
                        "ResourceType": "collection",
                    },
                    {
                        "Resource": [f"index/{collection_name}/*"],
                        "Permission": [
                            "aoss:CreateIndex",
                            "aoss:DeleteIndex",
                            "aoss:UpdateIndex",
                            "aoss:DescribeIndex",
                            "aoss:ReadDocument",
                            "aoss:WriteDocument",
                        ],
                        "ResourceType": "index",
                    },
                ],
                "Principal": [
                    (StsHook().conn.get_caller_identity()["Arn"]),
                    bedrock_role_arn,
                ],
            }
        ],
    )


@task
def create_collection(collection_name: str):
    """
    Call the Amazon OpenSearch Serverless API and create a collection with the provided name.

    :param collection_name: The name of the Collection to create.
    """
    log.info("\nCreating collection: %s.", collection_name)
    return aoss_client.create_collection(name=collection_name, type="VECTORSEARCH")[
        "createCollectionDetail"
    ]["id"]


@task
def create_vector_index(index_name: str, collection_id: str, region: str):
    """
    Use the OpenSearchPy client to create the vector index for the Amazon Open Search Serverless Collection.

    :param index_name: The vector index name to create.
    :param collection_id: ID of the collection to be indexed.
    :param region: Name of the AWS region the collection resides in.
    """
    # Build the OpenSearch client
    oss_client = OpenSearch(
        hosts=[{"host": f"{collection_id}.{region}.aoss.amazonaws.com", "port": 443}],
        http_auth=AWSV4SignerAuth(boto3.Session().get_credentials(), region, "aoss"),
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        timeout=300,
    )
    index_config = {
        "settings": {
            "index.knn": "true",
            "number_of_shards": 1,
            "knn.algo_param.ef_search": 512,
            "number_of_replicas": 0,
        },
        "mappings": {
            "properties": {
                "vector": {
                    "type": "knn_vector",
                    "dimension": 1536,
                    "method": {"name": "hnsw", "engine": "faiss", "space_type": "l2"},
                },
                "text": {"type": "text"},
                "text-metadata": {"type": "text"},
            }
        },
    }

    retries = 35
    while retries > 0:
        try:
            response = oss_client.indices.create(
                index=index_name, body=json.dumps(index_config)
            )
            log.info("Creating index: %s.", response)
            break
        except AuthorizationException as e:
            # Index creation can take up to a minute and there is no (apparent?) way to check the current state.
            log.info(
                "Access denied; policy permissions have likely not yet propagated, %s tries remaining.",
                retries,
            )
            log.debug(e)
            retries -= 1
            if retries:
                sleep(2)
            else:
                raise


@task
def copy_data_to_s3(bucket: str):
    """
    Download some sample data and upload it to 3S.

    :param bucket: Name of the Amazon S3 bucket to send the data to.
    """

    # Monkey patch the list of names available for NamedTempFile so we can pick the names of the downloaded files.
    backup_get_candidate_names = tempfile._get_candidate_names  # type: ignore[attr-defined]
    destinations = iter(
        [
            "AMZN-2022-Shareholder-Letter.pdf",
            "AMZN-2021-Shareholder-Letter.pdf",
            "AMZN-2020-Shareholder-Letter.pdf",
            "AMZN-2019-Shareholder-Letter.pdf",
        ]
    )
    tempfile._get_candidate_names = lambda: destinations  # type: ignore[attr-defined]

    # Download the sample data files, save them as named temp files using the names above, and upload to S3.
    sources = [
        "https://s2.q4cdn.com/299287126/files/doc_financials/2023/ar/2022-Shareholder-Letter.pdf",
        "https://s2.q4cdn.com/299287126/files/doc_financials/2022/ar/2021-Shareholder-Letter.pdf",
        "https://s2.q4cdn.com/299287126/files/doc_financials/2021/ar/Amazon-2020-Shareholder-Letter-and-1997-Shareholder-Letter.pdf",
        "https://s2.q4cdn.com/299287126/files/doc_financials/2020/ar/2019-Shareholder-Letter.pdf",
    ]

    for source in sources:
        with tempfile.NamedTemporaryFile(mode="w", prefix="") as data_file:
            urlretrieve(source, data_file.name)
            S3Hook().conn.upload_file(
                Filename=data_file.name,
                Bucket=bucket,
                Key=os.path.basename(data_file.name),
            )

    # Revert the monkey patch.
    tempfile._get_candidate_names = backup_get_candidate_names  # type: ignore[attr-defined]
    # Verify the path reversion worked.
    with tempfile.NamedTemporaryFile(mode="w", prefix=""):
        # If the reversion above did not apply correctly, this will fail with
        # a StopIteration error because the iterator will run out of names.
        ...


@task
def get_collection_arn(collection_id: str):
    """
    Return a collection ARN for a given collection ID.

    :param collection_id: ID of the collection to be indexed.
    """
    return next(
        colxn["arn"]
        for colxn in aoss_client.list_collections()["collectionSummaries"]
        if colxn["id"] == collection_id
    )


# [START howto_operator_bedrock_delete_data_source]
@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_data_source(knowledge_base_id: str, data_source_id: str):
    """
    Delete the Amazon Bedrock data source created earlier.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto_operator:BedrockDeleteDataSource`

    :param knowledge_base_id: The unique identifier of the knowledge base which the data source is attached to.
    :param data_source_id: The unique identifier of the data source to delete.
    """
    log.info(
        "Deleting data source %s from Knowledge Base %s.",
        data_source_id,
        knowledge_base_id,
    )
    bedrock_agent_client.delete_data_source(
        dataSourceId=data_source_id, knowledgeBaseId=knowledge_base_id
    )


# [END howto_operator_bedrock_delete_data_source]


# [START howto_operator_bedrock_delete_knowledge_base]
@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_knowledge_base(knowledge_base_id: str):
    """
    Delete the Amazon Bedrock knowledge base created earlier.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/operator:BedrockDeleteKnowledgeBase`

    :param knowledge_base_id: The unique identifier of the knowledge base to delete.
    """
    log.info("Deleting Knowledge Base %s.", knowledge_base_id)
    bedrock_agent_client.delete_knowledge_base(knowledgeBaseId=knowledge_base_id)


# [END howto_operator_bedrock_delete_knowledge_base]


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_vector_index(index_name: str, collection_id: str):
    """
    Delete the vector index created earlier.

    :param index_name: The name of the vector index to delete.
    :param collection_id: ID of the collection to be indexed.
    """
    host = f"{collection_id}.{region_name}.aoss.amazonaws.com"
    credentials = boto3.Session().get_credentials()
    awsauth = AWSV4SignerAuth(credentials, region_name, "aoss")

    # Build the OpenSearch client
    oss_client = OpenSearch(
        hosts=[{"host": host, "port": 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        timeout=300,
    )
    oss_client.indices.delete(index=index_name)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_collection(collection_id: str):
    """
    Delete the OpenSearch collection created earlier.

    :param collection_id: ID of the collection to be indexed.
    """
    log.info("Deleting collection %s.", collection_id)
    aoss_client.delete_collection(id=collection_id)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_opensearch_policies(collection_name: str):
    """
    Delete the security, network and data access policies created earlier.

    :param collection_name: All policies in the given collection name will be deleted.
    """

    access_policies = aoss_client.list_access_policies(
        type="data", resource=[f"collection/{collection_name}"]
    )["accessPolicySummaries"]
    log.info("Found access policies for %s: %s", collection_name, access_policies)
    if not access_policies:
        raise Exception("No access policies found?")
    for policy in access_policies:
        log.info("Deleting access policy for %s: %s", collection_name, policy["name"])
        aoss_client.delete_access_policy(name=policy["name"], type="data")

    for policy_type in ["encryption", "network"]:
        policies = aoss_client.list_security_policies(
            type=policy_type, resource=[f"collection/{collection_name}"]
        )["securityPolicySummaries"]
        if not policies:
            raise Exception("No security policies found?")
        log.info(
            "Found %s security policies for %s: %s",
            policy_type,
            collection_name,
            policies,
        )
        for policy in policies:
            log.info(
                "Deleting %s security policy for %s: %s",
                policy_type,
                collection_name,
                policy["name"],
            )
            aoss_client.delete_security_policy(name=policy["name"], type=policy_type)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context["ENV_ID"]

    aoss_client = OpenSearchServerlessHook(aws_conn_id=None).conn
    bedrock_agent_client = BedrockAgentHook(aws_conn_id=None).conn

    region_name = boto3.session.Session().region_name

    naming_prefix = "bedrock-kb-"
    bucket_name = f"{naming_prefix}{env_id}"
    index_name = f"{naming_prefix}index-{env_id}"
    knowledge_base_name = f"{naming_prefix}{env_id}"
    vector_store_name = f"{naming_prefix}{env_id}"
    data_source_name = f"{naming_prefix}ds-{env_id}"

    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket", bucket_name=bucket_name
    )

    opensearch_policies = create_opensearch_policies(
        bedrock_role_arn=test_context[ROLE_ARN_KEY],
        collection_name=vector_store_name,
        policy_name_suffix=env_id,
    )

    collection = create_collection(collection_name=vector_store_name)

    # [START howto_sensor_opensearch_collection_active]
    await_collection = OpenSearchServerlessCollectionActiveSensor(
        task_id="await_collection",
        collection_name=vector_store_name,
    )
    # [END howto_sensor_opensearch_collection_active]

    PROMPT = "What color is an orange?"
    # [START howto_operator_invoke_claude_model]
    invoke_claude_completions = BedrockInvokeModelOperator(
        task_id="invoke_claude_completions",
        model_id=CLAUDE_MODEL_ID,
        input_data={
            "max_tokens_to_sample": 4000,
            "prompt": f"\n\nHuman: {PROMPT}\n\nAssistant:",
        },
    )
    # [END howto_operator_invoke_claude_model]

    # [START howto_operator_bedrock_create_knowledge_base]
    create_knowledge_base = BedrockCreateKnowledgeBaseOperator(
        task_id="create_knowledge_base",
        name=knowledge_base_name,
        embedding_model_arn=f"arn:aws:bedrock:{region_name}::foundation-model/{TITAN_MODEL_ID}",
        role_arn=test_context[ROLE_ARN_KEY],
        storage_config={
            "type": "OPENSEARCH_SERVERLESS",
            "opensearchServerlessConfiguration": {
                "collectionArn": get_collection_arn(collection),
                "vectorIndexName": index_name,
                "fieldMapping": {
                    "vectorField": "vector",
                    "textField": "text",
                    "metadataField": "text-metadata",
                },
            },
        },
    )
    # [END howto_operator_bedrock_create_knowledge_base]
    create_knowledge_base.wait_for_completion = False

    # [START howto_sensor_bedrock_knowledge_base_active]
    await_knowledge_base = BedrockKnowledgeBaseActiveSensor(
        task_id="await_knowledge_base", knowledge_base_id=create_knowledge_base.output
    )
    # [END howto_sensor_bedrock_knowledge_base_active]

    # [START howto_operator_bedrock_create_data_source]
    create_data_source = BedrockCreateDataSourceOperator(
        task_id="create_data_source",
        knowledge_base_id=create_knowledge_base.output,
        name=data_source_name,
        bucket_name=bucket_name,
    )
    # [END howto_operator_bedrock_create_data_source]

    # In this demo, delete_data_source and delete_cluster are both trying to delete
    # the data from the S3 bucket and occasionally hitting a conflict.  This ensures that
    # delete_data_source doesn't attempt to delete the files, leaving that duty to delete_bucket.
    create_data_source.create_data_source_kwargs["dataDeletionPolicy"] = "RETAIN"

    # [START howto_operator_bedrock_ingest_data]
    ingest_data = BedrockIngestDataOperator(
        task_id="ingest_data",
        knowledge_base_id=create_knowledge_base.output,
        data_source_id=create_data_source.output,
    )
    # [END howto_operator_bedrock_ingest_data]
    ingest_data.wait_for_completion = False

    # [START howto_sensor_bedrock_ingest_data]
    await_ingest = BedrockIngestionJobSensor(
        task_id="await_ingest",
        knowledge_base_id=create_knowledge_base.output,
        data_source_id=create_data_source.output,
        ingestion_job_id=ingest_data.output,
    )
    # [END howto_sensor_bedrock_ingest_data]

    # [START howto_operator_bedrock_knowledge_base_rag]
    knowledge_base_rag = BedrockRaGOperator(
        task_id="knowledge_base_rag",
        input="Who was the CEO of Amazon on 2022?",
        source_type="KNOWLEDGE_BASE",
        model_arn=f"arn:aws:bedrock:{region_name}::foundation-model/{CLAUDE_MODEL_ID}",
        knowledge_base_id=create_knowledge_base.output,
    )
    # [END howto_operator_bedrock_knowledge_base_rag]

    # [START howto_operator_bedrock_retrieve]
    retrieve = BedrockRetrieveOperator(
        task_id="retrieve",
        knowledge_base_id=create_knowledge_base.output,
        retrieval_query="Who was the CEO of Amazon in 1997?",
    )
    # [END howto_operator_bedrock_retrieve]

    delete_bucket = S3DeleteBucketOperator(
        task_id="delete_bucket",
        trigger_rule=TriggerRule.ALL_DONE,
        bucket_name=bucket_name,
        force_delete=True,
    )

    chain(
        # TEST SETUP
        test_context,
        create_bucket,
        opensearch_policies,
        collection,
        await_collection,
        create_vector_index(
            index_name=index_name, collection_id=collection, region=region_name
        ),
        copy_data_to_s3(bucket=bucket_name),
        # TEST BODY
        invoke_claude_completions,
        create_knowledge_base,
        await_knowledge_base,
        create_data_source,
        ingest_data,
        await_ingest,
        knowledge_base_rag,
        external_sources_rag_group(),
        retrieve,
        delete_data_source(
            knowledge_base_id=create_knowledge_base.output,
            data_source_id=create_data_source.output,
        ),
        delete_knowledge_base(knowledge_base_id=create_knowledge_base.output),
        # TEST TEARDOWN
        delete_vector_index(index_name=index_name, collection_id=collection),
        delete_opensearch_policies(collection_name=vector_store_name),
        delete_collection(collection_id=collection),
        delete_bucket,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
