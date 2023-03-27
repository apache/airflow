files = ['example_rds_event.py',
 'example_local_to_s3.py',
 'example_glue.py',
 'example_sagemaker_endpoint.py',
 'example_athena.py',
 'example_eks_with_fargate_profile.py',
 'example_ecs.py',
 'example_s3_to_sql.py',
 'example_rds_instance.py',
 'example_ecs_fargate.py',
 'example_s3_to_sftp.py',
 'example_rds_snapshot.py',
 'example_mongo_to_s3.py',
 'example_step_functions.py',
 'example_google_api_youtube_to_s3.py',
 'example_rds_export.py',
 'example_sns.py',
 'example_eks_with_nodegroups.py',
 'example_redshift_s3_transfers.py',
 'example_google_api_sheets_to_s3.py',
 'example_quicksight.py',
 'example_datasync.py',
 'example_emr_serverless.py',
 'example_ftp_to_s3.py',
 'example_cloudformation.py',
 'example_hive_to_dynamodb.py',
 'example_emr_notebook_execution.py',
 'example_gcs_to_s3.py',
 'example_ec2.py',
 'import_fix.py',
 'example_emr.py',
 'example_glacier_to_gcs.py',
 'example_s3.py',
 'example_emr_eks.py',
 'example_appflow.py',
 'example_redshift.py',
 'example_lambda.py',
 'example_sql_to_s3.py',
 'example_eks_with_nodegroup_in_one_step.py',
 'example_eks_with_fargate_in_one_step.py',
 'example_imap_attachment_to_s3.py',
 'example_eks_templated.py',
 'example_dynamodb_to_s3.py',
 'example_s3_to_ftp.py',
 'example_dms.py',
 'example_salesforce_to_s3.py',
 'example_sagemaker.py',
 'example_sqs.py',
 'example_sftp_to_s3.py',
 'example_batch.py']


for file in files:
    base_path = '/opt/airflow/tests/system/providers/amazon/aws/'
    path = base_path + file
    ans = input(f"working on {path}: Y or N")
    if ans == 'N':
        break
    with open(path, "r") as f:
        contents = f.readlines()

    index = -1

    for i in range(len(contents)):
        if contents[i].startswith("from __future__"):
            print(f"index is {i}")
            index = i + 1
            break


    value = 'from pytest import importorskip\nimportorskip("aiobotocore")'

    contents.insert(index, value)

    with open(path, "w") as f:
        contents = "".join(contents)
        f.write(contents)
from pytest import importorskip
importorskip("aiobotocore")
