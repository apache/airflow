"""**Default AWS Batch configuration**
This is the default configuration for calling the Batch `submit_job` function.
The AWS Batch Executor calls Boto3's submit_job(**kwargs) function with the kwargs templated by this
dictionary. See the URL below for documentation on the parameters accepted by the Boto3 submit_job function.
In other words, if you don't like the way Airflow calls the Boto3 SubmitJob API, then send your own kwargs
by overriding the airflow config file.
.. seealso::
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch.html#Batch.Client.submit_job
:return: Dictionary kwargs to be used by Batch submit_job() function.
"""

from airflow.configuration import conf


def has_option(section, config_name) -> bool:
    """Returns True if configuration has a section and an option"""
    if conf.has_option(section, config_name):
        config_val = conf.get(section, config_name)
        return config_val is not None and config_val != ''
    return False


BATCH_SUBMIT_JOB_KWARGS = {}
if conf.has_option('batch', 'region'):
    BATCH_SUBMIT_JOB_KWARGS = {
        'jobName': conf.get('batch', 'job_name'),
        'jobQueue': conf.get('batch', 'job_queue'),
        'jobDefinition': conf.get('batch', 'job_definition'),
        'containerOverrides': {
            'command': []
        }
    }
