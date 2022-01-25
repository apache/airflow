from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_lambda_hook import AwsLambdaHook
import json

class AwsLambdaInvokeFunctionOperator(BaseOperator):
    """
    Invoke AWS Lambda functions with a JSON payload.

    The check_success_function signature should be a single param which will receive a dict.
    The dict will be the "Response Structure" described in
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda.html#Lambda.Client.invoke.
    It may be necessary to read the Payload see the actual response from the Lambda function e.g.,

    ```
    def succeeded(response):
        payload = json.loads(response['Payload'].read())
        # do something with payload
    ```

    :param function_name: The name of the Lambda function.
    :type function_name: str
    :param region_name: AWS region e.g., eu-west-1, ap-southeast-1, etc.
    :type region_name: str
    :param payload: The JSON to submit as input to a Lambda function.
    :type payload: str
    :param log_type: Set to Tail to include the execution log in the response. Otherwise, set to "None".
    :type log_type: str
    :param qualifier: A version or alias name for the Lambda.
    :type qualifier: str
    :param aws_conn_id: connection id of AWS credentials / region name. If None,
        credential boto3 strategy will be used
        (http://boto3.readthedocs.io/en/latest/guide/configuration.html).
    :type aws_conn_id: str
    """

    @apply_defaults
    def __init__(
        self,
        function_name,
        payload,
        log_type="None",
        qualifier="$LATEST",
        aws_conn_id=None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.function_name = function_name
        self.payload = payload
        self.log_type = log_type
        self.qualifier = qualifier
        self.aws_conn_id = aws_conn_id

    def get_hook(self):
        """
        Initialises an AWS Lambda hook

        :return: airflow.providers.amazon.aws.hooks.lambda_function.LambdaHook
        """
        return AwsLambdaHook(
            self.function_name,
            self.log_type,
            self.qualifier,
            aws_conn_id=self.aws_conn_id,
        )

    def execute(self, context):
        self.log.info("AWS Lambda: invoking %s", self.function_name)
        try:
            response = self.get_hook().invoke_lambda(self.payload)
        except Exception as e:
            self.log.error(e)
            raise e

        self.log.info("AWS Lambda: %s succeeded!", self.function_name)
        return json.loads(response["Payload"].read())