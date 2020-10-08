from airflow.models import BaseOperator


class GreatExpectationsBaseOperator(BaseOperator):
    """
        This is the base operator for all Great Expectations operators.
    """

    @apply_defaults
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def instantiate_data_context(self):
        """
            Create the great expectations data context.  The context specifies
            things like the data source containing the data to be validated
            and stores where expectations files and validation files will live.
        """
        raise NotImplementedError('Please implement execute() in sub class!')

    def get_batch_kwargs(self):
        """
            Tell GE where to fetch the batch of data to be validated.
        """
        raise NotImplementedError('Please implement execute() in sub class!')

    def execute(self, context):
        raise NotImplementedError('Please implement execute() in sub class!')


