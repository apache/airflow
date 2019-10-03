import pytz
from cron_descriptor import ExpressionDescriptor


class CronExpression:
    """Class that expresses a cron expression
    https://en.wikipedia.org/wiki/Cron#CRON_expression

    This does not support YEAR which is inline with Airflow Cron Expressions
    https://airflow.apache.org/scheduler.html

    Online Cron Tester: http://cron.schlitt.info/

    :raises TypeError in case the cron expression or timezone is invalid
    """
    cron_macros = {
        '@hourly':   '0 * * * *',
        '@daily':    '0 0 * * *',
        '@weekly':   '0 0 * * 0',
        '@monthly':  '0 0 1 * *',
        '@yearly':   '0 0 1 1 *',
        '@annually': '0 0 1 1 *',
        '@midnight': '0 0 * * *',
    }

    def __init__(
        self,
        expression: str,
        timezone: str,
        show_timezone: bool
    ):
        def get_formatted_expression(description: str):
            if self.show_timezone:
                return f"{description} [{self.timezone}]"
            else:
                return f"{description}"

        self.expression = expression
        self.timezone = timezone
        self.show_timezone = show_timezone

        # validating timezone
        if self.timezone not in pytz.all_timezones_set:
            raise TypeError(f"{self.timezone} is not a valid timezone")

        # setting description
        try:
            human_readable = ExpressionDescriptor(
                f"{self.expression}", throw_exception_on_parse_error=True
            ).get_description()
            self.description = get_formatted_expression(human_readable)
        except Exception as e:
            # try conversion
            if self.expression in self.cron_macros:
                try:
                    human_readable = ExpressionDescriptor(
                        f"{self.cron_macros[self.expression]}", throw_exception_on_parse_error=True
                    ).get_description()
                    self.description = get_formatted_expression(human_readable)
                    return
                except Exception as e:
                    pass
            # ExpressionDescriptor is not able to describe, revert to default
            self.description = get_formatted_expression(self.expression)

    def __str__(self):
        return self.description


def get_human_readable_cron(expression: str, timezone: str, include_timezone: bool = True) -> str:
    return str(CronExpression(expression = expression, timezone = timezone, show_timezone = timezone != 'US/Eastern'))
