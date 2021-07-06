from jsonschema import Draft4Validator, FormatChecker
from jsonschema.exceptions import ValidationError
from typing import Any


class Param(object):
    """
    Class to hold the default vallue of a Param and rule set to do the validations

    :param default: The value of this Param object holds
    :type default: Any
    :param description: Optional help text for the Param
    :type description: str
    :param schema: The validation schema of the Param, if not given then all kwargs except
    default & description will form the schema
    :type schema: dict
    """
    def __init__(self, default: Any = None, description: str = None, **kwargs):
        self.default = default
        self.description = description
        self.schema = kwargs.pop('schema') if 'schema' in kwargs else kwargs

        # If default is not None, then validate it once, may raise ValueError
        if default:
            try:
                validator = Draft4Validator(self.schema, format_checker=FormatChecker())
                validator.validate(self.default)
            except ValidationError as err:
                raise ValueError(err)

    def __call__(self, suppress_exception: bool = False) -> Any:
        """
        Runs the validations and returns the Param's default value. May raise ValueError on failed validations.

        :param suppress_exception: To raise an exception or not when the validations fails.
        If true and validations fails, the return value would be None.
        :type suppress_exception: bool
        """
        try:
            validator = Draft4Validator(self.schema, format_checker=FormatChecker())
            validator.validate(self.default)
        except ValidationError as err:
            if suppress_exception:
                return None
            raise ValueError(err)
        return self.default
