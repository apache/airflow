import os
from unittest import * # noqa
from unittest import TestCase as OriginalTestCase


class Unittest(OriginalTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        print(os.environ.get('PYTHON_PATH'))
