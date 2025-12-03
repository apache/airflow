 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Serialization
=============

To support data exchange, like arguments, between tasks, Airflow needs to serialize the data to be exchanged and
deserialize it again when required in a downstream task. Serialization also happens so that the webserver and
the scheduler (as opposed to the Dag processor) do not need to read the Dag file. This is done for security purposes
and efficiency.

Serialization is a surprisingly hard job. Python out of the box only has support for serialization of primitives,
like ``str`` and ``int`` and it loops over iterables. When things become more complex, custom serialization is required.

Airflow out of the box supports three ways of custom serialization. Primitives are returned as is, without
additional encoding, e.g. a ``str`` remains a ``str``. When it is not a primitive (or iterable thereof) Airflow
looks for a registered serializer and deserializer in the namespace of ``airflow.sdk.serialization.serializers``.
If not found it will look in the class for a ``serialize()`` method or in case of deserialization a
``deserialize(data, version: int)`` method. Finally, if the class is either decorated with ``@dataclass``
or ``@attr.define`` it will use the public methods for those decorators.

If you are looking to extend Airflow with a new serializer, it is good to know when to choose what way of serialization.
Objects that are under the control of Airflow, i.e. residing under the namespace of ``airflow.*`` like
``airflow.model.dag.DAG`` or under control of the developer e.g. ``my.company.Foo`` should first be examined to see
whether they can be decorated with ``@attr.define`` or ``@dataclass``. If that is not possible then the ``serialize``
and ``deserialize`` methods should be implemented. The ``serialize`` method should return a primitive or a dict.
It does not need to serialize the values in the dict, that will be taken care of, but the keys should be of a primitive
form.

Objects that are not under control of Airflow, e.g. ``numpy.int16`` will need a registered serializer and deserializer.
Versioning is required. Primitives, excluding ``bytes``, can be returned as dicts. Again ``dict`` values do not need to be serialized,
but its keys need to be of primitive form. In case you are implementing a registered serializer, take special care
not to have circular imports. Typically, this can be avoided by using ``str`` for populating the list of serializers.
Like so: ``serializers = ["my.company.Foo"]`` instead of ``serializers = [Foo]``.

.. note::

  Serialization and deserialization is dependent on speed. Use built-in functions like ``dict`` as much as you can and stay away from using classes and other complex structures.

Airflow Object
--------------

.. code-block:: python

    from typing import Any, ClassVar


    class Foo:
        __version__: ClassVar[int] = 1

        def __init__(self, a, v) -> None:
            self.a = a
            self.b = {"x": v}

        def serialize(self) -> dict[str, Any]:
            return {
                "a": self.a,
                "b": self.b,
            }

        @staticmethod
        def deserialize(data: dict[str, Any], version: int):
            f = Foo(a=data["a"], v=data["b"])
            return f


Registered
^^^^^^^^^^

.. code-block:: python

    from __future__ import annotations

    from typing import TYPE_CHECKING

    from airflow.utils.module_loading import qualname

    if TYPE_CHECKING:
        import decimal

        from airflow.sdk.serialization.serde import U


    serializers = [
        "decimal.Decimal"
    ]  # this can be a type or a fully qualified str. Str can be used to prevent circular imports
    deserializers = serializers  # in some cases you might not have a deserializer (e.g. k8s pod)

    __version__ = 1  # required


    # the serializer expects output, classname, version, is_serialized?
    def serialize(o: object) -> tuple[U, str, int, bool]:
        from decimal import Decimal

        if not isinstance(o, Decimal):
            return "", "", 0, False
        name = qualname(o)
        _, _, exponent = o.as_tuple()
        if isinstance(exponent, int) and exponent >= 0:  # No digits after the decimal point.
            return int(o), name, __version__, True
        # Technically lossy due to floating point errors, but the best we
        # can do without implementing a custom encode function.
        return float(o), name, __version__, True


    # the deserializer sanitizes the data for you, so you do not need to deserialize values yourself
    def deserialize(cls: type, version: int, data: object) -> Decimal:
        from decimal import Decimal

        # always check version compatibility
        if version > __version__:
            raise TypeError(f"serialized {version} of {qualname(cls)} > {__version__}")

        if cls is not Decimal:
            raise TypeError(f"do not know how to deserialize {qualname(cls)}")

        return Decimal(str(data))
