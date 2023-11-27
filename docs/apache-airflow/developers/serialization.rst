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

In Apache Airflow serialization is required to exchange data between different
processes, particularly because the workers are possibly running on different
hosts. When data is exchanged between tasks this is called XCom (short for
"cross-communication"). Serialization is also used to make the webserver
and the scheduler stateless, so they do not have to parse the DAGs on its own
to visualize. XCom and DAGs make use of their own serialization engine.

XCom requires serialization of arbitrary objects. Thus, if you ended up here
because you had an error with serialization, as in "could not
serialize a value of type X", then you are looking at XCom. Rarely it
happens that DAG serialization fails, because we control both the input and
output of the serialization process and this is being tested for.

If you are looking for to serialize something in your own code, you should
use the ``airflow.serialization.serde`` module. It is the engine that powers the
serialization of XCom and is an order of magnitude faster than the DAG serializer.
In addition it is also more flexible and can serialize more types. Finally, it
provides forward compatibility and backwards compatibility by versioning the
serialized objects.

.. note::

    At the time of writing, neither the DAG nor the XCom serialization is
    part of the public API. The serialized format for DAGs is part the public
    API, but the serialization process itself is not.


Serialization and Encoding
--------------------------

Serialization is the process of converting the state of an object into a form
that can be persisted or transported. This means the state of the object
is converted into primitives (strings, numbers, etc.) that can be stored in
a file or sent over a network. The complement of serialization is deserialization,
which converts a stream into an object. Together, these processes allow data
to be stored and transferred.

In Python the canonical format of an object is a dictionary. Hence, in Airflow
an object is firstly serialized into a dictionary and then the dictionary is
encoded into an arbitrary wire format, currently in Airflow this is JSON.
This creates a separation between the serialization of the object
and the encoding into the wire format and gives us the flexibility to change
the wire format in the future.

The serialization of an arbitrary object is done by the ``airflow.serialization.serde``
module. An encoder into JSON is provided by the ``airflow.utils.json.XComEncoder``
class.


.. note::
    In the code you might find references to "make json serializable" or alike
    which is a remnant of the time when we used to serialize directly into JSON.
    Newer functions should not assume JSON as the wire format.


Not Pickle
----------

The serialization of objects in Airflow is by default not done using Python's pickle.
Python's pickle is a powerful serialization engine, but it is also dangerous. It is dangerous
because it can execute arbitrary code when deserializing an object. This is a security
risk and we do not want to expose our users to this risk. In addition, pickle is
not guaranteed to be compatible across Python versions. This means that if you
serialize an object in Python 3.6, you might not be able to deserialize it in
Python 3.7.

Schema
------

The dictionary schema for serialization an object is as follows:

.. code-block:: python
    data = {
        "__class__": "fully.qualified.class.name",
        "__version__": 1,  # integers only
        "__data__": {
            "field1": "value1",  # primitives only
            "field2": abc,
            "field3": {
                "__class__": "fully.qualified.class.name2",
                "__version__": 1,
                "__data__": {},
            },
        },
    }


Having the class name in the schema allows us to deserialize the object by loading the required
module for it and verifying that the class is allowed to be serialized. Including version
information, makes it possible to provide backward and forward compatibility. Finally, the
``__data__`` field contains the actual data of the object. The data is a dictionary where
the keys are the field names and the values are the serialized primitive values of the fields.


``serialize``
-------------

The ``serialize`` function is the main entry point for serialization. It takes
an object and returns a python primitive that can be encoded into a wire format. The
function is defined as follows:

.. code-block::
    # U = Union[bool, float, int, dict, list, str, tuple, set]
    def serialize(o: object, depth: int = 0) -> U | None:


The function is recursive and will serialize all the objects in the object graph.
The ``depth`` parameter is used to limit the recursion depth. It will serialize any
object that is not a primitive (bool, float, int, str, etc). Complex objects are
serialized by:

1. Checking if the object has an Airflow provided serializer in the namespace of
   ``airflow.serialization.serializers``. If it does, it will call the serializer
   and return the result.
2. Checking if the object has a ``serialize`` method. If it does, it will call
   the method and return the result.
3. Check if the object is a subclass of ``dataclasses.dataclass``, ``attrs`` or
   ``pydantic``. If it is, it will serialize the object by calling the appropriate
   method.

If none of the above applies, it will raise an exception.

.. note::
    You will notice that ``isinstance`` is not used to check if an object is
    a subclass of ``dataclasses.dataclass``, ``attrs`` or ``pydantic`` and is
    used sparsely. This is because ``ininstance`` is quite slow.


Custom Serialization
^^^^^^^^^^^^^^^^^^^^

As mentioned there are three ways to provide custom serialization for an object.
The first one is to provide a serializer in the ``airflow.serialization.serializers``
namespace. The second one is to provide a ``serialize`` method in the object.
The third one is to make the object a subclass of ``dataclasses.dataclass``,
``attrs`` or ``pydantic``. The first two are explained in the following sections.


Namespace serializer
~~~~~~~~~~~~~~~~~~~~

In some cases it is not possible to add a ``serialize`` and ``deserialize`` method
to the object as it is not under the control of developer. In this case, it is
possible to provide a serializer in the ``airflow.serialization.serializers``
namespace. This namespace is scanned for serializers at startup for serializers
and deserializers.

When creating such serializer it is important to lazy load any modules, as the
serializer might not even be called. Registering happens by providing a list
of ``str`` that are the fully qualified names of the classes that can be serialized.
Typically one supports the same deserializer as serializer, but this is not required.

A simple serializer for a class ``MyObject`` is as follows:

.. code-block:: python

    __version__ = 1

    serializers = [
        "MyObject",
    ]
    deserializers = serializers


    # return value signature is: primitive, fully qualified name, version, is_serialized
    def serialize(o: object) -> tuple[U, str, int, bool]:
        data = {
            "field1": o.field1,
            "field2": o.field2,
        }
        return data, qualname(o), __version__, True


    def deserialize(classname: str, version: int, data: U) -> object:
        cast(dict, data)

        return MyObject(**data)


``serialize`` method
~~~~~~~~~~~~~~~~~~~~

For custom serialization, you can provide a ``serialize`` method in your class.
This method should return a dictionary (or other primitive) with the serialized
fields. You do not need to return every field serialized yourself. The serializer
will take care of serializing fields that are not primitives, if there is
a serializer for the field type.

The object is inspected for the required ``__version__ : ClassVar[int]`` attribute.
This is used to provide the version of the serialized object.

.. code-block:: python

    class MyObject:
        __version__: ClassVar[int] = 1

        def __init__(self, field1: str, field2: int):
            self.field1 = field1
            self.field2 = field2

        def serialize(self) -> dict:
            return {
                "field1": self.field1,
                "field2": self.field2,
            }


``deserialize`` method
~~~~~~~~~~~~~~~~~~~~~~

The companion to serialize is deserialize. This method is called during deserialization
and needs to be either a classmethod or a staticmethod. It takes a dictionary and a version
as arguments. The dictionary contains the serialized fields of the object and the version
is the version of the serialized object. The method should return an instance of the
class.

.. code-block:: python

    class MyObject:
        __version__: ClassVar[int] = 1

        def __init__(self, field1: str, field2: int):
            self.field1 = field1
            self.field2 = field2

        @classmethod
        def deserialize(cls, d: dict, version: int) -> "MyObject":
            return cls(
                field1=d["field1"],
                field2=d["field2"],
            )


Deserialization
---------------
Deserialization is the process of converting a stream of bytes into an object.
In Airflow this is done by the ``deserialize`` function. The function is defined
in ``airflow.serialization.serde`` and is as follows:

.. code-block::

    def deserialize(o: T | None, full=True, type_hint: Any = None) -> object:


The function takes a primitive and returns an object. The ``full`` parameter
controls if the object is fully deserialized or not. If it is not fully deserialized,
then the object is returned as a ``str`` representation suitable for displaying
on the UI.

The ``type_hint`` parameter is used to provide a type hint for the to be deserialized
class in case type information was lost during serialization. This does not override the
type information in the serialized object, but is only used to provide when it is missing.


DAG Serialization and Deserialization
-------------------------------------

DAGs are serialized and deserialized by the ``airflow.serialization.serialized_objects``
module. This module should only be used for DAG serialization and deserialization.
This includes Operators, Tasks, DAGs, DAG Runs, and Task Instances. Outside of DAG
serialization this module should be considered legacy as it is slow and does not provide
support for arbitrary objects. The module provides guarantees by schema validation for DAGs.

To serialize a DAG, you can use the ``BaseSerialization.serialize`` method.

.. code-block:: python

      from airflow.serialization.serialized_objects import SerializedDAG
      from airflow.serialization.serialized_objects import BaseSerialization

      dag = Dag(...)
      serialized_dag = BaseSerialization.to_dict(dag)


Deserialization happens the same way, but in reverse. Not that the returned object by deserialization
is a ``SerializedDAG`` object and not a ``DAG`` object.
