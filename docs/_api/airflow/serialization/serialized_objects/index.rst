:mod:`airflow.serialization.serialized_objects`
===============================================

.. py:module:: airflow.serialization.serialized_objects

.. autoapi-nested-parse::

   Serialized DAG and BaseOperator



Module Contents
---------------

.. data:: HAS_KUBERNETES
   :annotation: = True

   

.. data:: log
   

   

.. data:: FAILED
   :annotation: = serialization_failed

   

.. data:: BUILTIN_OPERATOR_EXTRA_LINKS
   :annotation: :List[str] = ['airflow.providers.google.cloud.operators.bigquery.BigQueryConsoleLink', 'airflow.providers.google.cloud.operators.bigquery.BigQueryConsoleIndexableLink', 'airflow.providers.google.cloud.operators.mlengine.AIPlatformConsoleLink', 'airflow.providers.qubole.operators.qubole.QDSLink', 'airflow.operators.dagrun_operator.TriggerDagRunLink', 'airflow.sensors.external_task_sensor.ExternalTaskSensorLink']

   

.. py:class:: BaseSerialization

   BaseSerialization provides utils for serialization.

   .. attribute:: _primitive_types
      

      

   .. attribute:: _datetime_types
      

      

   .. attribute:: _excluded_types
      

      

   .. attribute:: _json_schema
      :annotation: :Optional[Validator]

      

   .. attribute:: _CONSTRUCTOR_PARAMS
      :annotation: :Dict[str, Parameter]

      

   .. attribute:: SERIALIZER_VERSION
      :annotation: = 1

      

   .. attribute:: _deserialize_datetime
      

      

   .. attribute:: _deserialize_timezone
      

      

   
   .. classmethod:: to_json(cls, var: Union[DAG, BaseOperator, dict, list, set, tuple])

      Stringifies DAGs and operators contained by var and returns a JSON string of var.



   
   .. classmethod:: to_dict(cls, var: Union[DAG, BaseOperator, dict, list, set, tuple])

      Stringifies DAGs and operators contained by var and returns a dict of var.



   
   .. classmethod:: from_json(cls, serialized_obj: str)

      Deserializes json_str and reconstructs all DAGs and operators it contains.



   
   .. classmethod:: from_dict(cls, serialized_obj: Dict[Encoding, Any])

      Deserializes a python dict stored with type decorators and
      reconstructs all DAGs and operators it contains.



   
   .. classmethod:: validate_schema(cls, serialized_obj: Union[str, dict])

      Validate serialized_obj satisfies JSON schema.



   
   .. staticmethod:: _encode(x: Any, type_: Any)

      Encode data by a JSON dict.



   
   .. classmethod:: _is_primitive(cls, var: Any)

      Primitive types.



   
   .. classmethod:: _is_excluded(cls, var: Any, attrname: str, instance: Any)

      Types excluded from serialization.



   
   .. classmethod:: serialize_to_json(cls, object_to_serialize: Union[BaseOperator, DAG], decorated_fields: Set)

      Serializes an object to json



   
   .. classmethod:: _serialize(cls, var: Any)

      Helper function of depth first search for serialization.

      The serialization protocol is:

      (1) keeping JSON supported types: primitives, dict, list;
      (2) encoding other types as ``{TYPE: 'foo', VAR: 'bar'}``, the deserialization
          step decode VAR according to TYPE;
      (3) Operator has a special field CLASS to record the original class
          name for displaying in UI.



   
   .. classmethod:: _deserialize(cls, encoded_var: Any)

      Helper function of depth first search for deserialization.



   
   .. classmethod:: _deserialize_timedelta(cls, seconds: int)



   
   .. classmethod:: _is_constructor_param(cls, attrname: str, instance: Any)



   
   .. classmethod:: _value_is_hardcoded_default(cls, attrname: str, value: Any, instance: Any)

      Return true if ``value`` is the hard-coded default for the given attribute.

      This takes in to account cases where the ``concurrency`` parameter is
      stored in the ``_concurrency`` attribute.

      And by using `is` here only and not `==` this copes with the case a
      user explicitly specifies an attribute with the same "value" as the
      default. (This is because ``"default" is "default"`` will be False as
      they are different strings with the same characters.)

      Also returns True if the value is an empty list or empty dict. This is done
      to account for the case where the default value of the field is None but has the
      ``field = field or {}`` set.




.. py:class:: SerializedBaseOperator(*args, **kwargs)

   Bases: :class:`airflow.models.baseoperator.BaseOperator`, :class:`airflow.serialization.serialized_objects.BaseSerialization`

   A JSON serializable representation of operator.

   All operators are casted to SerializedBaseOperator after deserialization.
   Class specific attributes used by UI are move to object attributes.

   .. attribute:: _decorated_fields
      

      

   .. attribute:: _CONSTRUCTOR_PARAMS
      

      

   .. attribute:: task_type
      

      

   
   .. classmethod:: serialize_operator(cls, op: BaseOperator)

      Serializes operator into a JSON object.



   
   .. classmethod:: deserialize_operator(cls, encoded_op: Dict[str, Any])

      Deserializes an operator from a JSON object.



   
   .. classmethod:: _is_excluded(cls, var: Any, attrname: str, op: BaseOperator)



   
   .. classmethod:: _deserialize_operator_extra_links(cls, encoded_op_links: list)

      Deserialize Operator Links if the Classes  are registered in Airflow Plugins.
      Error is raised if the OperatorLink is not found in Plugins too.

      :param encoded_op_links: Serialized Operator Link
      :return: De-Serialized Operator Link



   
   .. classmethod:: _serialize_operator_extra_links(cls, operator_extra_links: Iterable[BaseOperatorLink])

      Serialize Operator Links. Store the import path of the OperatorLink and the arguments
      passed to it. Example
      ``[{'airflow.providers.google.cloud.operators.bigquery.BigQueryConsoleLink': {}}]``

      :param operator_extra_links: Operator Link
      :return: Serialized Operator Link




.. py:class:: SerializedDAG

   Bases: :class:`airflow.models.dag.DAG`, :class:`airflow.serialization.serialized_objects.BaseSerialization`

   A JSON serializable representation of DAG.

   A stringified DAG can only be used in the scope of scheduler and webserver, because fields
   that are not serializable, such as functions and customer defined classes, are casted to
   strings.

   Compared with SimpleDAG: SerializedDAG contains all information for webserver.
   Compared with DagPickle: DagPickle contains all information for worker, but some DAGs are
   not pickle-able. SerializedDAG works for all DAGs.

   .. attribute:: _decorated_fields
      

      

   .. attribute:: _CONSTRUCTOR_PARAMS
      

      

   .. attribute:: _json_schema
      

      

   
   .. staticmethod:: __get_constructor_defaults()



   
   .. classmethod:: serialize_dag(cls, dag: DAG)

      Serializes a DAG into a JSON object.



   
   .. classmethod:: deserialize_dag(cls, encoded_dag: Dict[str, Any])

      Deserializes a DAG from a JSON object.



   
   .. classmethod:: to_dict(cls, var: Any)

      Stringifies DAGs and operators contained by var and returns a dict of var.



   
   .. classmethod:: from_dict(cls, serialized_obj: dict)

      Deserializes a python dict in to the DAG and operators it contains.




.. py:class:: SerializedTaskGroup

   Bases: :class:`airflow.utils.task_group.TaskGroup`, :class:`airflow.serialization.serialized_objects.BaseSerialization`

   A JSON serializable representation of TaskGroup.

   
   .. classmethod:: serialize_task_group(cls, task_group: TaskGroup)

      Serializes TaskGroup into a JSON object.



   
   .. classmethod:: deserialize_task_group(cls, encoded_group: Dict[str, Any], parent_group: Optional[TaskGroup], task_dict: Dict[str, BaseOperator])

      Deserializes a TaskGroup from a JSON object.




