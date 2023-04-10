"""
Boto3 uses Botocore to dynamically generate low-level clients.
We can also use botocore to dynamically assert function calls based on the documentation. It's not 100% full-proof.
It's pretty much a deeply nested dictionary.
On the highest level here's how this works:
* Every service has a service-model (i.e: ecs, batch)
* Every service model has operations and shapes
* An operation is the function (i.e: run_task, submit_job)
    * Each operation has an input & output Shape
* A shape is the defined schema of the function call with a defined type.
* Most shapes are recursively composed of other shapes.
"""
import re
import botocore.loaders
import datetime as dt


def get_botocore_model(service_name):
    loader = botocore.loaders.Loader()
    return loader.load_service_model(service_name, 'service-2')


def assert_botocore_call(model, method_name, args, kwargs):
    """Asserts that a given method-call adheres to Botocore's API docs"""
    assert len(args) == 0, "Boto3 doesn't like args. Use only kwargs."
    # Now check kwargs, recursively
    input_shape_name = model['operations'][method_name]['input']['shape']
    input_shape = model['shapes'][input_shape_name]
    __assert_botocore_shape(model, input_shape_name, input_shape, kwargs)


def __assert_botocore_shape(model, input_shape_name, input_shape, input_value):
    """Asserts that a given value adheres to Botocore's API docs"""
    if 'required' in input_shape:
        for required_key in input_shape['required']:
            assert required_key in input_value.keys(), f'{input_shape_name} requires {required_key}'

    if 'type' in input_shape:
        if input_shape['type'] == 'boolean':
            assert isinstance(input_value, bool), \
                    f'Error in {input_shape_name}. {input_value} should be of type bool, not {type(input_value)}'
        elif input_shape['type'] == 'double':
            assert isinstance(input_value, float), \
                    f'Error in {input_shape_name}. {input_value} should be of type float, not {type(input_value)}'
        elif input_shape['type'] == 'integer':
            assert isinstance(input_value, int), \
                    f'Error in {input_shape_name}. {input_value} should be of type int, not {type(input_value)}'
        elif input_shape['type'] == 'list':
            assert isinstance(input_value, list), \
                    f'Error in {input_shape_name}. {input_value} should be of type list, not {type(input_value)}'
        elif input_shape['type'] == 'long':
            assert isinstance(input_value, int), \
                    f'Error in {input_shape_name}. {input_value} should be of type int, not {type(input_value)}'
        elif input_shape['type'] == 'map':
            assert isinstance(input_value, dict), \
                    f'Error in {input_shape_name}. {input_value} should be of type dict, not {type(input_value)}'
        elif input_shape['type'] == 'string':
            assert isinstance(input_value, str), \
                    f'Error in {input_shape_name}. {input_value} should be of type str, not {type(input_value)}'
        elif input_shape['type'] == 'timestamp':
            assert isinstance(input_value, (dt.datetime, dt.date)), \
                    f'Error in {input_shape_name}. {input_value} should date/datetime, not {type(input_value)}'
        elif input_shape['type'] == 'structure':
            __assert_structure(model, input_shape, input_value)

    if 'enum' in input_shape:
        assert input_value in input_shape['enum'], \
                f'Error in {input_shape_name}. Expected one of: {input_shape["enum"]}, Got: {input_value}'
    if 'min' in input_shape:
        assert input_value >= input_shape['min'], \
                f'Error in {input_shape_name}. Expected: {input_shape} >= {input_shape["min"]}'
    if 'max' in input_shape:
        assert input_value <= input_shape['max'], \
                f'Error in {input_shape_name}. Expected: {input_shape} <= {input_shape["max"]}'
    if 'pattern' in input_shape:
        assert bool(re.match(input_shape['pattern'], input_value)), \
                f'Error in {input_shape_name}. Expected {input_value} to match regex {input_shape["pattern"]}'


def __assert_structure(model, input_shape, input_value):
    if 'member' in input_shape:
        member_name = input_shape['member']['shape']
        member_shape = model['shapes'][member_name]
        if member_name in input_value:
            member_value = input_value[member_name]
            __assert_botocore_shape(model, member_name, member_shape, member_value)
    elif 'members' in input_shape:
        for member_name in input_shape['members']:
            shape_name = input_shape['members'][member_name]['shape']
            member_shape = model['shapes'][shape_name]
            if member_name in input_value:
                member_value = input_value[member_name]
                __assert_botocore_shape(model, shape_name, member_shape, member_value)
